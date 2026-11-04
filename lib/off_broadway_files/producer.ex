defmodule OffBroadwayFiles.Producer do
  @moduledoc """
  A Broadway producer that reads files from a directory.
  """
  use GenStage

  @behaviour Broadway.Producer
  # @behaviour Broadway.Acknowledger

  @max_binary_memory 50_000_000

  alias OffBroadwayFiles.Filter.MinSize
  # alias OffBroadwayFiles.Filter.NamePattern
  alias OffBroadwayFiles.Filter.RegularFile

  use Private

  require Logger

  @doc false
  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(args) do
    Logger.info("init: #{inspect(args)}")

    state_tab = args[:state_tab] || :"#{name(args)}_producer_state"
    Logger.debug("state_tab: #{state_tab}")

    # TODO: handle case where table already exists
    :ets.new(state_tab, [:named_table, :public, :set])

    config = %{
      # Source directory for files
      in_dir: args[:in_dir],

      # Directory to save files after they have been processed
      archive_dir: args[:archive_dir],

      # Directory to save files if processing fails
      failed_dir: args[:failed_dir],

      # Minimum age of files to process in seconds
      # Avoids processing files that are currently being written.
      min_age: args[:min_age] || 0,

      # Minimum size of files to process
      # Avoids processing empty files or files that are corrupted/truncated.
      min_size: args[:min_size] || 0,

      # Regex matching files to process
      # Files that do not match this pattern will be ignored
      # file_pattern: Regex.compile!(args[:file_pattern] || ".*\\.log$"),
      file_pattern: Regex.compile!(args[:file_pattern] || ".*$"),

      # Files to queue in advance of demand
      prefetch_count: args[:prefetch_count] || 10,

      # Number of times to try processing a file before moving it to the failed_dir
      tries: args[:tries] || 3,

      filters: args[:filters] || [RegularFile, MinSize],

      # Regex to extract datetime from filename
      datetime_pattern:
        Regex.compile!(
          args[:datetime_pattern] || "\.*-(?<year>\\d{4})(?<month>\\d{2})(?<day>\\d{2})-(?<hour>\\d{2})(?<min>\\d{2})"
        )
    }

    fetch_interval = args[:fetch_interval] || 10_000

    state = %{
      config: config,
      state_tab: state_tab,

      # Unfulfilled demand from consumers
      demand: 0,

      # Prefetch queue to avoid reading dir on every demand
      queue: :queue.new(),

      # How often to check for new files, in ms
      fetch_interval: fetch_interval
    }

    Logger.debug("state: #{inspect(state)}")

    Process.send(self(), :fetch, [])
    {:producer, state}
  end

  @impl true
  # Fulfill demand from queue
  def handle_demand(incoming_demand, state) do
    %{demand: demand, queue: queue} = state

    queue_len = :queue.len(queue)
    Logger.info("incoming_demand: #{incoming_demand}, demand: #{demand}, queue_len: #{queue_len}")

    new_demand = incoming_demand + demand

    {events, remaining_queue, remaining_demand} = dispatch_events(queue, queue_len, new_demand)

    Logger.debug(
      "events: #{inspect(events)}, remaining_queue: #{inspect(remaining_queue)}, remaining_demand: #{remaining_demand}"
    )

    messages = Enum.map(events, &event_to_message/1)

    {:noreply, messages, %{state | queue: remaining_queue, demand: remaining_demand}}
  end

  @impl true
  # Add new files from the input dir to the queue
  def handle_info(:fetch, state) do
    Logger.debug("handle_info(:fetch) state: #{inspect(state)}")

    %{queue: queue, demand: demand} = state

    maybe_garbage_collect()

    new_queue = add_files_to_queue(queue, state)

    {events, remaining_queue, remaining_demand} =
      dispatch_events(new_queue, :queue.len(new_queue), demand)

    messages = Enum.map(events, &event_to_message/1)

    Process.send_after(self(), :fetch, state.fetch_interval)
    {:noreply, messages, %{state | queue: remaining_queue, demand: remaining_demand}}
  end

  # Handler for Broadway.CallerAcknowledger
  def handle_info({:ack, _ref, successful_messages, failed_messages} = message, state) do
    Logger.info(fn -> "ACK: #{inspect(message)}" end)

    # Move files to archive_dir after successful processing
    Enum.each(successful_messages, &process_successful_message(&1, state))

    # Retry or move files to failed_dir
    retry_messages =
      failed_messages
      |> Enum.map(&process_failed_message(&1, state))
      |> List.flatten()

    {:noreply, retry_messages, state}
  end

  # Handler for Broadway.CallerAcknowledger
  def handle_info({:configure, _ref, _options} = message, state) do
    Logger.info(fn -> "ACK: #{inspect(message)}" end)
    {:noreply, [], state}
  end

  def handle_info(message, state) do
    Logger.info(fn -> "Unexpected message: #{inspect(message)}" end)
    {:noreply, [], state}
  end

  # Wrap GenStage event in a Broadway.Message with metadata
  defp event_to_message(event) do
    %Broadway.Message{
      data: event,
      acknowledger: Broadway.CallerAcknowledger.init({self(), make_ref()}, :ignored),
      metadata: Map.take(event, [:name, :path, :stat])
    }
  end

  # :ets.tab2list(:zones)

  private do
    # Fulfil demand from queue
    @spec dispatch_events(:queue.queue(), non_neg_integer(), non_neg_integer()) ::
            {events :: list(), remaining_queue :: :queue.queue(),
             remaining_demand :: non_neg_integer()}
    defp dispatch_events(queue, queue_len, demand)

    # queue is empty
    defp dispatch_events(queue, 0, demand) do
      {[], queue, demand}
    end

    # queue has enough to satisfy demand
    defp dispatch_events(queue, queue_len, demand) when queue_len >= demand do
      {events_queue, remaining_queue} = :queue.split(demand, queue)
      {:queue.to_list(events_queue), remaining_queue, 0}
    end

    # queue does not have enough to satisfy demand
    defp dispatch_events(queue, queue_len, demand) when queue_len < demand do
      {events_queue, remaining_queue} = :queue.split(queue_len, queue)
      {:queue.to_list(events_queue), remaining_queue, demand - queue_len}
    end

    # Read input directory and add new files to the queue
    @spec add_files_to_queue(:queue.queue(), map()) :: :queue.queue()
    defp add_files_to_queue(queue, state) do
      %{config: config, state_tab: state_tab} = state

      queue_len = :queue.len(queue)
      desired_count = config.prefetch_count - queue_len

      case read_files(config) do
        {:ok, all_files} ->
          now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())

          new_files =
            all_files
            # Get files that are not already being processed
            |> new_files(config)
            # Restrict to a reasonable number of files
            |> Enum.take(desired_count)
            # Stat file, ignoring missing files
            |> Enum.flat_map(&stat_file/1)
            # Apply filter modules
            |> apply_filters(config)
            # Only process regular files, not dirs and special files
            # |> Enum.filter(&regular_file?/1)
            # Only process files that have size greater than minumum
            # |> Enum.filter(&min_size(&1, config))
            # Skip files newer than min_age, avoiding files currently being written
            |> Enum.filter(&by_age(&1, now, config.min_age))

          # Add files to queue
          queue = Enum.reduce(new_files, queue, &:queue.in/2)

          # Add files to state table to indicate that they are being processed
          Enum.each(new_files, fn file -> :ets.insert(state_tab, {file.path, %{try: 1}}) end)

          Logger.debug("Added #{length(new_files)} files, queue len: #{:queue.len(queue)}")
          queue

        # |> Enum.map(&get_datetime_from_filename(&1, datetime_pattern))

        {:error, reason} ->
          Logger.error("Error reading files from #{config.in_dir}: #{inspect(reason)}")
          queue
      end
    end

    # Apply filters to files, returning only files that pass all filters
    @spec apply_filters(list(map()), map()) :: list(map())
    defp apply_filters(files, config) do
      for file <- files, Enum.all?(config.filters, &(&1.test(file, config))) do
        file
      end
    end

    # Read files from the input directory that match pattern
    @spec read_files(map()) ::
            {:ok, list(map())} | {:error, File.posix() | :badarg | {:no_translation, binary()}}
    defp read_files(config) do
      %{file_pattern: file_pattern, in_dir: in_dir} = config

      with {:ok, all_files} <- File.ls(in_dir) do
        Logger.debug("all_files in #{in_dir}: #{inspect(all_files)}")

        files =
          all_files
          |> match_names(file_pattern)
          |> Enum.sort()
          |> Enum.map(fn name -> %{name: name, path: Path.join(in_dir, name)} end)

        {:ok, files}
      end
    end

    # Select names that match Regex pattern, if any
    defp match_names(names, nil), do: names

    defp match_names(names, file_pattern) do
      Enum.filter(names, fn name -> Regex.match?(file_pattern, name) end)
    end

    # Get files that are not already in the state table
    @spec new_files(list(map()), map()) :: list(map())
    defp new_files(events, config) do
      %{state_tab: state_tab} = config
      file_state = :ets.tab2list(state_tab) |> Enum.into(%{})
      Enum.filter(events, fn event -> not Map.has_key?(file_state, event.path) end)
    end

    # Stat file and add info to record, returning empty list on error
    @spec stat_file(map()) :: list(map())
    defp stat_file(%{path: path} = rec) do
      case File.stat(path, time: :universal) do
        {:ok, stat} ->
          [Map.put(rec, :stat, stat)]

        {:error, reason} ->
          Logger.error("Could not stat file #{path}: #{reason}")
          []
      end
    end

    # Test if file is a regular file, not a dir or special file
    @spec regular_file?(map()) :: boolean()
    defp regular_file?(%{stat: %{type: :regular}}), do: true
    defp regular_file?(_), do: false

    # Optionally test if file is empty (size == 0)
    # Filter that checks if the file size is greater than or equal to the min size.
    @spec min_size(map(), map()) :: boolean()
    defp min_size(%{stat: stat}, %{min_size: min_size}) when stat.size >= min_size, do: true
    defp min_size(_, _), do: false

    # Filter to skip new files
    @spec by_age(map(), integer(), integer()) :: boolean()
    defp by_age(%{path: path, stat: stat}, now, min_age) do
      if age_in_seconds(stat.mtime, now) > min_age do
        true
      else
        Logger.debug("Skipping new file #{path}")
        false
      end
    end

    # Get age in seconds
    defp age_in_seconds(datetime, now) do
      now - :calendar.datetime_to_gregorian_seconds(datetime)
    end

    @spec process_successful_message(Broadway.Message.t(), map()) :: :ok
    defp process_successful_message(message, state) do
      %{archive_dir: archive_dir, datetime_pattern: pattern} = state.config
      event = message.metadata
      %{name: name, path: path} = event

      dest_path =
        case filename_to_datetime(path, pattern) do
          {:ok, datetime} ->
            filename_to_datetime(path, pattern)
            datetime_path = datetime_to_path(datetime)
            File.mkdir_p!(Path.join(archive_dir, datetime_path))
            Path.join([archive_dir, datetime_path, name])

          {:error, :no_match} ->
            # TODO: Use stat mtime to create a datetime path instead?
            Logger.warning("Filename #{path} does not match datetime pattern #{inspect(pattern)}")
            Path.join([archive_dir, name])
        end

      Logger.debug("Moving file #{path} to archive #{dest_path}")
      :ok = File.rename(path, dest_path)

      :ets.delete(state.state_tab, path)

      :ok
    end

    @spec process_failed_message(Broadway.Message.t(), map()) :: Broadway.Message.t() | []
    defp process_failed_message(message, state) do
      %{failed_dir: failed_dir, datetime_pattern: pattern, tries: tries} = state.config

      event = message.metadata
      %{path: path} = event

      case :ets.lookup(state.state_tab, path) do
        [] ->
          # This should not happen
          Logger.warning("File not found in state table, retrying: #{path}")
          :ets.insert(state.state_tab, {path, %{try: 1}})
          message

        [{_path, %{try: try}}] when try <= tries ->
          Logger.info("Retrying file (try #{try + 1}): #{path}")
          :ets.insert(state.state_tab, {path, %{try: try + 1}})
          message

        [{_path, %{try: try}}] ->
          {:ok, datetime} = filename_to_datetime(path, pattern)
          datetime_path = datetime_to_path(datetime)
          dest_path = Path.join([failed_dir, datetime_path, Path.basename(path)])

          Logger.info("File tries exceeded (#{try}), moving #{path} to failed #{dest_path}")

          File.mkdir_p!(Path.join(failed_dir, datetime_path))
          :ok = File.rename(path, dest_path)
          :ets.delete(state.state_tab, path)
          []
      end
    end

    # Extract datetime from filename using Regex pattern
    @spec get_datetime_from_filename(map(), Regex.t()) :: map()
    defp get_datetime_from_filename(rec, pattern) do
      %{path: path} = rec

      case filename_to_datetime(path, pattern) do
        {:ok, datetime} ->
          datetime_path = datetime_to_path(datetime)
          Map.merge(rec, %{datetime: datetime, datetime_path: datetime_path})

        {:error, :no_match} ->
          Logger.warning("Filename #{path} does not match datetime pattern #{inspect(pattern)}")
          rec
      end
    end

    # Get datetime from filename using Regex pattern
    @spec filename_to_datetime(binary(), Regex.t()) :: {:ok, DateTime.t()} | {:error, :no_match}
    defp filename_to_datetime(filename, pattern) do
      case Regex.named_captures(pattern, filename) do
        nil ->
          {:error, :no_match}

        named_captures ->
          {:ok, date} =
            Date.new(
              String.to_integer(named_captures["year"]),
              String.to_integer(named_captures["month"]),
              String.to_integer(named_captures["day"])
            )

          {:ok, time} =
            Time.new(
              capture_to_integer(named_captures["hour"]),
              capture_to_integer(named_captures["min"]),
              capture_to_integer(named_captures["sec"]),
              0
            )

          DateTime.new(date, time, "Etc/UTC")
      end
    end

    # Convert string to integer, returning default on failure
    @spec capture_to_integer(binary() | nil, integer()) :: integer()
    defp capture_to_integer(value, default \\ 0)
    defp capture_to_integer(nil, default), do: default

    defp capture_to_integer(value, default) do
      case Integer.parse(value) do
        {int, _} -> int
        :error -> default
      end
    end

    # Format datetime as path string "YYYY/MM/DD"
    @spec datetime_to_path(DateTime.t()) :: binary()
    defp datetime_to_path(datetime) do
      year = datetime.year |> Integer.to_string() |> String.pad_leading(4, "0")
      month = datetime.month |> Integer.to_string() |> String.pad_leading(2, "0")
      day = datetime.day |> Integer.to_string() |> String.pad_leading(2, "0")

      Path.join([year, month, day])
    end
  end

  defp name(args) do
    case Keyword.fetch(args, :broadway) do
      {:ok, config} ->
        config[:name]

      :error ->
        args[:name] || __MODULE__
    end
  end

  # Manually trigger garbage collection to clear refc binary memory
  @spec maybe_garbage_collect() :: :ok
  defp maybe_garbage_collect do
    case :recon.info(self(), :binary_memory) do
      {:binary_memory, binary} when binary > @max_binary_memory ->
        Logger.debug("Forcing garbage collection")
        :erlang.garbage_collect(self())

      _ ->
        :ok
    end
  end
end
