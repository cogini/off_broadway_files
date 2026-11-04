defmodule OffBroadwayFiles.Pipeline do
  @moduledoc """
  Test pipeline for producer.
  """
  use Broadway

  alias OffBroadwayFiles.Producer

  require Logger

  def start_link(args) do
    Logger.info("#{__MODULE__} init: #{inspect(args)}")

    in_dir = args[:in_dir] || "/tmp/off_broadway_files"
    archive_dir = args[:archive_dir] || Path.join(in_dir, "archive")
    failed_dir = args[:failed_dir] || Path.join(in_dir, "failed")

    processors_concurrency = args[:processors_concurrency] || System.schedulers_online()

    producer_config = [
      in_dir: in_dir,
      archive_dir: archive_dir,
      failed_dir: failed_dir
    ]

    File.mkdir_p!(in_dir)
    File.mkdir_p!(archive_dir)
    File.mkdir_p!(failed_dir)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Producer, producer_config}
      ],
      processors: [
        default: [concurrency: processors_concurrency]
      ]
    )
  end

  @impl true
  def handle_message(_processor_name, message, _context) do
    Logger.info("Received message: #{inspect(message)}")
    message
  end

  # @impl true
  # def handle_batch(batcher, messages, batch_info, _context) do
  #   message_ids = Enum.map(messages, &message_id/1)
  #   Logger.debug("Batch: #{inspect(batcher)} #{inspect(batch_info)} #{inspect(message_ids)}")
  #   messages
  # end

  # @impl true
  # def handle_failed(messages, _context) do
  #   message_ids = Enum.map(messages, &message_id/1)
  #   Logger.error("Failed messages: #{inspect(message_ids)}")
  #   messages
  # end
end
