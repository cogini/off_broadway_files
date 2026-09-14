defmodule OffBroadwayFiles.Pipeline do
  use Broadway

  alias OffBroadwayFiles.Producer

  require Logger

  def start_link(args) do
    Logger.info("#{__MODULE__} init: #{inspect(args)}")

    in_dir = args[:in_dir] || "/tmp/off_broadway_files"
    archive_dir = args[:archive_dir] || Path.join(in_dir, "archive")
    failed_dir = args[:failed_dir] || Path.join(in_dir, "failed")

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
        default: [concurrency: 1]
     ]
    )
  end

  @impl true
  def handle_message(_processor_name, message, _context) do
    Logger.info("Received message: #{inspect(message)}")
    message
  end
end
