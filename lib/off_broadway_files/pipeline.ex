defmodule OffBroadwayFiles.Pipeline do
  use Broadway

  alias OffBroadwayFiles.Producer

  require Logger

  def start_link(args) do
    Logger.info("#{__MODULE__} init: #{inspect(args)}")

    producer_config = [
      in_dir: "/Users/jake/tmp/flow",
      archive_dir: "/Users/jake/tmp/flow/archive",
      failed_dir: "/Users/jake/tmp/flow/failed",
    ]

    File.mkdir_p!(producer_config[:in_dir])
    File.mkdir_p!(producer_config[:archive_dir])
    File.mkdir_p!(producer_config[:failed_dir])

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Producer, producer_config}
      ],
      processors: [
        default: []
      ]
      # processors: [
      #   default: [concurrency: 50]
      # ],
      # batchers: [
      #   s3: [concurrency: 5, batch_size: 10, batch_timeout: 1000]
      # ]
    )
  end

  @impl true
  def handle_message(_processor_name, message, _context) do
    Logger.info("Received message: #{inspect(message)}")
    message
  end
end
