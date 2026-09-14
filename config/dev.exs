import Config

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:pid, :file, :line]

config :logger,
  level: :debug
