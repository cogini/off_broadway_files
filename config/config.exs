import Config

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:pid, :file, :line]

# metadata: [:pid, :module, :function, :line]

config :logger,
  level: :info

import_config "#{config_env()}.exs"
