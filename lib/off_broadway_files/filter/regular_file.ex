defmodule OffBroadwayFiles.Filter.RegularFile do
  @moduledoc """
  Producer filter that checks if the file is a regular file, not a dir or special file.
  """

  # @behaviour OffBroadwayFiles.Filter
  # @impl true

  @spec test(map(), map()) :: boolean()
  def test(event, config)
  def test(%{stat: stat}, %{min_size: min_size}) when stat.size >= min_size, do: true
  def test(_, _), do: false
end
