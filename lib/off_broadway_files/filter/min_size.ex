defmodule OffBroadwayFiles.Filter.MinSize do
  @moduledoc """
  Producer filter that checks if the file size is greater than or equal to the
  minimum configured size.
  """

  # @behaviour OffBroadwayFiles.Filter
  # @impl true

  @spec test(map(), map()) :: boolean()
  def test(event, config)
  def test(%{stat: stat}, %{min_size: min_size}) when stat.size >= min_size, do: true
  def test(_, _), do: false
end

