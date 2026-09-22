defmodule OffBroadwayFiles.Filter.RegularFile do
  @moduledoc """
  Producer filter that checks if the file is a regular file, not a dir or special file.
  """

  # @behaviour OffBroadwayFiles.Filter
  # @impl true

  @spec test(map(), map()) :: boolean()
  def test(%{stat: %{type: :regular}}, _config), do: true
  def test(_, _config), do: false
end
