defmodule OffBroadwayFiles.Filter.MinSize do
  @moduledoc """
  Producer filter that checks if the file size is greater than or equal to the
  minimum configured size.
  """

  # @behaviour OffBroadwayFiles.Filter
  # @impl true

  @doc "Test if file is a regular file, not a dir or special file"
  @spec test(map(), map()) :: boolean()
  def test(%{stat: %{type: :regular}}, _config), do: true
  def test(_, _config), do: false
end

