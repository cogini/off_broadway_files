defmodule OffBroadwayFiles.Filter.NamePattern do
  @moduledoc """
  Producer filter that checks if the file name matches a Regex pattern.
  """

  @spec test(map(), map()) :: boolean()
  # Allow if no file pattern is configured
  def test(_, %{file_pattern: nil}), do: true
  # Allow if the file name matches the configured pattern
  def test(%{name: name}, %{file_pattern: file_pattern}) do
    Regex.match?(file_pattern, name)
  end
end
