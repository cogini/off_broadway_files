![test workflow](https://github.com/cogini/off_broadway_files/actions/workflows/test.yml/badge.svg)
[![Module Version](https://img.shields.io/hexpm/v/off_broadway_files.svg)](https://hex.pm/packages/off_broadway_files)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/off_broadway_files)
[![Total Download](https://img.shields.io/hexpm/dt/off_broadway_files.svg)](https://hex.pm/packages/off_broadway_files)
[![License](https://img.shields.io/hexpm/l/off_broadway_files.svg)](https://github.com/cogini/off_broadway_files/blob/master/LICENSE.md)
[![Last Updated](https://img.shields.io/github/last-commit/cogini/off_broadway_files/main)](https://github.com/cogini/off_broadway_files/commits/main)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](CODE_OF_CONDUCT.md)

# off_broadway_files

An Elixir Broadway producer that reads files from a directory.

It periodically looks for new files in a specificed directory, 
passes them to the Broadway pipeline, then moves them to an `archive` directory
on success, or a `failure` directory failure.

It parses the date from the file name to determine where to archive them.
Files look like `traffic-20260108-2049.log`.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `off_broadway_files` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_files, "~> 0.7.0"}
  ]
end
```

Documentation can be found at <https://hexdocs.pm/off_broadway_files>.
