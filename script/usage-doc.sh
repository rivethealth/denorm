#!/bin/bash -e
base="$(dirname "$0")"

usage() {
  echo '##' "$1"
  shift
  echo '```sh'
  "$@"
  echo '```'
  echo
}

exec 1>"$base/../doc/usage.md"

echo '# Usage'
echo

usage common denorm --help
usage create-agg denorm create-agg --help
usage create-key denorm create-key --help
usage create-join denorm create-join --help

"$base/../node_modules/.bin/prettier" --write "$base/../doc/usage.md"
