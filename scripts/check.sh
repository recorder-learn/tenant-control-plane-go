#!/bin/sh
set -eu

fmt_out="$(gofmt -l .)"
if [ -n "$fmt_out" ]; then
  echo "not gofmt-formatted:"
  echo "$fmt_out"
  exit 1
fi
echo "gofmt: clean"

go vet ./...
echo "go vet: clean"

go test -race ./... -count=1
echo "All checks passed."
