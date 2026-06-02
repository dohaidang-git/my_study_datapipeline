#!/usr/bin/env bash

set -euo pipefail

# Git Bash / MSYS on Windows rewrites POSIX-looking arguments such as
# /opt/spark/work-dir before passing them to docker. That breaks commands like
# `docker exec -w /opt/...` because the container receives a host path instead.
# Disable argument conversion only for those shell environments.
case "${OSTYPE:-}" in
  msys*|cygwin*)
    export MSYS_NO_PATHCONV=1
    export MSYS2_ARG_CONV_EXCL='*'
    ;;
esac

case "${MSYSTEM:-}" in
  MINGW*|MSYS*)
    export MSYS_NO_PATHCONV=1
    export MSYS2_ARG_CONV_EXCL='*'
    ;;
esac
