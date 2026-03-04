#!/bin/bash
set -euo pipefail

# Default: no optional profiles
PROFILES=()

usage() {
  echo "Usage:"
  echo "  ./start.sh                # Core stack only"
  echo "  ./start.sh --load         # Core + load generator"
  echo "  ./start.sh --sim          # Core + simulators"
  echo "  ./start.sh --all          # Core + load + simulators"
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --load)
      PROFILES+=("--profile" "load")
      shift
      ;;
    --sim)
      PROFILES+=("--profile" "sim")
      shift
      ;;
    --all)
      PROFILES+=("--profile" "load" "--profile" "sim")
      shift
      ;;
    *)
      usage
      ;;
  esac
done

echo "Starting infrastructure..."
docker compose "${PROFILES[@]}" up -d --build

echo "Applying Feast repo..."
docker compose run --rm feast-apply

echo "System ready."