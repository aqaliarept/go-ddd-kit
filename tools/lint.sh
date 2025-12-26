#!/bin/bash
# Lint script that uses golangci-lint with config from tools/configs

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="${SCRIPT_DIR}/.."
CONFIG_PATH="${SCRIPT_DIR}/configs/.golangci.yml"

# Check if config file exists
if [ ! -f "$CONFIG_PATH" ]; then
    echo "Error: Config file not found at $CONFIG_PATH" >&2
    exit 1
fi

# Check if golangci-lint is installed
if ! command -v golangci-lint &> /dev/null; then
    echo "Error: golangci-lint is not installed" >&2
    echo "Please ensure golangci-lint is installed in the devcontainer" >&2
    exit 1
fi

# Change to workspace root
cd "$WORKSPACE_ROOT"

# Check if go.work exists
if [ ! -f "go.work" ]; then
    echo "Error: go.work file not found" >&2
    exit 1
fi

# Extract module paths from go.work
# This parses the "use (" section and extracts directory paths
MODULE_DIRS=$(awk '/^use \(/,/^\)/ { if (/^[[:space:]]+\.\//) { gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print } }' go.work)

if [ -z "$MODULE_DIRS" ]; then
    echo "Error: No modules found in go.work" >&2
    exit 1
fi

# Run golangci-lint on each module directory
EXIT_CODE=0
for MODULE_DIR in $MODULE_DIRS; do
    if [ -d "$MODULE_DIR" ]; then
        echo "Linting $MODULE_DIR..."
        if ! golangci-lint run --config "$CONFIG_PATH" "$MODULE_DIR/..." "$@"; then
            EXIT_CODE=1
        fi
    else
        echo "Warning: Module directory $MODULE_DIR not found, skipping..." >&2
    fi
done

exit $EXIT_CODE

