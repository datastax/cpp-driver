#!/bin/bash

# Read configuration
CONF_SCRIPT="$(dirname $(readlink -f $0))/config.sh"
. "${CONF_SCRIPT}"

echo "Cleaning..."

# Remove repo directory
echo "Removing repository..."
rm -rf "${REPO_DIR}"




