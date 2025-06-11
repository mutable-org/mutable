#!/bin/bash

# Save the original PATH
ORIGINAL_PATH=$PATH

# Define a minimal PATH for the build process
BUILD_PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# Run the build process within pipenv environment (using pipenv run)
echo "Running the build process within pipenv environment..."
pipenv run ./configure_command.sh
pipenv run ./build_command.sh

# Restore the original PATH
export PATH=$ORIGINAL_PATH

# Confirm that the original PATH is restored and you can run pipenv
echo "PATH restored to original."
echo "Running pipenv sync..."
pipenv sync  # Or any other pipenv command to ensure it's still functional
