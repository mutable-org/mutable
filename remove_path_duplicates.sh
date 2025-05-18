#!/bin/bash

# Script to remove duplicate entries from the PATH environment variable
# with the option to manually exclude certain directories from PATH

# Define a list of directories you want to exclude from the PATH
exclude_paths=(
  "/Users/oliver/google-cloud-sdk/bin"
  "/opt/homebrew/Caskroom/miniconda/base/bin"
  "/Users/oliver/.antigen"
  "/Users/oliver/.antigen/bundles/robbyrussell/oh-my-zsh/plugins/heroku"
  "/Users/oliver/.antigen/bundles/robbyrussell/oh-my-zsh/plugins/lein"
  "/Users/oliver/.antigen/bundles/zsh-users/zsh-syntax-highlighting"
  # Add any other directories you want to exclude
)

# Split the PATH into an array based on ':'
IFS=':' read -r -a path_array <<< "$PATH"

# Create an empty array to hold unique paths
unique_paths=()

# Iterate through the array and add each path to the unique_paths array if it's not already there
for path in "${path_array[@]}"; do
  # Skip any path that matches one of the exclude paths
  skip=false
  for exclude in "${exclude_paths[@]}"; do
    if [[ "$path" == "$exclude" ]]; then
      skip=true
      break
    fi
  done

  # If the path is not to be excluded and is not already in the unique_paths array, add it
  if [[ "$skip" == false && ! " ${unique_paths[@]} " =~ " ${path} " ]]; then
    unique_paths+=("$path")
  fi
done

# Rebuild the PATH variable from the unique paths
new_path=$(IFS=:; echo "${unique_paths[*]}")

# Set the new PATH variable
export PATH="$new_path"

# Optionally, print the new PATH
echo "Updated PATH: $PATH"
