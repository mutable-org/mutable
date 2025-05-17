#!/bin/bash

# Ensure both scripts are executable
chmod +x configure_command.sh
chmod +x build_command.sh

# Run the scripts in order
./configure_command.sh
./build_command.sh