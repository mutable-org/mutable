#!/bin/bash

# Ensure both scripts are executable
chmod +x configure_command.sh
chmod +x build_command.sh

# Run the scripts in order and log their outputs
./configure_command.sh > configure_command.log 2>&1
./build_command.sh > build_command.log 2>&1