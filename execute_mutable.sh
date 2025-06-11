#!/bin/bash

# Execute the file located at build/debug_shared/bin/shell
./build/debug_shared/bin/shell

# Example: Pass commands to the shell using a here document
./build/debug_shared/bin/shell <<EOF
echo "Hello from inside the shell"
# Add more commands as needed
EOF