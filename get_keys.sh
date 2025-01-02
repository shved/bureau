#!/bin/bash

# Check if the file 'keys' exists
if [[ ! -f "keys" ]]; then
  echo "The file 'keys' does not exist. Please create it and try again."
  exit 1
fi

# Read the file 'keys' line by line
while IFS= read -r line || [[ -n "$line" ]]; do
  # Remove trailing newline and assign to a variable
  key="$line"
  
  RUSTFLAGS=-Awarnings cargo run --bin bureau-client -- --command "GET $key"
done < keys
