#!/bin/bash

for ((i = 0; i < 400; ++i)); do
  chars='1234567890_-#@^&*+=~abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
  n=100

  key=""
  for ((j = 0; j < n; ++j)); do
      key+="${chars:RANDOM%${#chars}:1}"
  done

  value=""
  for ((j = 0; j < n; ++j)); do
      value+="${chars:RANDOM%${#chars}:1}"
  done

  RUSTFLAGS=-Awarnings cargo run --bin bureau-client -- --command "SET $key $value"
done
