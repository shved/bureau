require 'shellwords'

begin
  # Open the file 'keys' in read mode
  File.open('keys', 'r') do |file|
    # Iterate through each line in the file
    file.each_line do |line|
      # Strip leading/trailing whitespace from the line
      key = line.strip
      
      # Skip empty lines
      next if key.empty?
      
      # Construct the shell command
      command = %(RUSTFLAGS=-Awarnings cargo run --bin bureau-client -- --command "GET #{Shellwords.escape(key)}")
      
      # Execute the command and direct output to stdout
      system(command)
    end
  end
rescue Errno::ENOENT
  # Handle the case where the file does not exist
  puts "The file 'keys' does not exist. Please create it and try again."
rescue => e
  # Handle other exceptions
  puts "An error occurred: #{e.message}"
end
