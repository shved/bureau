use clap::Parser;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser)]
struct Args {
    /// The command text to send.
    #[clap(short, long)]
    command: String,

    /// The address in the form host:port.
    #[clap(short, long, default_value = "127.0.0.1:12650")]
    address: String,
}

// TODO: Extract client component. Make it hanlde connection reset from server and reconnect.
#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Connect to server.
    let parts: Vec<&str> = args.address.split(':').collect();
    if parts.len() != 2 {
        eprintln!("Invalid address format. Use host:port.");
        return Ok(());
    }
    let mut stream = TcpStream::connect(&args.address).await?;

    // Ensure the command has a trailing newline.
    let mut command = args.command.clone();
    if !command.ends_with('\n') {
        command.push('\n');
    }

    // Send the command.
    stream.write_all(command.as_bytes()).await?;

    // Read the response.
    let mut buffer = vec![0; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);

    println!("Received: {}", response);

    Ok(())
}
