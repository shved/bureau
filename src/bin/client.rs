use bureau::client::Client;
use bureau::protocol::Request;
use clap::Parser;

#[derive(Parser)]
struct Args {
    /// The command text to send.
    #[clap(short, long)]
    command: String,

    /// The address in the form host:port.
    #[clap(short, long, default_value = "127.0.0.1:12650")]
    address: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let parts: Vec<&str> = args.address.split(':').collect();
    if parts.len() != 2 {
        eprintln!("Invalid address format. Use host:port.");
        return;
    }

    let mut client = Client::connect(args.address.as_str()).await.unwrap();

    match Request::from_string(args.command) {
        Ok(cmd) => {
            let response = client.send(cmd).await.unwrap();
            println!("Received: {}", response);
        }
        Err(e) => {
            println!("Client error: {}", e);
        }
    }
}
