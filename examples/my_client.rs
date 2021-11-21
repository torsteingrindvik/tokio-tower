use anyhow::Result;
use async_bincode::{AsyncBincodeStream, AsyncDestination};
use tokio::net::TcpStream;
use tokio_tower::pipeline;
use tower::{buffer::Buffer, MakeService, Service, ServiceExt};
use tracing::{debug, info};

type HandshakeResponse = u16;
type HandshakeClient = pipeline::Client<
    AsyncBincodeStream<TcpStream, HandshakeResponse, String, AsyncDestination>,
    anyhow::Error,
    String,
>;

type EstablishedClient = pipeline::Client<
    AsyncBincodeStream<TcpStream, String, String, AsyncDestination>,
    anyhow::Error,
    String,
>;

async fn established_call(client: &mut EstablishedClient, request: String) -> Result<String> {
    Ok(client
        .ready()
        .await
        .map_err(|e| anyhow::anyhow!(e))?
        .call(request)
        .await
        .map_err(|e| anyhow::anyhow!(e))?)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Each client goes through the same TCP connection in order to "handshake",
    // i.e. establish a unique connection.
    const NUM_CLIENTS: usize = 9;

    // After a separate connection is established, how many messages loops to do.
    const NUM_MESSAGES: usize = 3;

    // How many rounds to perform the entire dance.
    const NUM_ROUNDS: usize = 3;

    let tx = TcpStream::connect("127.0.0.1:1234").await?;
    let tx = AsyncBincodeStream::from(tx).for_async();

    let handshaker: HandshakeClient = pipeline::Client::new(tx);

    let handshaker = handshaker.and_then(|port: u16| async move {
        let tx = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
        let tx = AsyncBincodeStream::from(tx).for_async();
        let client = pipeline::Client::new(tx);

        Result::<EstablishedClient>::Ok(client)
    });

    let handshaker = Buffer::new(handshaker, 1024);

    for round in 0..NUM_ROUNDS {
        info!(%round, "Starting");
        let mut handles = vec![];

        for index in 0..NUM_CLIENTS {
            let mut handshaker = handshaker.clone();
            let client_id = (round * NUM_CLIENTS) + index;

            let handle = tokio::spawn(async move {
                let mut client = handshaker
                    .ready()
                    .await
                    .expect("Handshaker not ready")
                    .make_service(format!("Client#{}", client_id))
                    .await
                    .expect("Could not create client");

                for _ in 0..NUM_MESSAGES {
                    let response =
                        established_call(&mut client, format!("Hi from client #{}", client_id))
                            .await
                            .expect("Client call should be ok");
                    info!("Response: {}", response);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }
    }
    Ok(())
}
