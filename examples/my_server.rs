use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use async_bincode::AsyncBincodeStream;
use futures_core::Future;
use rand::Rng;
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_tower::pipeline;
use tokio_util::sync::PollSemaphore;
use tower::{buffer::Buffer, Service};
use tracing::{debug, error, info};

#[derive(Debug)]
struct MainService {
    num_times_called: usize,
    user_permits: PollSemaphore,
    permits: Vec<OwnedSemaphorePermit>,
}

impl MainService {
    fn new() -> Self {
        Self {
            num_times_called: 0,
            user_permits: PollSemaphore::new(Arc::new(Semaphore::new(3))),
            permits: vec![],
        }
    }
}

async fn spawn_new_transport<S, T>(s: S) -> Result<u16>
where
    S: Service<T> + Send + 'static,
    <S as Service<T>>::Response: Serialize + Send,
    <S as Service<T>>::Error: fmt::Debug + Send,
    <S as Service<T>>::Future: Send,
    T: for<'a> serde::Deserialize<'a> + Send,
{
    let tcp = TcpListener::bind("127.0.0.1:0").await?;
    let addr = tcp.local_addr()?;

    tokio::spawn(async move {
        // Should have timeout here
        let (stream, peer) = tcp.accept().await.expect("Could not accept on socket");

        info!(?peer, "Peer accepted");

        let transport = AsyncBincodeStream::from(stream).for_async();
        let server = pipeline::server::Server::new(transport, s);

        match server.await {
            Ok(()) => debug!(?peer, "Server stopped"),
            Err(e) => error!("Server stopped with an issue: {:?}", e),
        }
    });

    Ok(addr.port())
}

impl Service<usize> for MainService {
    type Response = u16;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.permits.is_empty() {
            match self.user_permits.poll_acquire(cx) {
                Poll::Ready(permit) => match permit {
                    Some(permit) => {
                        self.permits.push(permit);
                        Ok(()).into()
                    }
                    None => unreachable!("PollSemaphore will never close"),
                },
                Poll::Pending => {
                    info!("Pending");
                    Poll::Pending
                }
            }
        } else {
            Ok(()).into()
        }
    }

    fn call(&mut self, req: usize) -> Self::Future {
        self.num_times_called += 1;
        info!("Main service called {} times", self.num_times_called);

        let _permit = self
            .permits
            .pop()
            .expect("`call` used more than `poll_ready`");

        Box::pin(async move {
            spawn_new_transport(SomeService {
                id: req,
                num_times_called: 0,
                _permit,
            })
            .await
        })
    }
}

#[derive(Debug)]
struct SomeService {
    id: usize,
    num_times_called: usize,
    _permit: OwnedSemaphorePermit,
}

impl Service<String> for SomeService {
    type Response = String;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: String) -> Self::Future {
        self.num_times_called += 1;

        let times = self.num_times_called;
        let wait_ms = rand::thread_rng().gen_range(500..=500);

        let wait_ms = Duration::from_millis(wait_ms);

        Box::pin(async move {
            tokio::time::sleep(wait_ms).await;
            Ok(format!(
                "(String) You said `{}`, I been called {} times",
                req, times
            ))
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Initializing server");

    let tcp = TcpListener::bind("127.0.0.1:1234").await?;

    let main_service = Buffer::new(MainService::new(), 1024);

    let mut clients = 0;
    loop {
        let (stream, peer) = tcp.accept().await.expect("Could not accept on socket");
        let peer_service = main_service.clone();
        clients += 1;

        tokio::spawn(async move {
            info!(?peer, ?clients, "Peer accepted");

            let transport = AsyncBincodeStream::from(stream).for_async();
            let server = pipeline::server::Server::new(transport, peer_service);

            match server.await {
                Ok(()) => info!("Server stopped"),
                Err(e) => error!("Server stopped with an issue: {:?}", e),
            }
        });
    }
}
