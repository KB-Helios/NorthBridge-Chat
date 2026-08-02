use std::{env, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::sleep,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut port = None;
    let mut lifetime_ms = None;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--port" => {
                port = arguments.next().and_then(|value| value.parse::<u16>().ok());
            }
            "--lifetime-ms" => {
                lifetime_ms = arguments.next().and_then(|value| value.parse::<u64>().ok());
            }
            _ => {}
        }
    }
    let port = port.expect("--port is required");
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    println!("fixture ready secret=fixture-secret");

    let lifetime = async move {
        match lifetime_ms {
            Some(value) => sleep(Duration::from_millis(value)).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(lifetime);

    loop {
        tokio::select! {
            () = &mut lifetime => return Ok(()),
            accepted = listener.accept() => {
                let (mut stream, _) = accepted?;
                tokio::spawn(async move {
                    let mut request = [0_u8; 2048];
                    let _ = stream.read(&mut request).await;
                    let response = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
                    let _ = stream.write_all(response).await;
                    let _ = stream.shutdown().await;
                });
            }
        }
    }
}
