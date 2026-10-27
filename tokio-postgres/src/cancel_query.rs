use crate::client::SocketConfig;
use crate::config::{SslMode, SslNegotiation};
use crate::tls::MakeTlsConnect;
use crate::{Error, Socket, cancel_query_raw, connect_socket};
use std::io;
use tokio::time;

pub(crate) async fn cancel_query<T>(
    config: Option<SocketConfig>,
    ssl_mode: SslMode,
    ssl_negotiation: SslNegotiation,
    mut tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )));
        }
    };

    let tls = tls
        .make_tls_connect(config.hostname.as_deref().unwrap_or(""))
        .map_err(|e| Error::tls(e.into()))?;
    let has_hostname = config.hostname.is_some();

    let socket = connect_socket::connect_socket(
        &config.addr,
        config.port,
        config.connect_timeout,
        config.tcp_user_timeout,
        config.keepalive.as_ref(),
    )
    .await?;

    let cancel = cancel_query_raw::cancel_query_raw(
        socket,
        ssl_mode,
        ssl_negotiation,
        tls,
        has_hostname,
        process_id,
        secret_key,
    );
    match config.connect_timeout {
        // The cancel waits for the server to close the connection, and would wait forever on one
        // that never does, so the rest of the exchange gets the same limit as the connect.
        Some(connect_timeout) => time::timeout(connect_timeout, cancel).await.map_err(|_| {
            Error::io(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for the server to handle the cancel request",
            ))
        })?,
        None => cancel.await,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::NoTls;
    use crate::client::Addr;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    /// How long the server in these tests takes to handle a cancel request.
    const HANDLING: Duration = Duration::from_millis(300);

    /// The cancel request for process 7 with secret key 11: its length, the cancel request
    /// code, the process ID and the secret key, each a big-endian 32-bit integer.
    const REQUEST: [u8; 16] = [0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 7, 0, 0, 0, 11];

    /// Listens for one cancel request and closes its connection `handling` after reading it,
    /// as a server does once it has passed the request on. Returns the port and the request.
    async fn server_handling_a_cancel_in(handling: Duration) -> (u16, JoinHandle<[u8; 16]>) {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 16];
            socket.read_exact(&mut request).await.unwrap();
            time::sleep(handling).await;
            request
        });
        (port, server)
    }

    async fn cancel(port: u16, connect_timeout: Option<Duration>) -> Result<(), Error> {
        let config = SocketConfig {
            addr: Addr::Tcp(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            hostname: None,
            port,
            connect_timeout,
            tcp_user_timeout: None,
            keepalive: None,
        };
        cancel_query(
            Some(config),
            SslMode::Disable,
            SslNegotiation::Postgres,
            NoTls,
            7,
            11,
        )
        .await
    }

    #[tokio::test]
    async fn returns_once_the_server_has_handled_the_request() {
        let (port, server) = server_handling_a_cancel_in(HANDLING).await;

        let started = Instant::now();
        cancel(port, None).await.unwrap();

        assert!(
            started.elapsed() >= HANDLING,
            "returned {:?} after sending the request, before the server closed the connection",
            started.elapsed()
        );
        assert_eq!(server.await.unwrap(), REQUEST);
    }

    #[tokio::test]
    async fn gives_up_on_a_server_that_does_not_close_after_the_connect_timeout() {
        let (port, _server) = server_handling_a_cancel_in(Duration::from_secs(60)).await;

        let started = Instant::now();
        let error = cancel(port, Some(HANDLING)).await.unwrap_err();

        assert!(
            started.elapsed() < Duration::from_secs(10),
            "waited {:?}",
            started.elapsed()
        );
        let cause = std::error::Error::source(&error).and_then(|cause| cause.downcast_ref());
        assert_eq!(
            cause.map(io::Error::kind),
            Some(io::ErrorKind::TimedOut),
            "unexpected error: {error:?}"
        );
    }
}
