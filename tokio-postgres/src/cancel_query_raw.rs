use crate::config::{SslMode, SslNegotiation};
use crate::tls::TlsConnect;
use crate::{Error, connect_tls};
use bytes::BytesMut;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn cancel_query_raw<S, T>(
    stream: S,
    mode: SslMode,
    negotiation: SslNegotiation,
    tls: T,
    has_hostname: bool,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
{
    let mut stream = connect_tls::connect_tls(stream, mode, negotiation, tls, has_hostname).await?;

    let mut buf = BytesMut::new();
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).await.map_err(Error::io)?;
    stream.flush().await.map_err(Error::io)?;
    stream.shutdown().await.map_err(Error::io)?;

    // The server closes the connection once it has handled the request, and until then the
    // request may still be in flight. One that lands after the query it was meant for has
    // finished cancels the next query the client sends instead, so wait for the close, as libpq
    // does. Nothing is expected to arrive, and an error counts as the close: the request has been
    // sent either way.
    let _ = stream.read(&mut [0; 1]).await;

    Ok(())
}
