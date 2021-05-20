use crate::proto::Packet;

#[pin_project::pin_project]
#[derive(Debug)]
pub(crate) struct LoggingFramed<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    #[pin] inner: tokio_util::codec::Framed<T, crate::proto::PacketCodec>,
}

impl<T> LoggingFramed<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    pub(crate) fn new(io: T) -> Self {
        LoggingFramed {
            inner: tokio_util::codec::Framed::new(io, Default::default()),
        }
    }
}

impl<T> futures_sink::Sink<Packet> for LoggingFramed<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    type Error = <crate::proto::PacketCodec as tokio_util::codec::Encoder<Packet>>::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: Packet) -> Result<(), Self::Error> {
        log::trace!(">>> {:?}", item);
        self.project().inner.start_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx)
    }
}

impl<T> futures_core::Stream for LoggingFramed<T>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    type Item = Result<
        <crate::proto::PacketCodec as tokio_util::codec::Decoder>::Item,
        <crate::proto::PacketCodec as tokio_util::codec::Decoder>::Error,
    >;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = self.project().inner.poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(item))) = &result {
            log::trace!("<<< {:?}", item);
        }
        result
    }
}
