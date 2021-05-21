pub trait PacketStream: futures_core::Stream<Item = Result<crate::proto::Packet, crate::proto::DecodeError>> {}
impl<S> PacketStream for S where S: futures_core::Stream<Item = Result<crate::proto::Packet, crate::proto::DecodeError>> {}

pub trait PacketSink: futures_sink::Sink<crate::proto::Packet, Error = crate::proto::EncodeError> {}
impl<S> PacketSink for S where S: futures_sink::Sink<crate::proto::Packet, Error = crate::proto::EncodeError> {}

/// This trait provides an I/O object and optional password that a [`Client`] can use.
///
/// The trait is automatically implemented for all [`FnMut`] that return a connection future.
#[cfg(feature = "client")]
pub trait Connector {
    /// The `PacketStream` object.
    type PacketStream: PacketStream;

    /// The `PacketSink` object.
    type PacketSink: PacketSink;

    /// The error type for this I/O object's connection future.
    type Error;

    /// The connection future. Contains the I/O object and optional password.
    type Future: std::future::Future<Output = Result<(Self::PacketStream, Self::PacketSink, Option<crate::proto::ByteStr>), Self::Error>>;

    /// Attempts the connection and returns a [`Future`] that resolves when the connection succeeds.
    fn connect(&mut self) -> Self::Future;
}

#[cfg(feature = "client")]
impl<F, A, St, Si, E> Connector for F
where
    F: FnMut() -> A,
    A: std::future::Future<Output = Result<(St, Si, Option<crate::proto::ByteStr>), E>>,
    St: PacketStream,
    Si: PacketSink,
{
    type PacketStream = St;
    type PacketSink = Si;
    type Error = E;
    type Future = A;

    fn connect(&mut self) -> Self::Future {
        (self)()
    }
}

#[cfg(feature = "server")]
pub trait Listener {
    type PacketStream: PacketStream;
    type PacketSink: PacketSink;

    fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(Self::PacketStream, Self::PacketSink)>>;
}

pub fn logging<St, Si>(stream: St, sink: Si) -> (LoggingStream<St>, LoggingSink<Si>)
where
    St: PacketStream,
    Si: PacketSink,
{
    (LoggingStream(stream), LoggingSink(sink))
}

#[pin_project::pin_project]
pub struct LoggingStream<S>(#[pin] S);

impl<S> futures_core::Stream for LoggingStream<S> where S: PacketStream {
    type Item = Result<crate::proto::Packet, crate::proto::DecodeError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let result = self.project().0.poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(item))) = &result {
            log::trace!("recv {:?}", item);
        }
        result
    }
}

#[pin_project::pin_project]
pub struct LoggingSink<S>(#[pin] S);

impl<S> futures_sink::Sink<crate::proto::Packet> for LoggingSink<S> where S: PacketSink {
    type Error = crate::proto::EncodeError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_ready(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: crate::proto::Packet) -> Result<(), Self::Error> {
        log::trace!("send {:?}", item);
        self.project().0.start_send(item)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().0.poll_close(cx)
    }
}
