use bytes::BufMut;

#[cfg(feature = "client")]
pub(crate) async fn connect(addr: impl smol::net::AsyncToSocketAddrs) -> std::io::Result<(impl mqtt3::io::PacketStream, impl mqtt3::io::PacketSink)> {
    let stream = smol::net::TcpStream::connect(addr).await?;

    let stream = stream.into_std()?;
    let sink = stream.try_clone()?;

    let stream = IoStream {
        io: smol::net::TcpStream::from_std(stream)?,
        decoder: Default::default(),
        read_state: super::ReadState::WaitingForMore(bytes::BytesMut::with_capacity(1024)),
    };

    let sink = IoSink {
        io: smol::net::TcpStream::from_std(sink)?,
        write_state: Default::default(),
        buffer_timeout: None,
    };

    Ok(mqtt3::io::logging(stream, sink))
}

#[cfg(feature = "server")]
pub(crate) struct Listener {
    accept: std::pin::Pin<Box<dyn futures_core::Stream<Item = std::io::Result<smol::net::TcpStream>>>>,
}

#[cfg(feature = "server")]
impl Listener {
    pub(crate) async fn bind(addr: impl smol::net::AsyncToSocketAddrs) -> std::io::Result<Self> {
        let listener = smol::net::TcpListener::bind(addr).await?;
        let accept = Box::pin(futures_util::stream::try_unfold(listener, |listener| async move {
            let (stream, _) = listener.accept().await?;
            Ok(Some((stream, listener)))
        }));
        Ok(Listener { accept })
    }
}

#[cfg(feature = "server")]
impl mqtt3::io::Listener for Listener {
    type PacketStream = mqtt3::io::LoggingStream<IoStream<smol::net::TcpStream>>;
    type PacketSink = mqtt3::io::LoggingSink<IoSink<smol::net::TcpStream>>;

    fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(Self::PacketStream, Self::PacketSink)>> {
        use futures_util::TryStream;

        let stream = match self.accept.as_mut().try_poll_next(cx)? {
            std::task::Poll::Ready(Some(stream)) => stream,
            std::task::Poll::Ready(None) => return std::task::Poll::Ready(Err(std::io::ErrorKind::UnexpectedEof.into())),
            std::task::Poll::Pending => return std::task::Poll::Pending,
        };

        let stream_ = IoStream {
            io: stream.clone(),
            decoder: Default::default(),
            read_state: super::ReadState::WaitingForMore(bytes::BytesMut::with_capacity(1024)),
        };

        let sink = IoSink {
            io: stream,
            write_state: Default::default(),
            buffer_timeout: None,
        };

        std::task::Poll::Ready(Ok(mqtt3::io::logging(stream_, sink)))
    }
}

#[pin_project::pin_project]
pub(crate) struct IoStream<Io> {
    #[pin] io: Io,
    decoder: mqtt3::proto::PacketDecoder,
    read_state: super::ReadState,
}

impl<Io> futures_core::Stream for IoStream<Io> where Io: smol::io::AsyncRead {
    type Item = Result<mqtt3::proto::Packet, mqtt3::proto::DecodeError>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.read_state {
                super::ReadState::WaitingForMore(buf) => {
                    let mut read_buf = as_read_buf(buf);

                    let read = match this.io.as_mut().poll_read(cx, &mut read_buf)? {
                        std::task::Poll::Ready(read) => read,
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    };

                    if read == 0 {
                        return std::task::Poll::Ready(None);
                    }

                    unsafe { buf.advance_mut(read); }
                    *this.read_state = super::ReadState::MightBeEnough(std::mem::take(buf));
                },

                super::ReadState::MightBeEnough(buf) => {
                    if let Some(packet) = mqtt3::proto::decode(this.decoder, buf)? {
                        return std::task::Poll::Ready(Some(Ok(packet)));
                    }

                    *this.read_state = super::ReadState::WaitingForMore(std::mem::take(buf));
                },
            }
        }
    }
}

fn as_read_buf(buf: &'_ mut bytes::BytesMut) -> &'_ mut [u8] {
    if !buf.has_remaining_mut() {
        buf.reserve(buf.capacity());
    }

    let chunk = buf.chunk_mut();

    // tokio converts the UninitSlice directly to [MaybeUninit<u8>] because UninitSlice is repr(transparent) over that type.
    // However this is actually an undocumented implementation detail of UninitSlice, let's not rely on it.
    // Instead we construct it manually from its as_mut_ptr() and len().

    let ptr: *mut std::mem::MaybeUninit<u8> = chunk.as_mut_ptr().cast();
    let len = chunk.len();
    unsafe {
        let read_buf = std::slice::from_raw_parts_mut(ptr, len);
        read_buf.fill(std::mem::MaybeUninit::new(0));

        // TODO: Use std::mem::MaybeUninit::slice_assume_init_mut when that is stabilized.
        let ptr: *mut u8 = read_buf.as_mut_ptr().cast();
        let read_buf = std::slice::from_raw_parts_mut(ptr, len);
        read_buf
    }
}

#[pin_project::pin_project]
pub(crate) struct IoSink<Io> {
    #[pin] io: Io,
    write_state: super::WriteState,
    buffer_timeout: Option<smol::Timer>,
}

impl<Io> futures_sink::Sink<mqtt3::proto::Packet> for IoSink<Io> where Io: smol::io::AsyncWrite {
    type Error = mqtt3::proto::EncodeError;

    fn poll_ready(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        if self.as_mut().project().write_state.prev.len() < super::NUM_IO_SLICES {
            std::task::Poll::Ready(Ok(()))
        }
        else {
            self.poll_flush(cx)
        }
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: mqtt3::proto::Packet) -> Result<(), Self::Error> {
        let this = self.project();
        mqtt3::proto::encode(item, this.write_state)?;
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        if this.write_state.prepare_for_write() {
            if this.write_state.prev.len() < super::NUM_IO_SLICES {
                use std::future::Future;

                let buffer_timeout = this.buffer_timeout.get_or_insert_with(|| smol::Timer::after(super::BUFFER_TIME));
                match std::pin::Pin::new(buffer_timeout).poll(cx) {
                    std::task::Poll::Ready(_) => (),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }

            while this.write_state.prepare_for_write() {
                let mut dst = [std::io::IoSlice::new(b""); super::NUM_IO_SLICES];
                let num_chunks = this.write_state.chunks_vectored(&mut dst);
                match this.io.as_mut().poll_write_vectored(cx, &dst[..num_chunks])? {
                    std::task::Poll::Ready(0) => return std::task::Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into())),
                    std::task::Poll::Ready(written) => this.write_state.advance(written),
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                }
            }

            match this.io.as_mut().poll_flush(cx)? {
                std::task::Poll::Ready(()) => (),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }

        *this.buffer_timeout = None;
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        match self.as_mut().poll_flush(cx)? {
            std::task::Poll::Ready(()) => (),
            std::task::Poll::Pending => return std::task::Poll::Pending,
        }

        match self.project().io.poll_close(cx)? {
            std::task::Poll::Ready(()) => (),
            std::task::Poll::Pending => return std::task::Poll::Pending,
        }

        std::task::Poll::Ready(Ok(()))
    }
}
