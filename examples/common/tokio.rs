use bytes::{Buf, BufMut};

#[cfg(feature = "client")]
pub(crate) async fn connect(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<(impl mqtt3::io::PacketStream, impl mqtt3::io::PacketSink)> {
    let stream = tokio::net::TcpStream::connect(addr).await?;

    let stream = stream.into_std()?;
    let sink = stream.try_clone()?;

    let stream = IoStream {
        io: tokio::net::TcpStream::from_std(stream)?,
        decoder: Default::default(),
        read_state: ReadState::WaitingForMore(bytes::BytesMut::with_capacity(1024)),
    };

    let sink = IoSink {
        io: tokio::net::TcpStream::from_std(sink)?,
        write_state: WriteState {
            in_progress: Default::default(),
            prev: Default::default(),
            curr: bytes::BytesMut::with_capacity(1024),
        },
    };

    Ok(mqtt3::io::logging(stream, sink))
}

#[cfg(feature = "server")]
pub(crate) struct Listener(tokio::net::TcpListener);

#[cfg(feature = "server")]
impl Listener {
    pub(crate) async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Listener(listener))
    }
}

#[cfg(feature = "server")]
impl mqtt3::io::Listener for Listener {
    type PacketStream = mqtt3::io::LoggingStream<IoStream<tokio::net::TcpStream>>;
    type PacketSink = mqtt3::io::LoggingSink<IoSink<tokio::net::TcpStream>>;

    fn poll_accept(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<(Self::PacketStream, Self::PacketSink)>> {
        let stream = match self.0.poll_accept(cx)? {
            std::task::Poll::Ready((stream, _)) => stream,
            std::task::Poll::Pending => return std::task::Poll::Pending,
        };

        // tokio::net::TcpStream doesn't have `try_clone`. It does have `into_split` but that allocates.
        // So convert it to std, `try_clone` it, then convert the two std streams back to tokio streams.
        let stream = stream.into_std()?;
        let sink = stream.try_clone()?;

        let stream = IoStream {
            io: tokio::net::TcpStream::from_std(stream)?,
            decoder: Default::default(),
            read_state: ReadState::WaitingForMore(bytes::BytesMut::with_capacity(1024)),
        };

        let sink = IoSink {
            io: tokio::net::TcpStream::from_std(sink)?,
            write_state: WriteState {
                in_progress: Default::default(),
                prev: Default::default(),
                curr: bytes::BytesMut::with_capacity(1024),
            },
        };

        std::task::Poll::Ready(Ok(mqtt3::io::logging(stream, sink)))
    }
}

#[pin_project::pin_project]
pub(crate) struct IoStream<Io> {
    #[pin] io: Io,
    decoder: mqtt3::proto::PacketDecoder,
    read_state: ReadState,
}

impl<Io> futures_core::Stream for IoStream<Io> where Io: tokio::io::AsyncRead {
    type Item = Result<mqtt3::proto::Packet, mqtt3::proto::DecodeError>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.read_state {
                ReadState::WaitingForMore(buf) => {
                    let mut read_buf = as_read_buf(buf);

                    match this.io.as_mut().poll_read(cx, &mut read_buf)? {
                        std::task::Poll::Ready(()) => (),
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }

                    let read = read_buf.filled().len();
                    if read == 0 {
                        return std::task::Poll::Ready(None);
                    }

                    unsafe { buf.advance_mut(read); }
                    *this.read_state = ReadState::MightBeEnough(std::mem::take(buf));
                },

                ReadState::MightBeEnough(buf) => {
                    if let Some(packet) = mqtt3::proto::decode(this.decoder, buf)? {
                        return std::task::Poll::Ready(Some(Ok(packet)));
                    }

                    *this.read_state = ReadState::WaitingForMore(std::mem::take(buf));
                },
            }
        }
    }
}

enum ReadState {
    WaitingForMore(bytes::BytesMut),
    MightBeEnough(bytes::BytesMut),
}

fn as_read_buf(buf: &'_ mut bytes::BytesMut) -> tokio::io::ReadBuf<'_> {
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
        tokio::io::ReadBuf::uninit(&mut *read_buf)
    }
}

#[pin_project::pin_project]
pub(crate) struct IoSink<Io> {
    #[pin] io: Io,
    write_state: WriteState,
}

impl<Io> futures_sink::Sink<mqtt3::proto::Packet> for IoSink<Io> where Io: tokio::io::AsyncWrite {
    type Error = mqtt3::proto::EncodeError;

    fn poll_ready(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: mqtt3::proto::Packet) -> Result<(), Self::Error> {
        let this = self.project();
        mqtt3::proto::encode(item, this.write_state)?;
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        loop {
            {
                let mut dst = [std::io::IoSlice::new(b""); 64];
                let num_chunks = this.write_state.chunks_vectored(&mut dst);
                if num_chunks > 0 {
                    match this.io.as_mut().poll_write_vectored(cx, &dst[..num_chunks])? {
                        std::task::Poll::Ready(0) => {
                            return std::task::Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into()));
                        },
                        std::task::Poll::Ready(written) => {
                            this.write_state.advance(written);
                        },
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }
            }

            match this.io.as_mut().poll_flush(cx)? {
                std::task::Poll::Ready(()) => (),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }

            if this.write_state.in_progress.is_empty() {
                break;
            }
        }

        std::task::Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        match self.as_mut().poll_flush(cx)? {
            std::task::Poll::Ready(()) => (),
            std::task::Poll::Pending => return std::task::Poll::Pending,
        }

        match self.project().io.poll_shutdown(cx)? {
            std::task::Poll::Ready(()) => (),
            std::task::Poll::Pending => return std::task::Poll::Pending,
        }

        std::task::Poll::Ready(Ok(()))
    }
}

struct WriteState {
    in_progress: std::collections::VecDeque<bytes::Bytes>,
    prev: std::collections::VecDeque<bytes::Bytes>,
    curr: bytes::BytesMut,
}

impl WriteState {
    fn chunks_vectored<'a>(&'a mut self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        if self.in_progress.is_empty() {
            std::mem::swap(&mut self.in_progress, &mut self.prev);
            if !self.curr.is_empty() {
                self.in_progress.push_back(self.curr.split().freeze());
            }
        }

        for (dst, src) in dst.iter_mut().zip(&self.in_progress) {
            *dst = std::io::IoSlice::new(&**src);
        }
        std::cmp::min(self.in_progress.len(), dst.len())
    }

    fn advance(&mut self, mut cnt: usize) {
        while let Some(mut buf) = self.in_progress.pop_front() {
            if cnt < buf.len() {
                buf.advance(cnt);
                self.in_progress.push_front(buf);
                cnt = 0;
                break;
            }

            cnt -= buf.len();
        }
        assert_eq!(cnt, 0);
    }
}

impl mqtt3::proto::ByteBuf for WriteState {
    fn put_u8_bytes(&mut self, n: u8) {
        self.curr.put_u8(n);
    }

    fn put_u16_bytes(&mut self, n: u16) {
        self.curr.put_u16(n);
    }

    fn put_bytes(&mut self, src: bytes::Bytes) {
        if !self.curr.is_empty() {
            self.prev.push_back(std::mem::take(&mut self.curr).freeze());
        }
        self.prev.push_back(src);
    }
}
