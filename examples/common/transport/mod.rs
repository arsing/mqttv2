use bytes::{Buf, BufMut};

#[cfg(feature = "transport-smol")]
pub(crate) mod smol;

#[cfg(feature = "transport-tokio")]
pub(crate) mod tokio;

enum ReadState {
    WaitingForMore(bytes::BytesMut),
    MightBeEnough(bytes::BytesMut),
}

struct WriteState {
    prev: std::collections::VecDeque<Bytes>,
    curr: bytes::BytesMut,
    pool: Vec<bytes::BytesMut>,
}

impl WriteState {
    /// Returns true if there is something to write.
    fn prepare_for_write(&mut self) -> bool {
        if !self.curr.is_empty() && self.prev.len() < NUM_IO_SLICES {
            let curr = std::mem::replace(&mut self.curr, self.pool.pop().unwrap_or_default());
            self.prev.push_back(Bytes::Pool(curr));
        }

        !self.prev.is_empty()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [std::io::IoSlice<'a>]) -> usize {
        for (dst, src) in dst.iter_mut().zip(&self.prev) {
            *dst = std::io::IoSlice::new(&**src);
        }
        std::cmp::min(self.prev.len(), dst.len())
    }

    fn advance(&mut self, mut cnt: usize) {
        while let Some(mut buf) = self.prev.pop_front() {
            if cnt < buf.len() {
                buf.advance(cnt);
                self.prev.push_front(buf);
                cnt = 0;
                break;
            }

            cnt -= buf.len();
            if let Bytes::Pool(mut b) = buf {
                if self.pool.len() < NUM_IO_SLICES {
                    b.clear();
                    self.pool.push(b);
                }
            }
        }
        assert_eq!(cnt, 0);
    }
}

impl Default for WriteState {
    fn default() -> Self {
        WriteState {
            // prev will receive at least one more packet even when it has NUM_IO_SLICES buffers, so space for more buffers
            prev: std::collections::VecDeque::with_capacity(NUM_IO_SLICES * 2),
            curr: bytes::BytesMut::with_capacity(32),
            pool: vec![],
        }
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
            let curr = std::mem::replace(&mut self.curr, self.pool.pop().unwrap_or_default());
            self.prev.push_back(Bytes::Pool(curr));
        }
        self.prev.push_back(Bytes::Frozen(src));
    }
}

enum Bytes {
    Frozen(bytes::Bytes),
    Pool(bytes::BytesMut),
}

impl Bytes {
    fn advance(&mut self, cnt: usize) {
        match self {
            Bytes::Frozen(b) => b.advance(cnt),
            Bytes::Pool(b) => b.advance(cnt),
        }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Bytes::Frozen(b) => &**b,
            Bytes::Pool(b) => &**b,
        }
    }
}

const NUM_IO_SLICES: usize = 128;
const BUFFER_TIME: std::time::Duration = std::time::Duration::from_millis(500);
