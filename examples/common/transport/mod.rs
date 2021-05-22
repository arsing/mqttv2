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
    prev: std::collections::VecDeque<bytes::Bytes>,
    curr: bytes::BytesMut,
}

impl WriteState {
    /// Returns true if there is something to write.
    fn prepare_for_write(&mut self) -> bool {
        if !self.curr.is_empty() && self.prev.len() < NUM_IO_SLICES {
            self.prev.push_back(self.curr.split().freeze());
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
        }
        assert_eq!(cnt, 0);
    }
}

impl Default for WriteState {
    fn default() -> Self {
        WriteState {
            // prev will receive at least one more packet even when it has NUM_IO_SLICES buffers, so space for more buffers
            prev: std::collections::VecDeque::with_capacity(NUM_IO_SLICES * 2),
            curr: bytes::BytesMut::with_capacity(1024),
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
            self.prev.push_back(std::mem::take(&mut self.curr).freeze());
        }
        self.prev.push_back(src);
    }
}

const NUM_IO_SLICES: usize = 128;
const BUFFER_TIME: std::time::Duration = std::time::Duration::from_millis(500);
