use std::convert::{TryFrom, TryInto};

/// Strings are prefixed with a two-byte big-endian length and are encoded as utf-8.
///
/// Ref: 1.5.3 UTF-8 encoded strings
#[derive(Clone)]
pub struct ByteStr(bytes::Bytes);

impl ByteStr {
    #[allow(clippy::declare_interior_mutable_const)]
    pub const EMPTY: Self = ByteStr(bytes::Bytes::from_static(b"\x00\x00"));

    pub const fn from_length_prefixed_static(s: &'static str) -> Self {
        ByteStr(bytes::Bytes::from_static(s.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[std::mem::size_of::<u16>()..]
    }

    pub fn len(&self) -> usize {
        u16::from_be_bytes(self.0[..std::mem::size_of::<u16>()].try_into().unwrap()).into()
    }

    pub fn is_empty(&self) -> bool {
        self.0 == b"\x00\x00"[..]
    }

    pub fn decode(src: &mut bytes::BytesMut) -> Result<Option<ByteStr>, super::DecodeError> {
        if src.len() < std::mem::size_of::<u16>() {
            return Ok(None);
        }

        let len: usize =
            u16::from_be_bytes(src[..std::mem::size_of::<u16>()].try_into().unwrap()).into();

        if src.len() < std::mem::size_of::<u16>() + len {
            return Ok(None);
        }

        let s = src.split_to(std::mem::size_of::<u16>() + len).freeze();
        Ok(Some(ByteStr(s)))
    }

    pub fn encode<B>(self, dst: &mut B) where B: super::ByteBuf {
        dst.put_bytes(self.0);
    }
}

impl AsRef<str> for ByteStr {
    fn as_ref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }
}

impl std::fmt::Debug for ByteStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl Default for ByteStr {
    fn default() -> Self {
        ByteStr::EMPTY
    }
}

impl std::fmt::Display for ByteStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}

impl TryFrom<String> for ByteStr {
    type Error = <u16 as TryFrom<usize>>::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        let len = u16::to_be_bytes(s.len().try_into()?);
        let mut s = s.into_bytes();
        s.splice(..0, std::array::IntoIter::new(len));
        Ok(ByteStr(s.into()))
    }
}

impl std::str::FromStr for ByteStr {
    type Err = <Self as TryFrom<String>>::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let len = u16::to_be_bytes(s.len().try_into()?);
        let s: Vec<_> = std::array::IntoIter::new(len).chain(s.as_bytes().iter().copied()).collect();
        Ok(ByteStr(s.into()))
    }
}

impl PartialEq for ByteStr {
    fn eq(&self, other: &Self) -> bool {
        let s: &str = self.as_ref();
        let other: &str = other.as_ref();
        s.eq(other)
    }
}

impl Eq for ByteStr {}

impl PartialOrd for ByteStr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let s: &str = self.as_ref();
        let other: &str = other.as_ref();
        s.partial_cmp(other)
    }
}

impl Ord for ByteStr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let s: &str = self.as_ref();
        let other: &str = other.as_ref();
        s.cmp(other)
    }
}

impl<'a> PartialEq<&'a str> for ByteStr {
    fn eq(&self, other: &&'a str) -> bool {
        self.as_ref().eq(*other)
    }
}

impl std::hash::Hash for ByteStr {
    fn hash<H>(&self, state: &mut H) where H: std::hash::Hasher {
        self.as_ref().hash(state)
    }
}
