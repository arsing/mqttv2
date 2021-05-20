#[derive(Clone, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct ByteStr(bytes::Bytes);

impl ByteStr {
    #[allow(clippy::declare_interior_mutable_const)]
    pub const EMPTY: Self = Self::from_static("");

    pub const fn from_static(s: &'static str) -> Self {
        ByteStr(bytes::Bytes::from_static(s.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for ByteStr {
    fn as_ref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl std::fmt::Debug for ByteStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = unsafe { std::str::from_utf8_unchecked(&self.0) };
        s.fmt(f)
    }
}

impl std::fmt::Display for ByteStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = unsafe { std::str::from_utf8_unchecked(&self.0) };
        s.fmt(f)
    }
}

impl From<String> for ByteStr {
    fn from(s: String) -> Self {
        ByteStr(s.into())
    }
}

impl std::str::FromStr for ByteStr {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(ByteStr(s.to_owned().into()))
    }
}

impl<'a> PartialEq<&'a str> for ByteStr {
    fn eq(&self, other: &&'a str) -> bool {
        (&*self.0).eq(other.as_bytes())
    }
}

impl std::convert::TryFrom<bytes::Bytes> for ByteStr {
    type Error = std::str::Utf8Error;

    fn try_from(b: bytes::Bytes) -> Result<Self, Self::Error> {
        let _ = std::str::from_utf8(&b)?;
        Ok(ByteStr(b))
    }
}
