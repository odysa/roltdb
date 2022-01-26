use std::fmt::Display;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidPageType,
    TxNotValid,
    PageEmpty,
    InodeOverFlow,
    InvalidInode,
    Error(String),
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Self::Error(s.to_string())
    }
}

impl Error {
    fn as_str(&self) -> &str {
        match *self {
            Error::InvalidPageType => "page type is not correct",
            Error::TxNotValid => "tx is not valid",
            Error::PageEmpty => "page is empty",
            Error::InodeOverFlow => "inode overflow",
            Error::InvalidInode => "inode is not valid type",
            Error::Error(ref s) => &s.as_str(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_str(), f)
    }
}
