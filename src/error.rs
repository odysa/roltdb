use std::fmt::Display;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidPageType,
}

impl Error {
    fn as_str(&self) -> &str {
        match *self {
            Error::InvalidPageType => "page type is not correct",
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_str(), f)
    }
}
