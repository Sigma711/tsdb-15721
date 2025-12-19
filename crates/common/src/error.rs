#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Corrupt(String),
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}
