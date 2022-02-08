use thiserror::Error;

pub type Result<T> = anyhow::Result<T>;

#[derive(Error, Debug)]
pub enum RoltError {
    #[error("invalid page type")]
    InvalidPageType,
    #[error("tx not valid")]
    TxNotValid,
    #[error("page is empty")]
    PageEmpty,
    #[error("inode is overflow")]
    InodeOverFlow,
    #[error("inode is not valid")]
    InvalidInode,
    #[error("bucket has been created")]
    BucketExist,
    #[error("stack empty")]
    StackEmpty,
    #[error("only allow one writable tx")]
    WritableTxNotAllowed,
}

#[macro_export]
macro_rules! Err {
    ($err:expr $(,)?) => {{
        let error = $err;
        Err(anyhow::anyhow!(error))
    }};
}
