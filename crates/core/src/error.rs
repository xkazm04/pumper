#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http engine: {0}")]
    Http(String),
    #[error("browser engine: {0}")]
    Browser(String),
    #[error("claude engine: {0}")]
    Claude(String),
    #[cfg(feature = "storage")]
    #[error("storage: {0}")]
    Storage(#[from] sqlx::Error),
    /// A session-vault profile problem: an unsafe/unusable profile name, or a
    /// profile dir that can't be prepared. Typed so a bad `profile` on a request
    /// is distinguishable from a transport failure.
    #[error("profile: {0}")]
    Profile(String),
    #[error("parse: {0}")]
    Parse(String),
    #[error("config: {0}")]
    Config(String),
    #[error("app: {0}")]
    App(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
