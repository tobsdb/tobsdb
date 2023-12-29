// TODO: consider thiserror crate
#[derive(Debug, Clone)]
pub enum TdbError {
    ConnFailed(String),
    QueryFailed(String),
    Disconnected,
    NoResponse,
}

impl std::fmt::Display for TdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TdbError::ConnFailed(reason) => write!(f, "Connection Failed: {}", reason),
            TdbError::QueryFailed(reason) => write!(f, "Query Failed: {}", reason),
            TdbError::Disconnected => write!(f, "Websocket disconnected"),
            TdbError::NoResponse => write!(f, "No response received"),
        }
    }
}
