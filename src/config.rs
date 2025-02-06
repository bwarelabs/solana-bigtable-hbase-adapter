#[derive(Clone, Debug)]
pub struct Config {
    pub zookeeper_quorum: String,
}

impl Config {
    pub fn from_env() -> Self {
        let zookeeper_quorum =
            std::env::var("ZOOKEEPER_QUORUM").unwrap_or_else(|_| "localhost:2181".to_string()); // Default for local testing
        Self { zookeeper_quorum }
    }
}
