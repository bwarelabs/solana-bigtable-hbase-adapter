#[derive(Clone, Debug)]
pub struct Config {
    pub hbase_host: String,
}

impl Config {
    pub fn from_env() -> Self {
        let hbase_host = std::env::var("HBASE_HOST").unwrap_or_else(|_| "localhost:9090".to_string());
        Self { hbase_host }
    }
}
