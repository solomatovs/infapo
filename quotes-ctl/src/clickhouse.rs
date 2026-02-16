use crate::config::Config;

pub struct ChClient {
    http: reqwest::Client,
    base_url: String,
    user: String,
    password: String,
    database: String,
}

impl ChClient {
    pub fn new(cfg: &Config) -> Self {
        let scheme = if cfg.ch_tls { "https" } else { "http" };
        let http = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("failed to create HTTP client");
        Self {
            http,
            base_url: format!("{scheme}://{}:{}", cfg.ch_host, cfg.ch_port),
            user: cfg.ch_user.clone(),
            password: cfg.ch_password.clone(),
            database: cfg.ch_database.clone(),
        }
    }

    /// Execute one or more SQL statements. Returns body text on success.
    pub async fn query(&self, sql: &str) -> Result<String, String> {
        let resp = self
            .http
            .post(&self.base_url)
            .query(&[
                ("user", self.user.as_str()),
                ("password", self.password.as_str()),
                ("database", self.database.as_str()),
            ])
            .body(sql.to_owned())
            .send()
            .await
            .map_err(|e| format!("ClickHouse request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| format!("ClickHouse read body failed: {e}"))?;

        if status.is_success() {
            Ok(body)
        } else {
            Err(body)
        }
    }

    /// Execute a query, return Ok(body) or Err. Convenience wrapper.
    pub async fn exec(&self, sql: &str) -> Result<String, String> {
        self.query(sql).await
    }
}
