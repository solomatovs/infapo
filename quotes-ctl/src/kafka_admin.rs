use std::collections::BTreeMap;
use std::sync::Arc;

use crate::config::Config;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{ClientBuilder, SaslConfig};
use rskafka::record::Record;

pub struct KafkaAdmin {
    broker: String,
    user: String,
    password: String,
    use_tls: bool,
}

impl KafkaAdmin {
    pub fn new(cfg: &Config) -> Self {
        Self {
            broker: cfg.kafka_broker.clone(),
            user: cfg.kafka_user.clone(),
            password: cfg.kafka_password.clone(),
            use_tls: cfg.kafka_tls,
        }
    }

    pub async fn build_client(&self) -> Result<rskafka::client::Client, String> {
        let mut builder = ClientBuilder::new(vec![self.broker.clone()]);

        // SASL PLAIN
        builder = builder.sasl_config(SaslConfig::Plain {
            username: self.user.clone(),
            password: self.password.clone(),
        });

        // TLS (accept self-signed certs)
        if self.use_tls {
            let tls_config = rustls::ClientConfig::builder()
                .with_safe_defaults()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();
            builder = builder.tls_config(Arc::new(tls_config));
        }

        builder
            .build()
            .await
            .map_err(|e| format!("Kafka connect failed: {e}"))
    }

    /// Delete a topic. Ignores "topic not found" errors.
    pub async fn delete_topic(&self, topic: &str) -> Result<(), String> {
        let client = self.build_client().await?;
        let ctrl = client
            .controller_client()
            .map_err(|e| format!("Kafka controller client failed: {e}"))?;

        match ctrl.delete_topic(topic, 10_000).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("UnknownTopicOrPartition") || msg.contains("does not exist") {
                    Ok(())
                } else {
                    Err(format!("Kafka delete topic '{topic}' failed: {msg}"))
                }
            }
        }
    }

    /// Produce messages to a topic (partition 0).
    pub async fn produce_messages(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, chrono::DateTime<chrono::Utc>)>,
    ) -> Result<(), String> {
        let client = self.build_client().await?;
        let partition_client = client
            .partition_client(topic, 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| format!("Kafka partition client failed: {e}"))?;

        // Send in batches of 500 to avoid oversized requests
        for chunk in messages.chunks(500) {
            let records: Vec<Record> = chunk
                .iter()
                .map(|(value, ts)| Record {
                    key: None,
                    value: Some(value.clone()),
                    headers: BTreeMap::new(),
                    timestamp: *ts,
                })
                .collect();
            partition_client
                .produce(records, Compression::NoCompression)
                .await
                .map_err(|e| format!("Kafka produce failed: {e}"))?;
        }

        Ok(())
    }

    /// Create a topic. Ignores "topic already exists" errors.
    pub async fn create_topic(
        &self,
        topic: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), String> {
        let client = self.build_client().await?;
        let ctrl = client
            .controller_client()
            .map_err(|e| format!("Kafka controller client failed: {e}"))?;

        match ctrl
            .create_topic(topic, partitions, replication_factor, 10_000)
            .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("TopicExistsException") || msg.contains("already exists") {
                    Ok(())
                } else {
                    Err(format!("Kafka create topic '{topic}' failed: {msg}"))
                }
            }
        }
    }
}

/// TLS certificate verifier that accepts any certificate (for self-signed certs).
struct NoVerifier;

impl rustls::client::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
