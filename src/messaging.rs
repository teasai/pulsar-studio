use crate::model::{AuthKind, MessageRow, Server};
use anyhow::{anyhow, Result};
use futures::StreamExt;
use pulsar::consumer::InitialPosition;
use pulsar::{producer, Authentication, Consumer, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use std::collections::HashMap;
use std::time::Duration;

pub type Client = Pulsar<TokioExecutor>;

pub async fn connect(server: &Server) -> Result<Client> {
    let mut builder = Pulsar::builder(&server.broker_url, TokioExecutor);
    match server.auth {
        AuthKind::Jwt => {
            builder = builder.with_auth(Authentication {
                name: "token".to_string(),
                data: server.token.trim().as_bytes().to_vec(),
            });
        }
        AuthKind::Basic => {
            let body = serde_json::json!({
                "userId": server.username,
                "password": server.password,
            })
            .to_string();
            builder = builder.with_auth(Authentication {
                name: "basic".to_string(),
                data: body.into_bytes(),
            });
        }
        AuthKind::None => {}
    }
    Ok(builder.build().await?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    Earliest,
    Latest,
    MessageId(i64, i64),
}

/// Read up to `count` messages from a topic using a temporary subscription,
/// which is removed again once the messages have been sampled.
pub async fn peek(
    client: &Client,
    topic: &str,
    count: usize,
    position: StartPosition,
) -> Result<Vec<MessageRow>> {
    let subscription = format!(
        "pulsar-studio-peek-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    );
    let options = match position {
        StartPosition::Earliest => ConsumerOptions::default()
            .durable(false)
            .with_initial_position(InitialPosition::Earliest),
        StartPosition::Latest => ConsumerOptions::default()
            .durable(false)
            .with_initial_position(InitialPosition::Latest),
        StartPosition::MessageId(ledger, entry) => {
            let mut mid = pulsar::message::proto::MessageIdData::default();
            mid.ledger_id = ledger.max(0) as u64;
            mid.entry_id = entry.max(0) as u64;
            ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Earliest)
                .starting_on_message(mid)
        }
    };

    let mut consumer: Consumer<Vec<u8>, TokioExecutor> = client
        .consumer()
        .with_topic(topic)
        .with_consumer_name("pulsar-studio")
        .with_subscription(&subscription)
        .with_subscription_type(SubType::Exclusive)
        .with_options(options)
        .build::<Vec<u8>>()
        .await?;

    let mut rows = Vec::new();
    // 定位到老 ledger 时 broker 需要回查 BookKeeper，首条等待默认 10 秒（可用 PS_PEEK_WAIT_MS 覆盖）
    let first_wait_ms = std::env::var("PS_PEEK_WAIT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10_000);
    let first_deadline = tokio::time::Instant::now() + Duration::from_millis(first_wait_ms);
    let idle = Duration::from_millis(2000);
    while rows.len() < count {
        let wait = if rows.is_empty() {
            first_deadline.saturating_duration_since(tokio::time::Instant::now())
        } else {
            idle
        };
        if wait.is_zero() {
            break;
        }
        match tokio::time::timeout(wait, consumer.next()).await {
            Ok(Some(Ok(msg))) => {
                let meta = &msg.payload.metadata;
                let properties: Vec<(String, String)> = meta
                    .properties
                    .iter()
                    .map(|kv| (kv.key.clone(), kv.value.clone()))
                    .collect();
                let id = msg.message_id();
                rows.push(MessageRow {
                    index: rows.len() + 1,
                    message_id: format!(
                        "{}:{}:{}",
                        id.ledger_id,
                        id.entry_id,
                        id.partition.unwrap_or(-1)
                    ),
                    publish_time: meta.publish_time as i64,
                    key: meta.partition_key.clone().unwrap_or_default(),
                    properties,
                    payload: msg.payload.data.clone(),
                });
            }
            Ok(Some(Err(e))) => return Err(anyhow!("订阅出错: {e}")),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let _ = consumer.unsubscribe().await;
    Ok(rows)
}

pub struct SearchOptions {
    pub needle: String,
    pub from: StartPosition,
    pub max_scan: u64,
    pub max_hits: usize,
    pub time_limit_secs: u64,
}

/// 全量扫描历史消息并按关键字匹配（payload / key / message id / 属性）。
/// 返回 (命中行, 已扫描条数, 是否因上限/超时截断)
pub async fn search(
    client: &Client,
    topic: &str,
    opts: SearchOptions,
    cancel: std::sync::Arc<std::sync::atomic::AtomicBool>,
    mut on_progress: impl FnMut(u64, usize),
) -> Result<(Vec<MessageRow>, u64, bool)> {
    use std::sync::atomic::Ordering;
    let subscription = format!(
        "pulsar-studio-search-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_millis()
    );
    let options = match opts.from {
        StartPosition::Earliest => ConsumerOptions::default()
            .durable(false)
            .with_initial_position(InitialPosition::Earliest),
        StartPosition::Latest => ConsumerOptions::default()
            .durable(false)
            .with_initial_position(InitialPosition::Latest),
        StartPosition::MessageId(ledger, entry) => {
            let mut mid = pulsar::message::proto::MessageIdData::default();
            mid.ledger_id = ledger.max(0) as u64;
            mid.entry_id = entry.max(0) as u64;
            ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Earliest)
                .starting_on_message(mid)
        }
    };

    let mut consumer: Consumer<Vec<u8>, TokioExecutor> = client
        .consumer()
        .with_topic(topic)
        .with_consumer_name("pulsar-studio-search")
        .with_subscription(&subscription)
        .with_subscription_type(SubType::Exclusive)
        .with_options(options)
        .build::<Vec<u8>>()
        .await?;

    let needle = opts.needle.to_lowercase();
    let mut rows: Vec<MessageRow> = Vec::new();
    let mut scanned: u64 = 0;
    let mut truncated = false;
    let started = tokio::time::Instant::now();
    let mut idle_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut last_report = tokio::time::Instant::now();

    while scanned < opts.max_scan && rows.len() < opts.max_hits {
        if cancel.load(Ordering::Relaxed) {
            truncated = true;
            break;
        }
        if started.elapsed() > Duration::from_secs(opts.time_limit_secs.max(5)) {
            truncated = true;
            break;
        }
        let wait = idle_deadline.saturating_duration_since(tokio::time::Instant::now());
        if wait.is_zero() {
            break;
        }
        match tokio::time::timeout(wait, consumer.next()).await {
            Ok(Some(Ok(msg))) => {
                scanned += 1;
                idle_deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
                let meta = &msg.payload.metadata;
                let properties: Vec<(String, String)> = meta
                    .properties
                    .iter()
                    .map(|kv| (kv.key.clone(), kv.value.clone()))
                    .collect();
                let id = msg.message_id();
                let message_id = format!(
                    "{}:{}:{}",
                    id.ledger_id,
                    id.entry_id,
                    id.partition.unwrap_or(-1)
                );
                let key = meta.partition_key.clone().unwrap_or_default();
                let payload = msg.payload.data.clone();

                let hit = (needle.is_empty())
                    || key.to_lowercase().contains(&needle)
                    || message_id.contains(&needle)
                    || String::from_utf8_lossy(&payload)
                        .to_lowercase()
                        .contains(&needle)
                    || properties.iter().any(|(k, v)| {
                        k.to_lowercase().contains(&needle) || v.to_lowercase().contains(&needle)
                    });

                if hit {
                    rows.push(MessageRow {
                        index: rows.len() + 1,
                        message_id,
                        publish_time: meta.publish_time as i64,
                        key,
                        properties,
                        payload,
                    });
                }

                if last_report.elapsed() > Duration::from_millis(200) {
                    on_progress(scanned, rows.len());
                    last_report = tokio::time::Instant::now();
                }
            }
            Ok(Some(Err(e))) => return Err(anyhow!("扫描出错: {e}")),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    if scanned >= opts.max_scan || rows.len() >= opts.max_hits {
        truncated = true;
    }
    on_progress(scanned, rows.len());
    let _ = consumer.unsubscribe().await;
    Ok((rows, scanned, truncated))
}

pub async fn publish(
    client: &Client,
    topic: &str,
    payload: Vec<u8>,
    key: Option<String>,
    properties: HashMap<String, String>,
) -> Result<()> {
    let mut producer = client.producer().with_topic(topic).build().await?;
    let msg = producer::Message {
        payload,
        properties,
        partition_key: key.filter(|k| !k.is_empty()),
        ..Default::default()
    };
    #[allow(deprecated)]
    producer.send(msg).await?;
    let _ = producer.close().await;
    Ok(())
}
