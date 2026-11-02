use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthKind {
    None,
    Basic,
    Jwt,
}

impl Default for AuthKind {
    fn default() -> Self {
        AuthKind::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub name: String,
    pub admin_url: String,
    pub broker_url: String,
    #[serde(default)]
    pub auth: AuthKind,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub token: String,
}

impl Default for Server {
    fn default() -> Self {
        Self {
            name: "Local".into(),
            admin_url: "http://localhost:8080".into(),
            broker_url: "pulsar://localhost:6650".into(),
            auth: AuthKind::None,
            username: String::new(),
            password: String::new(),
            token: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NamespaceRef {
    pub tenant: String,
    pub namespace: String,
}

impl NamespaceRef {
    pub fn new(tenant: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            namespace: namespace.into(),
        }
    }

    pub fn full(&self) -> String {
        format!("{}/{}", self.tenant, self.namespace)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicEntry {
    /// `persistent://tenant/ns/topic`
    pub full: String,
    /// `tenant/ns/topic`, partition suffix stripped
    pub short: String,
    pub partitioned: bool,
    pub local_name: String,
}

impl TopicEntry {
    /// REST 路径片段，例如 `persistent/public/default/my-topic`
    pub fn admin_path(&self) -> String {
        self.full.replace("://", "/")
    }

    pub fn from_full(full: &str) -> Self {
        let short = full
            .split_once("://")
            .map(|(_, rest)| rest.to_string())
            .unwrap_or_else(|| full.to_string());
        let local_name = short.rsplit('/').next().unwrap_or(&short).to_string();
        let partitioned = local_name.ends_with("-partition-0");
        let local_name = if partitioned {
            local_name
                .rsplit_once("-partition-")
                .map(|(a, _)| a.to_string())
                .unwrap_or(local_name.clone())
        } else {
            local_name
        };
        Self {
            full: full.to_string(),
            short,
            partitioned,
            local_name,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TopicSummary {
    pub producers: usize,
    pub subscriptions: usize,
    pub msg_rate_in: f64,
    pub msg_rate_out: f64,
    pub storage_size: i64,
    pub msg_backlog: i64,
    pub raw: String,
}

#[derive(Debug, Clone, Default)]
pub struct MessageRow {
    pub index: usize,
    pub message_id: String,
    pub publish_time: i64,
    pub key: String,
    pub properties: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

impl MessageRow {
    pub fn publish_time_text(&self) -> String {
        if self.publish_time <= 0 {
            return "-".into();
        }
        let millis = if self.publish_time > 1_000_000_000_000 {
            self.publish_time
        } else {
            self.publish_time * 1000
        };
        chrono::DateTime::from_timestamp_millis(millis)
            .map(|d| d.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| self.publish_time.to_string())
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubRow {
    pub name: String,
    pub sub_type: String,
    pub msg_rate_out: f64,
    pub backlog: i64,
    pub consumers: usize,
    pub unacked: i64,
}

impl SubRow {
    pub fn from_stats(name: &str, v: &serde_json::Value) -> Self {
        let get_f = |k: &str| v.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
        let get_i = |k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
        Self {
            name: v
                .get("name")
                .and_then(|x| x.as_str())
                .unwrap_or(name)
                .to_string(),
            sub_type: v
                .get("type")
                .and_then(|x| x.as_str())
                .unwrap_or("-")
                .to_string(),
            msg_rate_out: get_f("msgRateOut"),
            backlog: get_i("msgBacklog"),
            consumers: v
                .get("consumers")
                .and_then(|x| x.as_array())
                .map(|a| a.len())
                .unwrap_or(0),
            unacked: get_i("unackedMessages"),
        }
    }
}

impl SubRow {
    /// 从 topic stats 中汇总订阅信息（分区 Topic 会跨分区累加）。
    pub fn collect(stats: &serde_json::Value) -> Vec<SubRow> {
        let mut map: BTreeMap<String, SubRow> = BTreeMap::new();
        let nodes: Vec<&serde_json::Value> =
            if let Some(parts) = stats.get("partitions").and_then(|p| p.as_object()) {
                parts.values().collect()
            } else {
                vec![stats]
            };
        for node in nodes {
            if let Some(subs) = node.get("subscriptions").and_then(|s| s.as_object()) {
                for (name, st) in subs {
                    let row = SubRow::from_stats(name, st);
                    match map.get_mut(&row.name) {
                        Some(existing) => {
                            existing.msg_rate_out += row.msg_rate_out;
                            existing.backlog += row.backlog;
                            existing.consumers += row.consumers;
                            existing.unacked += row.unacked;
                        }
                        None => {
                            map.insert(row.name.clone(), row);
                        }
                    }
                }
            }
        }
        map.into_values().collect()
    }
}

/// 单个分区的统计
#[derive(Debug, Clone, Default)]
pub struct PartitionRow {
    pub name: String,
    pub msg_rate_in: f64,
    pub msg_rate_out: f64,
    pub backlog: i64,
    pub storage: i64,
}

impl PartitionRow {
    pub fn collect(stats: &serde_json::Value) -> Vec<PartitionRow> {
        let Some(parts) = stats.get("partitions").and_then(|p| p.as_object()) else {
            return Vec::new();
        };
        let mut rows: Vec<PartitionRow> = parts
            .iter()
            .map(|(name, v)| PartitionRow {
                name: name.rsplit('/').next().unwrap_or(name).to_string(),
                msg_rate_in: v.get("msgRateIn").and_then(|x| x.as_f64()).unwrap_or(0.0),
                msg_rate_out: v.get("msgRateOut").and_then(|x| x.as_f64()).unwrap_or(0.0),
                backlog: v.get("msgBacklog").and_then(|x| x.as_i64()).unwrap_or(0),
                storage: v.get("storageSize").and_then(|x| x.as_i64()).unwrap_or(0),
            })
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        rows
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProducerRow {
    pub name: String,
    pub rate_in: f64,
    pub throughput_in: f64,
    pub address: String,
    pub version: String,
    pub connected_since: String,
}

impl ProducerRow {
    pub fn collect(stats: &serde_json::Value) -> Vec<ProducerRow> {
        let mut out = Vec::new();
        let nodes: Vec<&serde_json::Value> =
            if let Some(parts) = stats.get("partitions").and_then(|p| p.as_object()) {
                parts.values().collect()
            } else {
                vec![stats]
            };
        for node in nodes {
            if let Some(list) = node
                .get("publishers")
                .or_else(|| node.get("producers"))
                .and_then(|p| p.as_array())
            {
                for p in list {
                    out.push(ProducerRow {
                        name: p
                            .get("producerName")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                        rate_in: p.get("msgRateIn").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        throughput_in: p
                            .get("msgThroughputIn")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0),
                        address: p
                            .get("address")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                        version: p
                            .get("clientVersion")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                        connected_since: p
                            .get("connectedSince")
                            .and_then(|v| v.as_str())
                            .unwrap_or("-")
                            .to_string(),
                    });
                }
            }
        }
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct GlobalTopicRow {
    pub topic: String,
    pub rate_in: f64,
    pub rate_out: f64,
    pub backlog: i64,
    pub storage: i64,
}

impl GlobalTopicRow {
    /// 解析 `/admin/v2/broker-stats/topics` 的嵌套结构：
    /// namespace -> bundle -> domain -> topic -> stats
    pub fn collect(v: &serde_json::Value) -> Vec<GlobalTopicRow> {
        let mut out = Vec::new();
        let Some(nss) = v.as_object() else {
            return out;
        };
        for (_ns, bundles) in nss {
            let Some(bmap) = bundles.as_object() else { continue };
            for (_bundle, domains) in bmap {
                let Some(dmap) = domains.as_object() else { continue };
                for (_domain, topics) in dmap {
                    let Some(tmap) = topics.as_object() else { continue };
                    for (topic, st) in tmap {
                        let mut backlog = 0i64;
                        if let Some(subs) = st.get("subscriptions").and_then(|s| s.as_object()) {
                            for (_n, sub) in subs {
                                backlog += sub
                                    .get("msgBacklog")
                                    .and_then(|x| x.as_i64())
                                    .unwrap_or(0);
                            }
                        }
                        out.push(GlobalTopicRow {
                            topic: topic
                                .split_once("://")
                                .map(|(_, r)| r.to_string())
                                .unwrap_or_else(|| topic.clone()),
                            rate_in: st.get("msgRateIn").and_then(|x| x.as_f64()).unwrap_or(0.0),
                            rate_out: st.get("msgRateOut").and_then(|x| x.as_f64()).unwrap_or(0.0),
                            backlog,
                            storage: st.get("storageSize").and_then(|x| x.as_i64()).unwrap_or(0),
                        });
                    }
                }
            }
        }
        out.sort_by(|a, b| b.backlog.cmp(&a.backlog));
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct LedgerRow {
    pub id: i64,
    pub entries: i64,
    pub size: i64,
    pub offloaded: bool,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, Default)]
pub struct StorageSummary {
    pub entries: i64,
    pub total_size: i64,
    pub state: String,
    pub last_confirmed: String,
    pub waiting_cursors: i64,
    pub ledgers: Vec<LedgerRow>,
    /// 订阅游标位置：订阅名 -> (ledger, entry)
    pub cursors: std::collections::BTreeMap<String, (i64, i64)>,
}

impl StorageSummary {
    pub fn from_value(v: &serde_json::Value) -> Self {
        let g = |k: &str| v.get(k).and_then(|x| x.as_i64()).unwrap_or(0);
        let s = |k: &str| {
            v.get(k)
                .and_then(|x| x.as_str())
                .unwrap_or("-")
                .to_string()
        };
        let ledgers = v
            .get("ledgers")
            .and_then(|l| l.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|l| LedgerRow {
                        id: l.get("ledgerId").and_then(|x| x.as_i64()).unwrap_or(0),
                        entries: l.get("entries").and_then(|x| x.as_i64()).unwrap_or(0),
                        size: l.get("size").and_then(|x| x.as_i64()).unwrap_or(0),
                        offloaded: l
                            .get("offloaded")
                            .and_then(|x| x.as_bool())
                            .unwrap_or(false),
                        under_replicated: l
                            .get("underReplicated")
                            .and_then(|x| x.as_bool())
                            .unwrap_or(false),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut cursors = std::collections::BTreeMap::new();
        if let Some(map) = v.get("cursors").and_then(|c| c.as_object()) {
            for (name, cur) in map {
                if let Some(pos) = cur.get("markDeletePosition").and_then(|x| x.as_str()) {
                    let mut it = pos.split(':');
                    if let (Some(l), Some(e)) = (
                        it.next().and_then(|x| x.parse::<i64>().ok()),
                        it.next().and_then(|x| x.parse::<i64>().ok()),
                    ) {
                        cursors.insert(name.clone(), (l, e));
                    }
                }
            }
        }
        Self {
            entries: g("numberOfEntries"),
            total_size: g("totalSize"),
            state: s("state"),
            last_confirmed: s("lastConfirmedEntry"),
            waiting_cursors: g("waitingCursorsCount"),
            ledgers,
            cursors,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ConsumerRow {
    pub sub: String,
    pub name: String,
    pub address: String,
    pub rate_out: f64,
    pub unacked: i64,
    pub version: String,
    pub connected_since: String,
}

impl ConsumerRow {
    pub fn collect(stats: &serde_json::Value) -> Vec<ConsumerRow> {
        let mut out = Vec::new();
        let nodes: Vec<&serde_json::Value> =
            if let Some(parts) = stats.get("partitions").and_then(|p| p.as_object()) {
                parts.values().collect()
            } else {
                vec![stats]
            };
        for node in nodes {
            if let Some(subs) = node.get("subscriptions").and_then(|s| s.as_object()) {
                for (sub_name, sub) in subs {
                    if let Some(list) = sub.get("consumers").and_then(|c| c.as_array()) {
                        for c in list {
                            out.push(ConsumerRow {
                                sub: sub_name.clone(),
                                name: c
                                    .get("consumerName")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-")
                                    .to_string(),
                                address: c
                                    .get("address")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-")
                                    .to_string(),
                                rate_out: c
                                    .get("msgRateOut")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                unacked: c
                                    .get("unackedMessages")
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(0),
                                version: c
                                    .get("clientVersion")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-")
                                    .to_string(),
                                connected_since: c
                                    .get("connectedSince")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("-")
                                    .to_string(),
                            });
                        }
                    }
                }
            }
        }
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClusterRow {
    pub name: String,
    pub service_url: String,
    pub broker_url: String,
    pub brokers: usize,
    pub peers: Vec<String>,
}
