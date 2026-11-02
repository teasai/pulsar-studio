use crate::model::{AuthKind, NamespaceRef, Server, TopicEntry, TopicSummary};
use anyhow::{anyhow, Result};
use base64::Engine;
use reqwest::{Client, Method};
use serde_json::Value;

#[derive(Clone)]
pub struct AdminClient {
    base: String,
    client: Client,
    auth: Option<String>,
}

impl AdminClient {
    pub fn new(server: &Server) -> Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(std::time::Duration::from_secs(20))
            .build()?;
        let auth = match server.auth {
            AuthKind::Basic => {
                let raw = format!("{}:{}", server.username, server.password);
                Some(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
                ))
            }
            AuthKind::Jwt => Some(format!("Bearer {}", server.token.trim())),
            AuthKind::None => None,
        };
        Ok(Self {
            base: server.admin_url.trim_end_matches('/').to_string(),
            client,
            auth,
        })
    }

    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let url = format!("{}{}", self.base, path);
        let mut req = self.client.request(method, &url);
        if let Some(a) = &self.auth {
            req = req.header(reqwest::header::AUTHORIZATION, a);
        }
        if let Some(b) = &body {
            req = req.json(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow!("连接失败: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            let reason = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| {
                    v.get("reason")
                        .and_then(|r| r.as_str())
                        .map(|s| s.to_string())
                })
                .unwrap_or_else(|| text.chars().take(300).collect());
            return Err(anyhow!("HTTP {}: {}", status.as_u16(), reason));
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_str(&text).unwrap_or(Value::String(text)))
    }

    async fn get(&self, path: &str) -> Result<Value> {
        self.send(Method::GET, path, None).await
    }

    pub async fn clusters(&self) -> Result<Vec<String>> {
        Ok(str_list(self.get("/admin/v2/clusters").await?))
    }

    pub async fn tenants(&self) -> Result<Vec<String>> {
        Ok(str_list(self.get("/admin/v2/tenants").await?))
    }

    pub async fn namespaces(&self, tenant: &str) -> Result<Vec<String>> {
        let path = format!("/admin/v2/namespaces/{}", urlencoding::encode(tenant));
        Ok(str_list(self.get(&path).await?))
    }

    /// List topics in one namespace. Both persistent and non-persistent are merged.
    pub async fn topics(&self, ns: &NamespaceRef) -> Result<Vec<TopicEntry>> {
        let t = urlencoding::encode(&ns.tenant);
        let n = urlencoding::encode(&ns.namespace);
        let mut out = Vec::new();
        for domain in ["persistent", "non-persistent"] {
            let path = format!("/admin/v2/{domain}/{t}/{n}");
            if let Ok(v) = self.get(&path).await {
                for full in str_list(v) {
                    out.push(TopicEntry::from_full(&full));
                }
            }
        }
        out.sort_by(|a, b| a.short.cmp(&b.short));
        out.dedup_by(|a, b| a.short == b.short);
        Ok(out)
    }

    pub async fn partitioned_topics(&self, ns: &NamespaceRef) -> Result<Vec<String>> {
        let path = format!(
            "/admin/v2/persistent/{}/{}/partitioned",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        Ok(str_list(self.get(&path).await?))
    }

    pub async fn namespace_policies(&self, ns: &NamespaceRef) -> Result<Value> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        self.get(&path).await
    }

    pub async fn topic_stats(&self, topic: &TopicEntry) -> Result<TopicSummary> {
        let path = format!("/admin/v2/{}/stats", topic.admin_path());
        let v = self.get(&path).await?;
        Ok(summarize_stats(&v))
    }

    pub async fn topic_stats_raw(&self, topic: &TopicEntry) -> Result<String> {
        let path = format!("/admin/v2/{}/stats", topic.admin_path());
        let v = self.get(&path).await?;
        Ok(serde_json::to_string_pretty(&v).unwrap_or_default())
    }

    pub async fn subscriptions(&self, topic: &TopicEntry) -> Result<Vec<String>> {
        let path = format!("/admin/v2/{}/subscriptions", topic.admin_path());
        Ok(str_list(self.get(&path).await?))
    }

    pub async fn create_tenant(&self, tenant: &str, clusters: Vec<String>) -> Result<()> {
        let path = format!("/admin/v2/tenants/{}", urlencoding::encode(tenant));
        self.send(Method::PUT, &path, Some(serde_json::json!({"allowedClusters": clusters})))
            .await?;
        Ok(())
    }

    pub async fn delete_tenant(&self, tenant: &str) -> Result<()> {
        let base = format!("/admin/v2/tenants/{}", urlencoding::encode(tenant));
        match self
            .send(Method::DELETE, &format!("{base}?force=true"), None)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                // 部分 broker 禁止强制删除，退回普通删除
                let msg = e.to_string();
                if msg.contains("405") || msg.to_lowercase().contains("forced") {
                    self.send(Method::DELETE, &base, None).await?;
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn create_namespace(&self, ns: &NamespaceRef) -> Result<()> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        self.send(Method::PUT, &path, Some(serde_json::json!({}))).await?;
        Ok(())
    }

    pub async fn delete_namespace(&self, ns: &NamespaceRef) -> Result<()> {
        let base = format!(
            "/admin/v2/namespaces/{}/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        match self
            .send(Method::DELETE, &format!("{base}?force=true"), None)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("405") || msg.to_lowercase().contains("forced") {
                    self.send(Method::DELETE, &base, None).await?;
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    pub async fn create_topic(&self, topic: &TopicEntry) -> Result<()> {
        let path = format!("/admin/v2/{}", topic.admin_path());
        self.send(Method::PUT, &path, None).await?;
        Ok(())
    }

    pub async fn delete_topic(&self, topic: &TopicEntry) -> Result<()> {
        let path = format!("/admin/v2/{}?force=true", topic.admin_path());
        self.send(Method::DELETE, &path, None).await?;
        Ok(())
    }
}

fn str_list(v: Value) -> Vec<String> {
    match v {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|i| i.as_str().map(|s| s.to_string()))
            .collect(),
        Value::Null => Vec::new(),
        other => other
            .as_str()
            .map(|s| vec![s.to_string()])
            .unwrap_or_default(),
    }
}

fn num(v: &Value, key: &str) -> f64 {
    v.get(key).and_then(|x| x.as_f64()).unwrap_or(0.0)
}

/// Pulsar returns either a plain stats object or `{partitions: {..}, metadata: {..}}`.
pub fn summarize_stats(v: &Value) -> TopicSummary {
    let mut summary = TopicSummary::default();
    summary.raw = serde_json::to_string_pretty(v).unwrap_or_default();

    let stats_nodes: Vec<&Value> = if let Some(parts) = v.get("partitions").and_then(|p| p.as_object()) {
        parts.values().collect()
    } else {
        vec![v]
    };

    for node in stats_nodes {
        summary.msg_rate_in += num(node, "msgRateIn");
        summary.msg_rate_out += num(node, "msgRateOut");
        summary.storage_size += num(node, "storageSize") as i64;
        summary.msg_backlog += num(node, "msgBacklog") as i64;
        if let Some(p) = node
            .get("publishers")
            .or_else(|| node.get("producers"))
            .and_then(|p| p.as_array())
        {
            summary.producers += p.len();
        }
        if let Some(s) = node.get("subscriptions").and_then(|s| s.as_object()) {
            summary.subscriptions += s.len();
        }
    }
    summary
}

// ---- 订阅管理 ----
impl AdminClient {
    pub async fn subscription_stats(&self, topic: &TopicEntry, sub: &str) -> Result<Value> {
        let path = format!(
            "/admin/v2/{}/subscriptions/{}",
            topic.admin_path(),
            urlencoding::encode(sub)
        );
        self.get(&path).await
    }

    pub async fn reset_cursor_to_time(
        &self,
        topic: &TopicEntry,
        sub: &str,
        timestamp_ms: i64,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}/resetcursor/{}",
            topic.admin_path(),
            urlencoding::encode(sub),
            timestamp_ms
        );
        self.send(Method::POST, &path, None).await?;
        Ok(())
    }

    pub async fn reset_cursor_to_id(
        &self,
        topic: &TopicEntry,
        sub: &str,
        ledger: i64,
        entry: i64,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}/resetcursor",
            topic.admin_path(),
            urlencoding::encode(sub)
        );
        self.send(
            Method::POST,
            &path,
            Some(serde_json::json!({"ledgerId": ledger, "entryId": entry})),
        )
        .await?;
        Ok(())
    }

    pub async fn skip_all(&self, topic: &TopicEntry, sub: &str) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}/skip_all",
            topic.admin_path(),
            urlencoding::encode(sub)
        );
        self.send(Method::POST, &path, None).await?;
        Ok(())
    }

    pub async fn skip_messages(&self, topic: &TopicEntry, sub: &str, count: u64) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}/skip/{}",
            topic.admin_path(),
            urlencoding::encode(sub),
            count
        );
        self.send(Method::POST, &path, None).await?;
        Ok(())
    }

    pub async fn unsubscribe(&self, topic: &TopicEntry, sub: &str) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}?force=true",
            topic.admin_path(),
            urlencoding::encode(sub)
        );
        self.send(Method::DELETE, &path, None).await?;
        Ok(())
    }

    pub async fn expire_messages(
        &self,
        topic: &TopicEntry,
        sub: &str,
        seconds: u64,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}/expireMessages/{}",
            topic.admin_path(),
            urlencoding::encode(sub),
            seconds
        );
        self.send(Method::POST, &path, None).await?;
        Ok(())
    }
}

// ---- 命名空间策略 / 权限 ----
impl AdminClient {
    pub async fn set_namespace_policy(
        &self,
        ns: &NamespaceRef,
        policy: &str,
        body: Value,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace),
            policy
        );
        self.send(Method::POST, &path, Some(body)).await?;
        Ok(())
    }

    pub async fn namespace_permissions(&self, ns: &NamespaceRef) -> Result<Value> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/permissions",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        self.get(&path).await
    }

    pub async fn grant_permission(
        &self,
        ns: &NamespaceRef,
        role: &str,
        actions: Vec<String>,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/permissions/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace),
            urlencoding::encode(role)
        );
        self.send(Method::POST, &path, Some(serde_json::json!(actions)))
            .await?;
        Ok(())
    }

    pub async fn revoke_permission(&self, ns: &NamespaceRef, role: &str) -> Result<()> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/permissions/{}",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace),
            urlencoding::encode(role)
        );
        self.send(Method::DELETE, &path, None).await?;
        Ok(())
    }
}

// ---- 消息定位 ----
impl AdminClient {
    /// 按时间戳（毫秒）定位消息 ID，返回 (ledgerId, entryId)
    pub async fn message_id_by_timestamp(
        &self,
        topic: &TopicEntry,
        timestamp_ms: i64,
    ) -> Result<(i64, i64)> {
        let path = format!(
            "/admin/v2/{}/messageid/{}",
            topic.admin_path(),
            timestamp_ms
        );
        let v = self.get(&path).await?;
        let ledger = v.get("ledgerId").and_then(|x| x.as_i64()).unwrap_or(-1);
        let entry = v.get("entryId").and_then(|x| x.as_i64()).unwrap_or(-1);
        if ledger < 0 || entry < 0 {
            return Err(anyhow!("该时间点没有可用消息"));
        }
        Ok((ledger, entry))
    }
}

impl AdminClient {
    /// 取 topic stats；分区 Topic 尚未落盘时回退为逐分区聚合。
    pub async fn stats_value(&self, topic: &TopicEntry) -> Result<Value> {
        match self
            .get(&format!("/admin/v2/{}/stats", topic.admin_path()))
            .await
        {
            Ok(v) => Ok(v),
            Err(base_err) => {
                let parts = self
                    .get(&format!("/admin/v2/{}/partitions", topic.admin_path()))
                    .await
                    .ok()
                    .and_then(|v| v.get("partitions").and_then(|x| x.as_i64()))
                    .unwrap_or(0);
                if parts <= 0 {
                    return Err(base_err);
                }
                let mut map = serde_json::Map::new();
                for i in 0..parts {
                    let path = format!("/admin/v2/{}-partition-{}/stats", topic.admin_path(), i);
                    if let Ok(v) = self.get(&path).await {
                        map.insert(format!("{}-partition-{}", topic.short, i), v);
                    }
                }
                if map.is_empty() {
                    Err(base_err)
                } else {
                    Ok(serde_json::json!({ "partitions": map }))
                }
            }
        }
    }
}

// ---- 集群 / Broker ----
impl AdminClient {
    pub async fn cluster_info(&self, cluster: &str) -> Result<Value> {
        self.get(&format!(
            "/admin/v2/clusters/{}",
            urlencoding::encode(cluster)
        ))
        .await
    }

    pub async fn brokers(&self, cluster: &str) -> Result<Vec<String>> {
        Ok(str_list(
            self.get(&format!(
                "/admin/v2/brokers/{}",
                urlencoding::encode(cluster)
            ))
            .await?,
        ))
    }

    pub async fn broker_version(&self) -> Result<String> {
        let v = self.get("/admin/v2/brokers/version").await?;
        Ok(v.as_str().unwrap_or_default().to_string())
    }
}

// ---- 订阅创建 / 分区创建 ----
impl AdminClient {
    /// 创建订阅；`message_id` 为空表示从最新位置开始。
    pub async fn create_subscription(
        &self,
        topic: &TopicEntry,
        sub: &str,
        message_id: Option<(i64, i64)>,
    ) -> Result<()> {
        let path = format!(
            "/admin/v2/{}/subscription/{}",
            topic.admin_path(),
            urlencoding::encode(sub)
        );
        let body = message_id.map(|(l, e)| serde_json::json!({"ledgerId": l, "entryId": e}));
        self.send(Method::PUT, &path, body).await?;
        Ok(())
    }

    /// 创建分区 Topic（`partitions` 为分区数）。
    pub async fn create_partitioned_topic(&self, topic: &TopicEntry, partitions: i64) -> Result<()> {
        let path = format!("/admin/v2/{}/partitions", topic.admin_path());
        self.send(Method::PUT, &path, Some(serde_json::json!(partitions)))
            .await?;
        Ok(())
    }
}

// ---- Schema / 全局 Topic ----
impl AdminClient {
    pub async fn topic_schema(&self, topic: &TopicEntry) -> Result<Value> {
        let path = format!("/admin/v2/schemas/{}/schema", topic.short);
        self.get(&path).await
    }

    pub async fn all_broker_topics(&self) -> Result<Value> {
        self.get("/admin/v2/broker-stats/topics").await
    }
}

// ---- 内部统计 / 租户 ----
impl AdminClient {
    pub async fn internal_stats(&self, topic: &TopicEntry) -> Result<Value> {
        let path = format!("/admin/v2/{}/internalStats", topic.admin_path());
        self.get(&path).await
    }

    pub async fn tenant_info(&self, tenant: &str) -> Result<Value> {
        self.get(&format!("/admin/v2/tenants/{}", urlencoding::encode(tenant)))
            .await
    }

    pub async fn update_tenant(
        &self,
        tenant: &str,
        clusters: Vec<String>,
        admin_roles: Vec<String>,
    ) -> Result<()> {
        let path = format!("/admin/v2/tenants/{}", urlencoding::encode(tenant));
        self.send(
            Method::POST,
            &path,
            Some(serde_json::json!({
                "allowedClusters": clusters,
                "adminRoles": admin_roles
            })),
        )
        .await?;
        Ok(())
    }
}

// ---- 集群增删改 / Bundles ----
impl AdminClient {
    pub async fn create_cluster(
        &self,
        name: &str,
        service_url: &str,
        broker_url: &str,
        peers: Vec<String>,
    ) -> Result<()> {
        let path = format!("/admin/v2/clusters/{}", urlencoding::encode(name));
        self.send(
            Method::PUT,
            &path,
            Some(serde_json::json!({
                "serviceUrl": service_url,
                "brokerServiceUrl": broker_url,
                "peerClusterNames": peers
            })),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_cluster(&self, name: &str) -> Result<()> {
        let path = format!("/admin/v2/clusters/{}", urlencoding::encode(name));
        self.send(Method::DELETE, &path, None).await?;
        Ok(())
    }

    pub async fn bundles(&self, ns: &NamespaceRef) -> Result<i64> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/bundles",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        let v = self.get(&path).await?;
        Ok(v.get("numBundles").and_then(|x| x.as_i64()).unwrap_or(0))
    }

    pub async fn set_bundles(&self, ns: &NamespaceRef, n: i64) -> Result<()> {
        let path = format!(
            "/admin/v2/namespaces/{}/{}/bundles",
            urlencoding::encode(&ns.tenant),
            urlencoding::encode(&ns.namespace)
        );
        self.send(Method::PUT, &path, Some(serde_json::json!({ "numBundles": n })))
            .await?;
        Ok(())
    }
}
