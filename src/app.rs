use crate::admin::AdminClient;
use crate::config::{self, Config};
use crate::decode::{self, Format, ALL_FORMATS};
use crate::fonts;
use crate::i18n::{self, Lang};
use crate::messaging::{self, StartPosition};
use crate::model::{AuthKind, MessageRow, NamespaceRef, Server, SubRow, TopicEntry, TopicSummary};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;

pub enum Event {
    Status(String),
    Error(String),
    Connected {
        tenants: Vec<String>,
        clusters: Vec<String>,
    },
    Namespaces {
        tenant: String,
        list: Vec<String>,
    },
    Topics {
        ns: NamespaceRef,
        list: Vec<TopicEntry>,
    },
    Detail {
        topic: String,
        summary: Box<TopicSummary>,
        raw: String,
        subs: Vec<SubRow>,
        policies: Option<Value>,
        perms: Option<Value>,
    },
    Messages(Vec<MessageRow>),
    MessagesAppended(Vec<MessageRow>),
    SearchProgress {
        scanned: u64,
        hits: usize,
    },
    SearchDone {
        rows: Vec<MessageRow>,
        scanned: u64,
        truncated: bool,
    },
    Published,
    Done(String),
    ClusterInfo {
        rows: Vec<crate::model::ClusterRow>,
        version: String,
    },
    Bundles(i64),
    Schema(Value),
    Storage(Value),
    SubPreview(Vec<MessageRow>),
    TenantInfo {
        tenant: String,
        info: Value,
    },
    GlobalTopics(Vec<crate::model::GlobalTopicRow>),
    Tenants(Vec<String>),
    Clusters(Vec<String>),
    TenantCreated,
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Tab {
    Messages,
    Subscriptions,
    Producers,
    Consumers,
    Schema,
    Storage,
    Policies,
    Stats,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SubStart {
    Latest,
    Earliest,
    MessageId,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StartMode {
    Earliest,
    Latest,
    Time,
    MessageId,
}

#[derive(Clone)]
struct SubPreview {
    sub: String,
    rows: Vec<MessageRow>,
    selected: usize,
    decoded: String,
    loading: bool,
}

#[derive(Clone, PartialEq, Eq)]
enum ClusterDialog {
    Create,
    Edit(String),
}

#[derive(Clone, PartialEq, Eq)]
enum TenantDialog {
    Create,
    Edit(String),
    Delete(String),
}

#[derive(Clone, PartialEq, Eq)]
enum SubOp {
    ResetTime,
    ResetId,
    Skip,
    Expire,
    Unsubscribe,
}

#[derive(Clone)]
struct SubDialog {
    sub: String,
    op: SubOp,
}

#[derive(Default, Clone)]
struct PolicyForm {
    ttl: String,
    ret_time: String,
    ret_size: String,
    backlog_size: String,
    publish_msg: String,
    publish_byte: String,
    subscribe_msg: String,
    dispatch_msg: String,
    dispatch_byte: String,
    max_producers: String,
    max_consumers: String,
    ensemble: String,
    write_q: String,
    ack_q: String,
    dedup: bool,
    replication: String,
}

fn jget<'a>(v: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    Some(cur)
}

fn jnum(v: Option<&Value>) -> String {
    match v {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        _ => String::new(),
    }
}

impl PolicyForm {
    fn apply(&mut self, pol: &Value) {
        self.ttl = jnum(jget(pol, &["message_ttl_in_seconds"]));
        self.ret_time = jnum(jget(pol, &["retention_policies", "retentionTimeInMinutes"]));
        self.ret_size = jnum(jget(pol, &["retention_policies", "retentionSizeInMB"]));
        let bq = jget(pol, &["backlog_quota_map", "default"]);
        self.backlog_size = jnum(
            jget(pol, &["backlog_quota_map", "default", "limit"])
                .or_else(|| bq.and_then(|b| b.get("limitSize"))),
        );
        let pub_rate = jget(pol, &["publishMaxMessageRate"])
            .and_then(|m| m.as_object())
            .and_then(|m| m.values().next());
        if let Some(r) = pub_rate {
            self.publish_msg = jnum(r.get("msgRateIn"));
            self.publish_byte = jnum(r.get("byteRateIn"));
        }
        let sub_rate = jget(pol, &["clusterSubscribeRate"])
            .and_then(|m| m.as_object())
            .and_then(|m| m.values().next());
        if let Some(r) = sub_rate {
            self.subscribe_msg = jnum(r.get("msgRateOut"));
        }
        let dispatch = jget(pol, &["topicDispatchRate"])
            .and_then(|m| m.as_object())
            .and_then(|m| m.values().next());
        if let Some(r) = dispatch {
            self.dispatch_msg = jnum(r.get("dispatchThrottlingRateInMsg"));
            self.dispatch_byte = jnum(r.get("dispatchThrottlingRateInByte"));
        }
        self.max_producers = jnum(jget(pol, &["max_producers_per_topic"]));
        self.max_consumers = jnum(jget(pol, &["max_consumers_per_topic"]));
        self.ensemble = jnum(jget(pol, &["persistence", "bookkeeperEnsemble"]));
        self.write_q = jnum(jget(pol, &["persistence", "bookkeeperWriteQuorum"]));
        self.ack_q = jnum(jget(pol, &["persistence", "bookkeeperAckQuorum"]));
        self.dedup = jget(pol, &["deduplicationEnabled"])
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        self.replication = jget(pol, &["replication_clusters"])
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
    }
}

pub struct PulsarStudio {
    rt: tokio::runtime::Runtime,
    ctx: egui::Context,
    tx: Sender<Event>,
    rx: Receiver<Event>,

    cfg: Config,
    server_idx: usize,
    admin: Option<AdminClient>,
    connected: bool,

    clusters: Vec<String>,
    tenants: Vec<String>,
    ns_cache: BTreeMap<String, Vec<String>>,
    ns_pending: Vec<String>,
    topics_cache: BTreeMap<String, Vec<TopicEntry>>,
    topics_pending: Vec<String>,
    auto_expand: Option<String>,

    selected_ns: Option<NamespaceRef>,
    topics: Vec<TopicEntry>,
    topic_filter: String,
    selected_topic: Option<TopicEntry>,

    summary: Option<TopicSummary>,
    stats_raw: String,
    tab: Tab,

    messages: Arc<Vec<MessageRow>>,
    selected_msg: Option<usize>,
    decoded: String,
    format: Format,
    avro_schema: String,
    peek_count: usize,
    start_mode: StartMode,
    start_time: String,
    start_ledger: String,
    start_entry: String,
    msg_filter: String,
    msg_sort: Option<(u8, bool)>,
    sub_cursors: BTreeMap<String, (i64, i64)>,
    sub_preview: Option<SubPreview>,
    auto_preview_done: bool,
    search_text: String,
    search_cancel: Option<Arc<AtomicBool>>,
    search_active: bool,
    search_scanned: u64,
    search_hits: usize,
    search_truncated: bool,
    partitions: Vec<crate::model::PartitionRow>,

    subs: Vec<SubRow>,
    producers: Vec<crate::model::ProducerRow>,
    consumers: Vec<crate::model::ConsumerRow>,
    storage: Option<crate::model::StorageSummary>,
    new_tenant_roles: String,
    tree_filter: String,
    /// 已在资源树里触发过全库搜索的关键字；为空表示只做本地过滤
    tree_search_term: String,
    new_topic_partitions: String,
    sub_create_dialog: bool,
    new_sub_name: String,
    new_sub_pos: SubStart,
    new_sub_ledger: String,
    new_sub_entry: String,
    policies: Option<Value>,
    pf: PolicyForm,
    perms: Vec<(String, Vec<String>)>,
    perm_role: String,
    perm_produce: bool,
    perm_consume: bool,
    perm_functions: bool,

    status: String,
    busy: usize,
    auto_connected: bool,

    server_dialog: bool,
    edit_server: Option<usize>,
    delete_server_confirm: bool,
    draft: Server,
    publish_dialog: bool,
    publish_payload: String,
    publish_key: String,
    publish_props: String,
    create_topic_dialog: bool,
    new_topic_name: String,
    create_ns_dialog: bool,
    new_ns_name: String,
    new_tenant_name: String,
    sub_dialog: Option<SubDialog>,
    sub_arg1: String,
    sub_arg2: String,
    dark: bool,
    lang: Lang,
    filter_from: String,
    filter_to: String,
    tenant_dialog: Option<TenantDialog>,
    ns_dialog: Option<NamespaceRef>,
    new_tenant: String,
    new_tenant_cluster: String,
    cluster_window: bool,
    cluster_rows: Vec<crate::model::ClusterRow>,
    broker_version: String,
    settings_window: bool,
    cluster_dialog: Option<ClusterDialog>,
    cluster_name: String,
    cluster_service: String,
    cluster_broker: String,
    cluster_peers: String,
    bundles_count: i64,
    schema_info: Option<Value>,
    global_window: bool,
    global_rows: Vec<crate::model::GlobalTopicRow>,
    global_filter: String,
}

impl PulsarStudio {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        fonts::install(&cc.egui_ctx);
        let cfg = config::load();
        let lang = Lang::from_code(&cfg.language);
        let dark = cfg.dark_theme;
        let peek_count = cfg.peek_count.clamp(1, 5000);
        cc.egui_ctx.set_zoom_factor(cfg.font_scale.clamp(0.7, 2.0));
        let (tx, rx) = channel();
        let mut style = (*cc.egui_ctx.style()).clone();
        style.spacing.item_spacing = egui::vec2(8.0, 6.0);
        // 统一字号体系：等宽字体与正文同高，避免表格里出现大小字
        style.text_styles.insert(
            egui::TextStyle::Monospace,
            egui::FontId::monospace(14.0),
        );
        style
            .text_styles
            .insert(egui::TextStyle::Small, egui::FontId::proportional(12.0));
        cc.egui_ctx.set_style(style);

        Self {
            rt: tokio::runtime::Builder::new_multi_thread()
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .expect("tokio runtime"),
            ctx: cc.egui_ctx.clone(),
            tx,
            rx,
            cfg,
            server_idx: 0,
            admin: None,
            connected: false,
            clusters: Vec::new(),
            tenants: Vec::new(),
            ns_cache: BTreeMap::new(),
            ns_pending: Vec::new(),
            topics_cache: BTreeMap::new(),
            topics_pending: Vec::new(),
            auto_expand: None,
            selected_ns: None,
            topics: Vec::new(),
            topic_filter: String::new(),
            selected_topic: None,
            summary: None,
            stats_raw: String::new(),
            tab: Tab::Messages,
            messages: Arc::new(Vec::new()),
            selected_msg: None,
            decoded: String::new(),
            format: Format::Text,
            avro_schema: String::new(),
            peek_count,
            start_mode: StartMode::Earliest,
            start_time: String::new(),
            start_ledger: String::new(),
            start_entry: String::new(),
            msg_filter: String::new(),
            msg_sort: None,
            sub_cursors: BTreeMap::new(),
            sub_preview: None,
            auto_preview_done: false,
            search_text: String::new(),
            search_cancel: None,
            search_active: false,
            search_scanned: 0,
            search_hits: 0,
            search_truncated: false,
            partitions: Vec::new(),
            subs: Vec::new(),
            producers: Vec::new(),
            consumers: Vec::new(),
            storage: None,
            new_tenant_roles: String::new(),
            tree_filter: String::new(),
            tree_search_term: String::new(),
            new_topic_partitions: String::new(),
            sub_create_dialog: false,
            new_sub_name: String::new(),
            new_sub_pos: SubStart::Latest,
            new_sub_ledger: String::new(),
            new_sub_entry: String::new(),
            policies: None,
            pf: PolicyForm::default(),
            perms: Vec::new(),
            perm_role: String::new(),
            perm_produce: true,
            perm_consume: true,
            perm_functions: false,
            status: i18n::t(lang, "就绪", "就緒", "Ready").into(),
            busy: 0,
            auto_connected: false,
            server_dialog: false,
            edit_server: None,
            delete_server_confirm: false,
            draft: Server::default(),
            publish_dialog: false,
            publish_payload: String::new(),
            publish_key: String::new(),
            publish_props: String::new(),
            create_topic_dialog: false,
            new_topic_name: String::new(),
            create_ns_dialog: false,
            new_ns_name: String::new(),
            new_tenant_name: String::new(),
            sub_dialog: None,
            sub_arg1: String::new(),
            sub_arg2: String::new(),
            dark,
            lang,
            filter_from: String::new(),
            filter_to: String::new(),
            tenant_dialog: None,
            ns_dialog: None,
            new_tenant: String::new(),
            new_tenant_cluster: String::new(),
            cluster_window: false,
            cluster_rows: Vec::new(),
            broker_version: String::new(),
            settings_window: false,
            cluster_dialog: None,
            cluster_name: String::new(),
            cluster_service: String::new(),
            cluster_broker: String::new(),
            cluster_peers: String::new(),
            bundles_count: 0,
            schema_info: None,
            global_window: false,
            global_rows: Vec::new(),
            global_filter: String::new(),
        }
    }

    fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.rt.spawn(fut);
    }

    fn notify(&mut self, text: impl Into<String>) {
        self.status = text.into();
        self.debug_log(&self.status.clone());
    }

    /// 设置环境变量 PS_DEBUG_LOG 后，把状态写入该文件，便于无界面排查。
    fn debug_log(&self, line: &str) {
        if let Ok(path) = std::env::var("PS_DEBUG_LOG") {
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let _ = writeln!(f, "{}", line);
            }
        }
    }

    fn current_server(&self) -> Server {
        self.cfg
            .servers
            .get(self.server_idx)
            .cloned()
            .unwrap_or_default()
    }

    fn connect(&mut self) {
        let server = self.current_server();
        let admin = match AdminClient::new(&server) {
            Ok(a) => a,
            Err(e) => {
                self.notify(format!("初始化失败: {e}"));
                return;
            }
        };
        self.admin = Some(admin.clone());
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.notify(format!(
            "{} {} ...",
            i18n::t(self.lang, "正在连接", "正在連線", "Connecting"),
            server.admin_url
        ));
        self.busy += 1;
        self.spawn(async move {
            let clusters = admin.clusters().await;
            let tenants = admin.tenants().await;
            let ev = match (clusters, tenants) {
                (Ok(clusters), Ok(tenants)) => Event::Connected { tenants, clusters },
                (Err(e), _) | (_, Err(e)) => Event::Error(format!("连接失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    fn load_tenant_info(&mut self, tenant: String) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            match admin.tenant_info(&tenant).await {
                Ok(info) => {
                    let _ = tx.send(Event::TenantInfo { tenant, info });
                }
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("获取租户信息失败: {e}")));
                }
            }
            ctx.request_repaint();
        });
    }

    fn update_tenant(&mut self, tenant: String) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let split = |s: &str| -> Vec<String> {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        };
        let clusters = split(&self.new_tenant_cluster);
        let roles = split(&self.new_tenant_roles);
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.update_tenant(&tenant, clusters, roles).await {
                Ok(()) => Event::Status(format!("已更新租户 {tenant}")),
                Err(e) => Event::Error(format!("更新租户失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::TenantCreated);
            ctx.request_repaint();
        });
    }

    fn load_global_topics(&mut self) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.global_window = true;
        self.notify(i18n::t(
            self.lang,
            "正在加载全局 Topic ...",
            "正在載入全域 Topic ...",
            "Loading all topics ...",
        ));
        self.spawn(async move {
            match admin.all_broker_topics().await {
                Ok(v) => {
                    let rows = crate::model::GlobalTopicRow::collect(&v);
                    let _ = tx.send(Event::GlobalTopics(rows));
                }
                Err(e) => {
                    let _ = tx.send(Event::Error(format!("加载全局 Topic 失败: {e}")));
                }
            }
            ctx.request_repaint();
        });
    }

    fn save_cluster(&mut self) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let name = self.cluster_name.trim().to_string();
        if name.is_empty() {
            self.notify(i18n::t(
                self.lang,
                "集群名不能为空",
                "叢集名不能為空",
                "Cluster name required",
            ));
            return;
        }
        let service = self.cluster_service.trim().to_string();
        let broker = self.cluster_broker.trim().to_string();
        let peers: Vec<String> = self
            .cluster_peers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.create_cluster(&name, &service, &broker, peers).await {
                Ok(()) => Event::Status(format!("已保存集群 {name}")),
                Err(e) => Event::Error(format!("保存集群失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("cluster".into()));
            ctx.request_repaint();
        });
    }

    fn delete_cluster(&mut self, name: String) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.delete_cluster(&name).await {
                Ok(()) => Event::Status(format!("已删除集群 {name}")),
                Err(e) => Event::Error(format!("删除集群失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("cluster".into()));
            ctx.request_repaint();
        });
    }

    fn load_cluster_info(&mut self) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let clusters = self.clusters.clone();
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let mut rows = Vec::new();
            for c in clusters {
                let info = admin.cluster_info(&c).await.unwrap_or(Value::Null);
                let brokers = admin.brokers(&c).await.unwrap_or_default();
                let peers = info
                    .get("peerClusterNames")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                rows.push(crate::model::ClusterRow {
                    name: c.clone(),
                    service_url: info
                        .get("serviceUrl")
                        .and_then(|v| v.as_str())
                        .unwrap_or("-")
                        .to_string(),
                    broker_url: info
                        .get("brokerServiceUrl")
                        .and_then(|v| v.as_str())
                        .unwrap_or("-")
                        .to_string(),
                    brokers: brokers.len(),
                    peers,
                });
            }
            let version = admin.broker_version().await.unwrap_or_default();
            let _ = tx.send(Event::ClusterInfo { rows, version });
            ctx.request_repaint();
        });
    }

    fn create_tenant(&mut self) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let name = self.new_tenant.trim().to_string();
        if name.is_empty() {
            return;
        }
        let cluster = if self.new_tenant_cluster.trim().is_empty() {
            self.clusters.first().cloned().unwrap_or_default()
        } else {
            self.new_tenant_cluster.trim().to_string()
        };
        let clusters = if cluster.is_empty() {
            Vec::new()
        } else {
            vec![cluster]
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.create_tenant(&name, clusters).await {
                Ok(()) => Event::Status(format!("已创建租户 {name}")),
                Err(e) => Event::Error(format!("创建租户失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::TenantCreated);
            ctx.request_repaint();
        });
    }

    fn delete_namespace(&mut self, ns: NamespaceRef) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.delete_namespace(&ns).await {
                Ok(()) => Event::Status(format!("已删除命名空间 {}", ns.full())),
                Err(e) => Event::Error(format!("删除命名空间失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::TenantCreated);
            ctx.request_repaint();
        });
    }

    fn delete_tenant(&mut self, tenant: String) {
        let Some(admin) = self.admin.clone() else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.delete_tenant(&tenant).await {
                Ok(()) => Event::Status(format!("已删除租户 {tenant}")),
                Err(e) => Event::Error(format!("删除租户失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::TenantCreated);
            ctx.request_repaint();
        });
    }

    fn load_namespaces(&mut self, tenant: String) {
        if self.ns_cache.contains_key(&tenant) || self.ns_pending.contains(&tenant) {
            return;
        }
        let Some(admin) = self.admin.clone() else { return };
        self.ns_pending.push(tenant.clone());
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.spawn(async move {
            let list = admin.namespaces(&tenant).await.unwrap_or_default();
            let _ = tx.send(Event::Namespaces { tenant, list });
            ctx.request_repaint();
        });
    }

    fn load_topics(&mut self, ns: NamespaceRef) {
        let key = ns.full();
        if self.topics_pending.contains(&key) {
            return;
        }
        let Some(admin) = self.admin.clone() else { return };
        self.topics_pending.push(key);
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.notify(format!(
            "{} {} ...",
            i18n::t(self.lang, "获取 Topic 列表", "取得 Topic 列表", "Loading topics for"),
            ns.full()
        ));
        self.spawn(async move {
            let list = admin.topics(&ns).await.unwrap_or_default();
            let _ = tx.send(Event::Topics { ns, list });
            ctx.request_repaint();
        });
    }

    /// 资源树搜索：拉取所有租户的命名空间，以及各命名空间下的 Topic，
    /// 让「过滤资源」能真正按 Topic 名跨命名空间查找。
    fn tree_search_all(&mut self) {
        let needle = self.tree_filter.trim().to_lowercase();
        if needle.len() < 2 {
            return;
        }
        self.tree_search_term = needle;
        for tenant in self.tenants.clone() {
            match self.ns_cache.get(&tenant).cloned() {
                Some(list) => {
                    for full in list {
                        if self.topics_cache.contains_key(&full) {
                            continue;
                        }
                        let parts: Vec<&str> = full.split('/').collect();
                        let ns = NamespaceRef::new(
                            parts.first().copied().unwrap_or(""),
                            parts.get(1).copied().unwrap_or(""),
                        );
                        self.load_topics(ns);
                    }
                }
                None => self.load_namespaces(tenant),
            }
        }
    }

    fn load_topic_detail(&mut self, topic: TopicEntry) {
        let Some(admin) = self.admin.clone() else { return };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.summary = None;
        self.stats_raw.clear();
        self.policies = None;
        self.perms.clear();
        self.subs.clear();
        self.messages = Arc::new(Vec::new());
        self.selected_msg = None;
        self.decoded.clear();
        self.schema_info = None;
        self.storage = None;
        self.consumers.clear();
        if let Some(c) = &self.search_cancel {
            c.store(true, Ordering::Relaxed);
        }
        self.search_cancel = None;
        self.search_active = false;
        self.search_scanned = 0;
        self.search_hits = 0;
        self.search_truncated = false;

        let parts: Vec<&str> = topic.short.split('/').collect();
        let ns = NamespaceRef::new(
            parts.first().copied().unwrap_or(""),
            parts.get(1).copied().unwrap_or(""),
        );
        let t = topic.clone();
        self.spawn(async move {
            let stats_value = admin.stats_value(&t).await.unwrap_or(Value::Null);
            let summary = crate::admin::summarize_stats(&stats_value);
            let raw = serde_json::to_string_pretty(&stats_value).unwrap_or_default();

            let mut subs = SubRow::collect(&stats_value);
            if subs.is_empty() {
                if let Ok(names) = admin.subscriptions(&t).await {
                    subs = names
                        .into_iter()
                        .map(|name| SubRow {
                            name,
                            ..Default::default()
                        })
                        .collect();
                }
            }

            let policies = admin.namespace_policies(&ns).await.ok();
            let perms = admin.namespace_permissions(&ns).await.ok();
            if let Ok(schema) = admin.topic_schema(&t).await {
                let _ = tx.send(Event::Schema(schema));
            }
            if let Ok(st) = admin.internal_stats(&t).await {
                let _ = tx.send(Event::Storage(st));
            }
            if let Ok(n) = admin.bundles(&ns).await {
                let _ = tx.send(Event::Bundles(n));
            }

            let _ = tx.send(Event::Detail {
                topic: t.short.clone(),
                summary: Box::new(summary),
                raw,
                subs,
                policies,
                perms,
            });
            ctx.request_repaint();
        });
    }

    fn peek_messages(&mut self) {
        let Some(topic) = self.selected_topic.clone() else {
            return;
        };
        let server = self.current_server();
        let admin = self.admin.clone();
        let count = self.peek_count;
        let mode = self.start_mode;
        let ts_text = self.start_time.clone();
        let ledger_text = self.start_ledger.clone();
        let entry_text = self.start_entry.clone();
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.notify(format!(
            "{} {} ...",
            i18n::t(self.lang, "正在读取消息", "正在讀取訊息", "Fetching"),
            count
        ));
        self.spawn(async move {
            let start = match mode {
                StartMode::Earliest => StartPosition::Earliest,
                StartMode::Latest => StartPosition::Latest,
                StartMode::MessageId => match (
                    ledger_text.trim().parse::<i64>(),
                    entry_text.trim().parse::<i64>(),
                ) {
                    (Ok(l), Ok(e)) => StartPosition::MessageId(l, e),
                    _ => {
                        let _ = tx.send(Event::Error(
                            "消息 ID 需要 ledger 与 entry 两个数字".into(),
                        ));
                        ctx.request_repaint();
                        return;
                    }
                },
                StartMode::Time => {
                    let ts = match ts_text.trim().parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = tx.send(Event::Error("时间戳需要 Unix 毫秒数字".into()));
                            ctx.request_repaint();
                            return;
                        }
                    };
                    let Some(admin) = admin else {
                        let _ = tx.send(Event::Error("未连接管理接口，无法按时间定位".into()));
                        ctx.request_repaint();
                        return;
                    };
                    match admin.message_id_by_timestamp(&topic, ts).await {
                        Ok((l, e)) => StartPosition::MessageId(l, e),
                        Err(e) => {
                            let _ =
                                tx.send(Event::Error(format!("按时间定位失败: {e}")));
                            ctx.request_repaint();
                            return;
                        }
                    }
                }
            };

            let ev = match messaging::connect(&server).await {
                Ok(client) => match messaging::peek(&client, &topic.full, count, start).await {
                    Ok(rows) => Event::Messages(rows),
                    Err(e) => Event::Error(format!("读取消息失败: {e}")),
                },
                Err(e) => Event::Error(format!("连接 Broker 失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    /// 从订阅游标（mark-delete 位置）只读预览未消费消息，不创建订阅、不改游标。
    fn preview_subscription(&mut self, sub: String) {
        let Some(topic) = self.selected_topic.clone() else {
            return;
        };
        let Some((ledger, entry)) = self.sub_cursors.get(&sub).copied() else {
            self.notify(i18n::t(
                self.lang,
                "该订阅暂无可用的游标位置（可能从未消费或已被删除）",
                "該訂閱暫無可用的游標位置（可能從未消費或已被刪除）",
                "No cursor position available for this subscription",
            ));
            return;
        };
        let server = self.current_server();
        let count = self.peek_count.clamp(1, 200);
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.sub_preview = Some(SubPreview {
            sub: sub.clone(),
            rows: Vec::new(),
            selected: 0,
            decoded: String::new(),
            loading: true,
        });
        self.busy += 1;
        self.notify(format!(
            "{} {} @ {ledger}:{entry}",
            i18n::t(self.lang, "读取订阅积压", "讀取訂閱積壓", "Reading backlog of"),
            sub
        ));
        self.spawn(async move {
            let ev = match messaging::connect(&server).await {
                Ok(client) => {
                    match messaging::peek(
                        &client,
                        &topic.full,
                        count,
                        StartPosition::MessageId(ledger, entry),
                    )
                    .await
                    {
                        Ok(rows) => Event::SubPreview(rows),
                        Err(e) => Event::Error(format!("读取订阅积压失败: {e}")),
                    }
                }
                Err(e) => Event::Error(format!("连接 Broker 失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    fn start_search(&mut self) {
        let Some(topic) = self.selected_topic.clone() else {
            self.notify(i18n::t(
                self.lang,
                "请先打开一个 Topic",
                "請先開啟一個 Topic",
                "Open a topic first",
            ));
            return;
        };
        // 搜索框为空时，自动沿用「页内过滤」里输入的关键字
        if self.search_text.trim().is_empty() && !self.msg_filter.trim().is_empty() {
            self.search_text = self.msg_filter.clone();
        }
        let needle = self.search_text.trim().to_string();
        if needle.is_empty() {
            self.notify(i18n::t(
                self.lang,
                "请输入搜索关键字",
                "請輸入搜尋關鍵字",
                "Enter a keyword first",
            ));
            return;
        }
        self.debug_log(&format!("search start: {needle} @ {}", topic.short));
        let server = self.current_server();
        let cancel = Arc::new(AtomicBool::new(false));
        self.search_cancel = Some(cancel.clone());
        self.search_active = true;
        self.search_scanned = 0;
        self.search_hits = 0;
        self.search_truncated = false;
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.notify(i18n::t(
            self.lang,
            "开始全库扫描 ...",
            "開始全庫掃描 ...",
            "Scanning history ...",
        ));
        self.spawn(async move {
            let ev = match messaging::connect(&server).await {
                Ok(client) => {
                    let opts = messaging::SearchOptions {
                        needle: needle.clone(),
                        from: StartPosition::Earliest,
                        max_scan: 500_000,
                        max_hits: 500,
                        time_limit_secs: 180,
                    };
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    match messaging::search(
                        &client,
                        &topic.full,
                        opts,
                        cancel,
                        move |scanned, hits| {
                            let _ = tx2.send(Event::SearchProgress { scanned, hits });
                            ctx2.request_repaint();
                        },
                    )
                    .await
                    {
                        Ok((rows, scanned, truncated)) => Event::SearchDone {
                            rows,
                            scanned,
                            truncated,
                        },
                        Err(e) => Event::Error(format!("搜索失败: {e}")),
                    }
                }
                Err(e) => Event::Error(format!("连接 Broker 失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    fn next_page(&mut self) {
        let Some(last) = self.messages.last().cloned() else {
            return;
        };
        let parts: Vec<&str> = last.message_id.split(':').collect();
        if parts.len() < 2 {
            return;
        }
        let (Ok(ledger), Ok(entry)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) else {
            return;
        };
        let Some(topic) = self.selected_topic.clone() else {
            return;
        };
        let server = self.current_server();
        let count = self.peek_count;
        // 从当前最后一条所在位置重新读，随后在追加时去重，避免 ledger 边界丢消息
        let start = StartPosition::MessageId(ledger, entry);
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.notify(i18n::t(
            self.lang,
            "加载下一页 ...",
            "載入下一頁 ...",
            "Loading next page ...",
        ));
        self.spawn(async move {
            let ev = match messaging::connect(&server).await {
                Ok(client) => match messaging::peek(&client, &topic.full, count, start).await {
                    Ok(rows) => Event::MessagesAppended(rows),
                    Err(e) => Event::Error(format!("读取消息失败: {e}")),
                },
                Err(e) => Event::Error(format!("连接 Broker 失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    fn create_subscription(&mut self) {
        let (Some(admin), Some(topic)) = (self.admin.clone(), self.selected_topic.clone()) else {
            return;
        };
        let name = self.new_sub_name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let mid = match self.new_sub_pos {
            SubStart::Latest => None,
            SubStart::Earliest => Some((-1i64, -1i64)),
            SubStart::MessageId => match (
                self.new_sub_ledger.trim().parse::<i64>(),
                self.new_sub_entry.trim().parse::<i64>(),
            ) {
                (Ok(l), Ok(e)) => Some((l, e)),
                _ => {
                    self.notify(i18n::t(
                        self.lang,
                        "起始消息 ID 需要 ledger 与 entry 两个数字",
                        "起始訊息 ID 需要 ledger 與 entry 兩個數字",
                        "Start message id needs ledger and entry",
                    ));
                    return;
                }
            },
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.create_subscription(&topic, &name, mid).await {
                Ok(()) => Event::Status(format!("已创建订阅 {name}")),
                Err(e) => Event::Error(format!("创建订阅失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("sub_action".into()));
            ctx.request_repaint();
        });
    }

    fn publish(&mut self) {
        let Some(topic) = self.selected_topic.clone() else {
            return;
        };
        let server = self.current_server();
        let payload = self.publish_payload.clone().into_bytes();
        let key = Some(self.publish_key.clone()).filter(|k| !k.trim().is_empty());
        let mut props = HashMap::new();
        for line in self.publish_props.lines() {
            if let Some((k, v)) = line.split_once('=') {
                props.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.notify("正在发送消息 ...".to_string());
        self.spawn(async move {
            let ev = match messaging::connect(&server).await {
                Ok(client) => {
                    match messaging::publish(&client, &topic.full, payload, key, props).await {
                        Ok(()) => Event::Published,
                        Err(e) => Event::Error(format!("发送失败: {e}")),
                    }
                }
                Err(e) => Event::Error(format!("连接 Broker 失败: {e}")),
            };
            let _ = tx.send(ev);
            ctx.request_repaint();
        });
    }

    fn create_topic(&mut self) {
        let (Some(admin), Some(ns)) = (self.admin.clone(), self.selected_ns.clone()) else {
            return;
        };
        let name = self.new_topic_name.trim().to_string();
        if name.is_empty() {
            return;
        }
        let full = format!("persistent://{}/{}", ns.full(), name);
        let topic = TopicEntry::from_full(&full);
        let partitions = self
            .new_topic_partitions
            .trim()
            .parse::<i64>()
            .unwrap_or(0);
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = if partitions > 1 {
                match admin.create_partitioned_topic(&topic, partitions).await {
                    Ok(()) => Event::Status(format!(
                        "已创建分区 Topic {} ({} 分区)",
                        topic.short, partitions
                    )),
                    Err(e) => Event::Error(format!("创建失败: {e}")),
                }
            } else {
                match admin.create_topic(&topic).await {
                    Ok(()) => Event::Status(format!("已创建 Topic {}", topic.short)),
                    Err(e) => Event::Error(format!("创建失败: {e}")),
                }
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("create_topic".into()));
            ctx.request_repaint();
        });
    }

    fn delete_topic(&mut self, topic: TopicEntry) {
        let Some(admin) = self.admin.clone() else { return };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.delete_topic(&topic).await {
                Ok(()) => Event::Status(format!("已删除 Topic {}", topic.short)),
                Err(e) => Event::Error(format!("删除失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("delete_topic".into()));
            ctx.request_repaint();
        });
    }

    fn run_sub_action(&mut self) {
        let Some(dialog) = self.sub_dialog.clone() else {
            return;
        };
        let (Some(admin), Some(topic)) = (self.admin.clone(), self.selected_topic.clone()) else {
            return;
        };
        let arg1 = self.sub_arg1.trim().to_string();
        let arg2 = self.sub_arg2.trim().to_string();
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.sub_dialog = None;
        self.spawn(async move {
            let result = match dialog.op {
                SubOp::ResetTime => match arg1.parse::<i64>() {
                    Ok(ts) => admin.reset_cursor_to_time(&topic, &dialog.sub, ts).await,
                    Err(_) => Err(anyhow::anyhow!("时间戳必须是数字（毫秒或秒）")),
                },
                SubOp::ResetId => match (arg1.parse::<i64>(), arg2.parse::<i64>()) {
                    (Ok(l), Ok(e)) => admin.reset_cursor_to_id(&topic, &dialog.sub, l, e).await,
                    _ => Err(anyhow::anyhow!("需要 ledgerId:entryId 两个数字")),
                },
                SubOp::Skip => match arg1.parse::<u64>() {
                    Ok(n) => admin.skip_messages(&topic, &dialog.sub, n).await,
                    Err(_) => Err(anyhow::anyhow!("条数必须是数字")),
                },
                SubOp::Expire => match arg1.parse::<u64>() {
                    Ok(n) => admin.expire_messages(&topic, &dialog.sub, n).await,
                    Err(_) => Err(anyhow::anyhow!("秒数必须是数字")),
                },
                SubOp::Unsubscribe => admin.unsubscribe(&topic, &dialog.sub).await,
            };
            let ev = match result {
                Ok(()) => Event::Status(format!("订阅 {} 操作完成", dialog.sub)),
                Err(e) => Event::Error(format!("订阅操作失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("sub_action".into()));
            ctx.request_repaint();
        });
    }

    fn save_policies(&mut self) {
        let (Some(admin), Some(ns)) = (self.admin.clone(), self.selected_ns.clone()) else {
            return;
        };
        let pf = self.pf.clone();
        let mut jobs: Vec<(String, Value)> = Vec::new();

        if let Ok(v) = pf.ttl.trim().parse::<i64>() {
            jobs.push(("messageTTL".into(), Value::from(v)));
        }
        if let (Ok(t), Ok(s)) = (pf.ret_time.trim().parse::<u64>(), pf.ret_size.trim().parse::<u64>()) {
            jobs.push((
                "retention".into(),
                serde_json::json!({"retentionTimeInMinutes": t, "retentionSizeInMB": s}),
            ));
        }
        if let Ok(v) = pf.backlog_size.trim().parse::<i64>() {
            jobs.push((
                "backlogQuota?backlogQuotaType=destination_storage".into(),
                serde_json::json!({"limitSize": v, "limitTime": 0, "policy": "producer_request_hold"}),
            ));
        }
        if let (Ok(m), Ok(b)) = (pf.publish_msg.trim().parse::<f64>(), pf.publish_byte.trim().parse::<f64>()) {
            jobs.push((
                "publishRate".into(),
                serde_json::json!({"msgRateIn": m, "byteRateIn": b}),
            ));
        }
        if let Ok(v) = pf.subscribe_msg.trim().parse::<f64>() {
            jobs.push((
                "subscribeRate".into(),
                serde_json::json!({"msgRateOut": v, "consumerRate": 0}),
            ));
        }
        if let (Ok(m), Ok(b)) = (pf.dispatch_msg.trim().parse::<f64>(), pf.dispatch_byte.trim().parse::<f64>()) {
            jobs.push((
                "dispatchRate".into(),
                serde_json::json!({
                    "dispatchThrottlingRateInMsg": m,
                    "dispatchThrottlingRateInByte": b,
                    "ratePeriodInSecond": 1
                }),
            ));
        }
        if let Ok(v) = pf.max_producers.trim().parse::<i64>() {
            jobs.push(("maxProducersPerTopic".into(), Value::from(v)));
        }
        if let Ok(v) = pf.max_consumers.trim().parse::<i64>() {
            jobs.push(("maxConsumersPerTopic".into(), Value::from(v)));
        }
        if let (Ok(e), Ok(w), Ok(a)) = (
            pf.ensemble.trim().parse::<i64>(),
            pf.write_q.trim().parse::<i64>(),
            pf.ack_q.trim().parse::<i64>(),
        ) {
            jobs.push((
                "persistence".into(),
                serde_json::json!({
                    "bookkeeperEnsemble": e,
                    "bookkeeperWriteQuorum": w,
                    "bookkeeperAckQuorum": a,
                    "managedLedgerMaxMarkDeleteRate": 0.0
                }),
            ));
        }
        jobs.push(("deduplication".into(), Value::Bool(pf.dedup)));
        let clusters: Vec<String> = pf
            .replication
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !clusters.is_empty() {
            jobs.push((
                "replication".into(),
                serde_json::json!({"clusters": clusters, "allowedClusters": clusters}),
            ));
        }

        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        let lang = self.lang;
        self.busy += 1;
        self.notify(format!(
            "{} {} ...",
            i18n::t(lang, "正在保存策略", "正在儲存策略", "Saving policies"),
            jobs.len()
        ));
        self.spawn(async move {
            let mut errors = Vec::new();
            for (policy, body) in jobs {
                if let Err(e) = admin.set_namespace_policy(&ns, &policy, body).await {
                    errors.push(format!("{policy}: {e}"));
                }
            }
            let ev = if errors.is_empty() {
                Event::Status(i18n::t(lang, "策略已保存", "策略已儲存", "Policies saved").into())
            } else {
                Event::Error(format!("部分策略保存失败: {}", errors.join(" | ")))
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("policy".into()));
            ctx.request_repaint();
        });
    }

    fn grant_permission(&mut self) {
        let (Some(admin), Some(ns)) = (self.admin.clone(), self.selected_ns.clone()) else {
            return;
        };
        let role = self.perm_role.trim().to_string();
        if role.is_empty() {
            return;
        }
        let mut actions = Vec::new();
        if self.perm_produce {
            actions.push("produce".to_string());
        }
        if self.perm_consume {
            actions.push("consume".to_string());
        }
        if self.perm_functions {
            actions.push("functions".to_string());
        }
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.grant_permission(&ns, &role, actions).await {
                Ok(()) => Event::Status(format!("已授予 {role} 权限")),
                Err(e) => Event::Error(format!("授权失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("perm".into()));
            ctx.request_repaint();
        });
    }

    fn revoke_permission(&mut self, role: String) {
        let (Some(admin), Some(ns)) = (self.admin.clone(), self.selected_ns.clone()) else {
            return;
        };
        let tx = self.tx.clone();
        let ctx = self.ctx.clone();
        self.busy += 1;
        self.spawn(async move {
            let ev = match admin.revoke_permission(&ns, &role).await {
                Ok(()) => Event::Status(format!("已撤销 {role} 权限")),
                Err(e) => Event::Error(format!("撤销失败: {e}")),
            };
            let _ = tx.send(ev);
            let _ = tx.send(Event::Done("perm".into()));
            ctx.request_repaint();
        });
    }

    fn refresh_decoded(&mut self) {
        let Some(m) = self.selected_msg.and_then(|i| self.messages.get(i).cloned()) else {
            return;
        };
        let mut text = String::new();
        if !m.properties.is_empty() {
            text.push_str("-- properties --\n");
            for (k, v) in &m.properties {
                text.push_str(&format!("{k} = {v}\n"));
            }
            text.push('\n');
        }
        text.push_str(&decode::decode(
            &m.payload,
            self.format,
            Some(self.avro_schema.as_str()),
        ));
        self.decoded = text;
    }

    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                Event::Status(s) => self.notify(s),
                Event::Error(e) => {
                    self.busy = self.busy.saturating_sub(1);
                    self.notify(format!("错误: {e}"));
                }
                Event::Connected { tenants, clusters } => {
                    self.busy = self.busy.saturating_sub(1);
                    self.connected = true;
                    self.tenants = tenants;
                    self.clusters = clusters;
                    self.debug_log(&format!(
                        "connected tenants={:?} clusters={:?}",
                        self.tenants, self.clusters
                    ));
                    self.notify(format!(
                        "{}: {} tenants / {} clusters",
                        i18n::t(self.lang, "已连接", "已連線", "Connected"),
                        self.tenants.len(),
                        self.clusters.len()
                    ));
                    if let Some(first) = self.tenants.first().cloned() {
                        self.load_namespaces(first);
                    }
                    if std::env::var("PS_CLUSTERS").is_ok() {
                        self.cluster_window = true;
                        self.load_cluster_info();
                    }
                    if std::env::var("PS_SETTINGS").is_ok() {
                        self.settings_window = true;
                    }
                    if std::env::var("PS_DARK").is_ok() {
                        self.dark = true;
                    }
                    if let Ok(ns_path) = std::env::var("PS_OPEN_NS") {
                        let parts: Vec<&str> = ns_path.split('/').collect();
                        let ns = NamespaceRef::new(
                            parts.first().copied().unwrap_or(""),
                            parts.get(1).copied().unwrap_or(""),
                        );
                        self.auto_expand = Some(ns.full());
                        self.selected_ns = Some(ns.clone());
                        self.load_topics(ns);
                    }
                    if let Ok(full) = std::env::var("PS_OPEN_TOPIC") {
                        let topic = TopicEntry::from_full(&full);
                        let parts: Vec<&str> = topic.short.split('/').collect();
                        self.selected_ns = Some(NamespaceRef::new(
                            parts.first().copied().unwrap_or(""),
                            parts.get(1).copied().unwrap_or(""),
                        ));
                        self.selected_topic = Some(topic.clone());
                        self.load_topic_detail(topic);
                        self.peek_messages();
                        if let Ok(kw) = std::env::var("PS_AUTOSEARCH") {
                            self.search_text = kw;
                            self.start_search();
                        }
                        if let Ok(tab) = std::env::var("PS_TAB") {
                            self.tab = match tab.as_str() {
                                "subs" => Tab::Subscriptions,
                                "producers" => Tab::Producers,
                                "consumers" => Tab::Consumers,
                                "schema" => Tab::Schema,
                                "storage" => Tab::Storage,
                                "policies" => Tab::Policies,
                                "stats" => Tab::Stats,
                                _ => Tab::Messages,
                            };
                        }
                    }
                }
                Event::Namespaces { tenant, list } => {
                    self.ns_pending.retain(|t| t != &tenant);
                    let auto_pick = if self.selected_ns.is_none() {
                        list.first().cloned()
                    } else {
                        None
                    };
                    // 全库搜索期间，继续把该租户下各命名空间的 Topic 拉进来
                    if !self.tree_search_term.is_empty() {
                        for full in &list {
                            if self.topics_cache.contains_key(full) {
                                continue;
                            }
                            let parts: Vec<&str> = full.split('/').collect();
                            let ns = NamespaceRef::new(
                                parts.first().copied().unwrap_or(""),
                                parts.get(1).copied().unwrap_or(""),
                            );
                            self.load_topics(ns);
                        }
                    }
                    self.ns_cache.insert(tenant, list);
                    if let Some(full) = auto_pick {
                        let parts: Vec<&str> = full.split('/').collect();
                        let ns = NamespaceRef::new(
                            parts.first().copied().unwrap_or(""),
                            parts.get(1).copied().unwrap_or(""),
                        );
                        self.selected_ns = Some(ns.clone());
                        self.load_topics(ns);
                    }
                }
                Event::Topics { ns, list } => {
                    self.busy = self.busy.saturating_sub(1);
                    self.debug_log(&format!("topics {} => {}", ns.full(), list.len()));
                    self.topics_pending.retain(|k| k != &ns.full());
                    self.topics_cache.insert(ns.full(), list.clone());
                    if self.selected_ns.as_ref() == Some(&ns) {
                        self.notify(format!(
                            "{} {}: {} topics",
                            ns.full(),
                            i18n::t(self.lang, "下共", "下共", "has"),
                            list.len()
                        ));
                        self.topics = list;
                    }
                }
                Event::Detail {
                    topic,
                    summary,
                    raw,
                    subs,
                    policies,
                    perms,
                } => {
                    self.busy = self.busy.saturating_sub(1);
                    if self
                        .selected_topic
                        .as_ref()
                        .map(|t| t.short == topic)
                        .unwrap_or(false)
                    {
                        self.summary = Some(*summary);
                        self.stats_raw = raw;
                        let stats_value: Value =
                            serde_json::from_str(&self.stats_raw).unwrap_or(Value::Null);
                        self.partitions = crate::model::PartitionRow::collect(&stats_value);
                        self.producers = crate::model::ProducerRow::collect(&stats_value);
                        self.consumers = crate::model::ConsumerRow::collect(&stats_value);
                        self.subs = subs;
                        if let Some(p) = policies {
                            self.pf.apply(&p);
                            self.policies = Some(p);
                        }
                        if let Some(pm) = perms {
                            self.perms = pm
                                .as_object()
                                .map(|m| {
                                    m.iter()
                                        .map(|(role, v)| {
                                            let actions = v
                                                .as_array()
                                                .map(|a| {
                                                    a.iter()
                                                        .filter_map(|x| x.as_str().map(String::from))
                                                        .collect::<Vec<_>>()
                                                })
                                                .unwrap_or_default();
                                            (role.clone(), actions)
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                        }
                    }
                }
                Event::Messages(rows) => {
                    self.busy = self.busy.saturating_sub(1);
                    self.notify(format!(
                        "{} {}",
                        i18n::t(self.lang, "已读取", "已讀取", "Fetched"),
                        rows.len()
                    ));
                    self.messages = Arc::new(rows);
                    self.selected_msg = if self.messages.is_empty() { None } else { Some(0) };
                    self.refresh_decoded();
                }
                Event::MessagesAppended(rows) => {
                    self.busy = self.busy.saturating_sub(1);
                    let mut list = (*self.messages).clone();
                    let last_id = list.last().map(|r| r.message_id.clone());
                    let base = list.len();
                    let mut added = 0usize;
                    for mut r in rows {
                        if Some(&r.message_id) == last_id.as_ref() {
                            continue;
                        }
                        r.index = base + added + 1;
                        added += 1;
                        list.push(r);
                    }
                    let total = list.len();
                    self.messages = Arc::new(list);
                    self.notify(format!(
                        "{} {}",
                        i18n::t(self.lang, "累计", "累計", "Total"),
                        total
                    ));
                }
                Event::SearchProgress { scanned, hits } => {
                    self.search_scanned = scanned;
                    self.search_hits = hits;
                }
                Event::SearchDone {
                    rows,
                    scanned,
                    truncated,
                } => {
                    self.search_active = false;
                    self.search_cancel = None;
                    self.search_scanned = scanned;
                    self.search_hits = rows.len();
                    self.search_truncated = truncated;
                    let hits = rows.len();
                    self.messages = Arc::new(rows);
                    self.selected_msg = if hits == 0 { None } else { Some(0) };
                    self.refresh_decoded();
                    self.notify(format!(
                        "{}: {} / {}",
                        i18n::t(self.lang, "扫描完成", "掃描完成", "Scan finished"),
                        scanned,
                        hits
                    ));
                }
                Event::Published => {
                    self.busy = self.busy.saturating_sub(1);
                    self.publish_dialog = false;
                    self.notify(i18n::t(self.lang, "消息发送成功", "訊息發布成功", "Message sent"));
                }
                Event::ClusterInfo { rows, version } => {
                    self.busy = self.busy.saturating_sub(1);
                    self.cluster_rows = rows;
                    self.broker_version = version;
                }
                Event::Schema(v) => {
                    self.schema_info = Some(v);
                }
                Event::Storage(v) => {
                    let st = crate::model::StorageSummary::from_value(&v);
                    self.sub_cursors = st.cursors.clone();
                    self.storage = Some(st);
                    if !self.auto_preview_done {
                        if let Ok(sub) = std::env::var("PS_PREVIEW_SUB") {
                            if self.sub_cursors.contains_key(&sub) {
                                self.auto_preview_done = true;
                                self.preview_subscription(sub);
                            }
                        }
                    }
                }
                Event::SubPreview(rows) => {
                    self.busy = self.busy.saturating_sub(1);
                    self.debug_log(&format!("sub preview rows={}", rows.len()));
                    let first_payload = rows.first().map(|m| m.payload.clone());
                    let text = match first_payload {
                        Some(payload) => {
                            decode::decode(&payload, self.format, Some(&self.avro_schema))
                        }
                        None => String::new(),
                    };
                    if let Some(p) = self.sub_preview.as_mut() {
                        p.loading = false;
                        p.selected = 0;
                        p.decoded = text;
                        p.rows = rows;
                    }
                }
                Event::Bundles(n) => {
                    self.bundles_count = n;
                }
                Event::TenantInfo { tenant, info } => {
                    let join = |key: &str| {
                        info.get(key)
                            .and_then(|x| x.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|v| v.as_str())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            })
                            .unwrap_or_default()
                    };
                    self.new_tenant_cluster = join("allowedClusters");
                    self.new_tenant_roles = join("adminRoles");
                    self.tenant_dialog = Some(TenantDialog::Edit(tenant));
                }
                Event::GlobalTopics(rows) => {
                    self.busy = self.busy.saturating_sub(1);
                    self.notify(format!(
                        "{}: {}",
                        i18n::t(self.lang, "全局 Topic 数", "全域 Topic 數", "Total topics"),
                        rows.len()
                    ));
                    self.global_rows = rows;
                }
                Event::Tenants(list) => {
                    self.tenants = list;
                    self.ns_cache.clear();
                }
                Event::Clusters(list) => {
                    self.clusters = list;
                }
                Event::TenantCreated => {
                    self.busy = self.busy.saturating_sub(1);
                    if let Some(admin) = self.admin.clone() {
                        let tx = self.tx.clone();
                        let ctx = self.ctx.clone();
                        self.spawn(async move {
                            if let Ok(list) = admin.tenants().await {
                                let _ = tx.send(Event::Tenants(list));
                            }
                            ctx.request_repaint();
                        });
                    }
                }
                Event::Done(kind) => {
                    self.busy = self.busy.saturating_sub(1);
                    match kind.as_str() {
                        "create_topic" | "delete_topic" => {
                            if let Some(ns) = self.selected_ns.clone() {
                                self.load_topics(ns);
                            }
                        }
                        "policy" | "perm" | "sub_action" => {
                            if let Some(t) = self.selected_topic.clone() {
                                self.load_topic_detail(t);
                            }
                        }
                        "cluster" => {
                            if self.cluster_window {
                                self.load_cluster_info();
                            }
                            if let Some(admin) = self.admin.clone() {
                                let tx = self.tx.clone();
                                let ctx = self.ctx.clone();
                                self.spawn(async move {
                                    if let Ok(list) = admin.clusters().await {
                                        let _ = tx.send(Event::Clusters(list));
                                    }
                                    ctx.request_repaint();
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn top_bar(&mut self, ctx: &egui::Context) {
        let mut want_connect = false;
        let mut want_disconnect = false;
        let mut want_refresh = false;
        let mut want_new = false;
        let mut want_edit = false;
        let mut want_delete_server = false;
        let mut want_clusters = false;
        let mut want_global = false;
        let mut want_settings = false;
        let mut pick_server = None;
        let lang = self.lang;

        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.heading("Pulsar Studio");
                ui.separator();
                ui.label(i18n::t(lang, "服务器", "伺服器", "Server"));
                let current = self
                    .cfg
                    .servers
                    .get(self.server_idx)
                    .map(|s| s.name.clone())
                    .unwrap_or_else(|| "<未配置>".into());
                egui::ComboBox::from_id_salt("server_pick")
                    .width(190.0)
                    .selected_text(current)
                    .show_ui(ui, |ui| {
                        for (i, s) in self.cfg.servers.iter().enumerate() {
                            if ui
                                .selectable_label(
                                    i == self.server_idx,
                                    format!("{} ({})", s.name, s.admin_url),
                                )
                                .clicked()
                            {
                                pick_server = Some(i);
                            }
                        }
                    });
                if ui
                    .button(i18n::t(lang, "新建连接", "新增連線", "New"))
                    .clicked()
                {
                    want_new = true;
                }
                if ui
                    .small_button(i18n::t(lang, "编辑", "編輯", "Edit"))
                    .clicked()
                {
                    want_edit = true;
                }
                if ui
                    .small_button(i18n::t(lang, "删除", "刪除", "Delete"))
                    .clicked()
                {
                    want_delete_server = true;
                }
                if !self.connected {
                    if ui.button(i18n::t(lang, "连接", "連線", "Connect")).clicked() {
                        want_connect = true;
                    }
                } else if ui
                    .button(i18n::t(lang, "断开", "中斷", "Disconnect"))
                    .clicked()
                {
                    want_disconnect = true;
                }
                if self.connected
                    && ui.button(i18n::t(lang, "刷新", "重新整理", "Refresh")).clicked()
                {
                    want_refresh = true;
                }
                if self.connected
                    && ui
                        .button(i18n::t(lang, "集群信息", "叢集資訊", "Clusters"))
                        .clicked()
                {
                    want_clusters = true;
                }
                if self.connected
                    && ui
                        .button(i18n::t(lang, "全局 Topic", "全域 Topic", "All topics"))
                        .clicked()
                {
                    want_global = true;
                }
                if self.busy > 0 {
                    ui.spinner();
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui
                        .button(if self.dark { "☀ 浅色" } else { "🌙 深色" })
                        .clicked()
                    {
                        self.dark = !self.dark;
                        self.cfg.dark_theme = self.dark;
                        let _ = config::save(&self.cfg);
                    }
                    if ui
                        .button("⚙")
                        .on_hover_text(i18n::t(lang, "设置", "設定", "Settings"))
                        .clicked()
                    {
                        want_settings = true;
                    }
                    egui::ComboBox::from_id_salt("lang")
                        .width(96.0)
                        .selected_text(self.lang.label())
                        .show_ui(ui, |ui| {
                            for l in [Lang::Zh, Lang::ZhHant, Lang::En] {
                                if ui.selectable_label(self.lang == l, l.label()).clicked() {
                                    self.lang = l;
                                }
                            }
                        });
                    ui.label(egui::RichText::new(format!("集群 {}", self.clusters.len())).weak());
                });
            });
            ui.add_space(4.0);
        });

        if lang != self.lang {
            self.cfg.language = self.lang.code().to_string();
            let _ = config::save(&self.cfg);
        }
        if let Some(i) = pick_server {
            self.server_idx = i;
            self.connected = false;
            self.admin = None;
            self.tenants.clear();
            self.ns_cache.clear();
            self.topics.clear();
            self.selected_topic = None;
            self.selected_ns = None;
        }
        if want_new {
            self.draft = Server::default();
            self.edit_server = None;
            self.server_dialog = true;
        }
        if want_edit {
            self.draft = self.current_server();
            self.edit_server = Some(self.server_idx);
            self.server_dialog = true;
        }
        if want_delete_server {
            self.delete_server_confirm = true;
        }
        if want_connect {
            self.connect();
        }
        if want_disconnect {
            self.connected = false;
            self.admin = None;
            self.tenants.clear();
            self.ns_cache.clear();
            self.topics.clear();
            self.selected_topic = None;
            self.selected_ns = None;
            self.notify(i18n::t(self.lang, "已断开", "已中斷", "Disconnected"));
        }
        if want_refresh {
            self.ns_cache.clear();
            self.connect();
        }
        if want_clusters {
            self.cluster_window = true;
            self.load_cluster_info();
        }
        if want_global {
            self.load_global_topics();
        }
        if want_settings {
            self.settings_window = true;
        }
    }

    fn tree_panel(&mut self, ctx: &egui::Context) {
        let lang = self.lang;
        let mut pick_ns: Option<NamespaceRef> = None;
        let mut to_load: Vec<String> = Vec::new();
        let mut want_new_ns = false;
        let mut want_new_tenant = false;
        let mut delete_tenant: Option<String> = None;
        let mut edit_tenant: Option<String> = None;
        let mut delete_ns: Option<NamespaceRef> = None;
        let mut pick_topic: Option<TopicEntry> = None;
        let mut to_load_ns: Vec<NamespaceRef> = Vec::new();

        egui::SidePanel::left("tree")
            .resizable(true)
            .default_width(270.0)
            .width_range(200.0..=520.0)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.strong(i18n::t(lang, "资源树", "資源樹", "Resources"));
                    if ui
                        .small_button(i18n::t(lang, "+ 租户", "+ 租戶", "+ Tenant"))
                        .clicked()
                    {
                        want_new_tenant = true;
                    }
                    if ui
                        .small_button(i18n::t(lang, "+ 命名空间", "+ 命名空間", "+ Namespace"))
                        .clicked()
                    {
                        want_new_ns = true;
                    }
                });
                let filter_resp = ui.add(
                    egui::TextEdit::singleline(&mut self.tree_filter)
                        .hint_text(i18n::t(
                            lang,
                            "过滤资源（回车搜全库）",
                            "篩選資源（Enter 搜全庫）",
                            "Filter (Enter = search all)",
                        ))
                        .desired_width(f32::INFINITY),
                );
                if filter_resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                    self.tree_search_all();
                }
                if filter_resp.changed() && self.tree_filter.trim().is_empty() {
                    self.tree_search_term.clear();
                }
                ui.separator();

                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if !self.connected {
                            ui.label(
                                egui::RichText::new(i18n::t(
                                    lang, "未连接", "未連線", "Not connected",
                                ))
                                .weak(),
                            );
                            return;
                        }
                        let needle = self.tree_filter.trim().to_lowercase();
                        let mut shown = 0usize;
                        for tenant in self.tenants.clone() {
                            let tenant_match =
                                !needle.is_empty() && tenant.to_lowercase().contains(&needle);
                            let ns_loaded = self.ns_cache.contains_key(&tenant);
                            // 该租户下要显示的命名空间：名字命中，或其中已加载的 Topic 命中
                            let mut visible_ns: Vec<String> = Vec::new();
                            if let Some(list) = self.ns_cache.get(&tenant) {
                                for full in list {
                                    if needle.is_empty()
                                        || tenant_match
                                        || full.to_lowercase().contains(needle.as_str())
                                    {
                                        visible_ns.push(full.clone());
                                        continue;
                                    }
                                    if let Some(topics) = self.topics_cache.get(full) {
                                        if topics
                                            .iter()
                                            .any(|t| t.local_name.to_lowercase().contains(needle.as_str()))
                                        {
                                            visible_ns.push(full.clone());
                                        }
                                    }
                                }
                            }
                            // 已加载却没有任何命中：过滤时整条分支隐藏
                            if !needle.is_empty() && ns_loaded && visible_ns.is_empty() {
                                continue;
                            }
                            let force_open =
                                !needle.is_empty() && (tenant_match || !visible_ns.is_empty());
                            let header = egui::CollapsingHeader::new(format!("🏢 {tenant}"))
                                .id_salt(format!("tenant-{tenant}"));
                            let header = if force_open {
                                header.open(Some(true))
                            } else {
                                header
                            };
                            let resp = header.show(ui, |ui| {
                                    if !ns_loaded {
                                        ui.horizontal(|ui| {
                                            ui.spinner();
                                            ui.label(i18n::t(
                                                lang, "加载中 ...", "載入中 ...", "Loading ...",
                                            ));
                                        });
                                        to_load.push(tenant.clone());
                                    } else {
                                        if visible_ns.is_empty() && needle.is_empty() {
                                            ui.label(
                                                egui::RichText::new(i18n::t(
                                                    lang,
                                                    "无命名空间",
                                                    "無命名空間",
                                                    "No namespaces",
                                                ))
                                                .weak(),
                                            );
                                        }
                                        for full in visible_ns.clone() {
                                            let parts: Vec<&str> = full.split('/').collect();
                                            let ns = NamespaceRef::new(
                                                parts.first().copied().unwrap_or(""),
                                                parts.get(1).copied().unwrap_or(""),
                                            );
                                            let ns_key = ns.full();
                                            let ns_sel = self.selected_ns.as_ref() == Some(&ns);
                                            let auto =
                                                self.auto_expand.as_deref() == Some(ns_key.as_str());
                                            // 过滤时：命名空间名命中则显示其全部 Topic，否则只显示命中的 Topic
                                            let ns_match = needle.is_empty()
                                                || tenant_match
                                                || full.to_lowercase().contains(needle.as_str());
                                            let header = egui::CollapsingHeader::new(format!(
                                                "📁 {}",
                                                ns.namespace
                                            ))
                                            .id_salt(format!("ns-{ns_key}"))
                                            .default_open(auto);
                                            let header = if needle.is_empty() {
                                                header
                                            } else {
                                                header.open(Some(true))
                                            };
                                            let resp = header.show(ui, |ui| {
                                                if !self.topics_cache.contains_key(&ns_key) {
                                                    ui.horizontal(|ui| {
                                                        ui.spinner();
                                                        ui.label(
                                                            i18n::t(
                                                                lang,
                                                                "加载中 ...",
                                                                "載入中 ...",
                                                                "Loading ...",
                                                            ),
                                                        );
                                                    });
                                                    to_load_ns.push(ns.clone());
                                                } else if let Some(list) =
                                                    self.topics_cache.get(&ns_key).cloned()
                                                {
                                                    if list.is_empty() {
                                                        ui.label(
                                                            egui::RichText::new(i18n::t(
                                                                lang,
                                                                "无 Topic",
                                                                "無 Topic",
                                                                "No topics",
                                                            ))
                                                            .weak(),
                                                        );
                                                    }
                                                    for t in list {
                                                        if !ns_match
                                                            && !t
                                                                .local_name
                                                                .to_lowercase()
                                                                .contains(needle.as_str())
                                                        {
                                                            continue;
                                                        }
                                                        let sel = self
                                                            .selected_topic
                                                            .as_ref()
                                                            .map(|x| x.short == t.short)
                                                            .unwrap_or(false);
                                                        let max_chars =
                                                            ((ui.available_width() - 26.0) / 8.0)
                                                                .max(10.0)
                                                                as usize;
                                                        let label = if t
                                                            .local_name
                                                            .chars()
                                                            .count()
                                                            > max_chars
                                                        {
                                                            format!(
                                                                "📄 {}…",
                                                                t.local_name
                                                                    .chars()
                                                                    .take(
                                                                        max_chars
                                                                            .saturating_sub(1),
                                                                    )
                                                                    .collect::<String>()
                                                            )
                                                        } else {
                                                            format!("📄 {}", t.local_name)
                                                        };
                                                        if ui
                                                            .selectable_label(sel, label)
                                                            .on_hover_text(&t.local_name)
                                                            .clicked()
                                                        {
                                                            pick_topic = Some(t.clone());
                                                        }
                                                    }
                                                }
                                            });
                                            let header_clicked = resp.header_response.clicked();
                                            resp.header_response.clone().context_menu(|ui| {
                                                if ui
                                                    .button(i18n::t(
                                                        lang,
                                                        "删除命名空间",
                                                        "刪除命名空間",
                                                        "Delete namespace",
                                                    ))
                                                    .clicked()
                                                {
                                                    delete_ns = Some(ns.clone());
                                                    ui.close_menu();
                                                }
                                            });
                                            if header_clicked {
                                                pick_ns = Some(ns.clone());
                                            }
                                            if resp.openness > 0.0
                                                && !self.topics_cache.contains_key(&ns_key)
                                                && !self.topics_pending.contains(&ns_key)
                                            {
                                                to_load_ns.push(ns.clone());
                                            }
                                            let _ = ns_sel;
                                        }
                                    }
                                });
                            let header_clicked = resp.header_response.clicked();
                            resp.header_response.clone().context_menu(|ui| {
                                if ui
                                    .button(i18n::t(
                                        lang,
                                        "编辑租户",
                                        "編輯租戶",
                                        "Edit tenant",
                                    ))
                                    .clicked()
                                {
                                    edit_tenant = Some(tenant.clone());
                                    ui.close_menu();
                                }
                                if ui
                                    .button(i18n::t(
                                        lang,
                                        "删除租户",
                                        "刪除租戶",
                                        "Delete tenant",
                                    ))
                                    .clicked()
                                {
                                    delete_tenant = Some(tenant.clone());
                                    ui.close_menu();
                                }
                            });
                            if header_clicked && !self.ns_cache.contains_key(&tenant) {
                                to_load.push(tenant.clone());
                            }
                            shown += 1;
                        }
                        if self.tenants.is_empty() {
                            ui.label(egui::RichText::new("没有租户，检查连接或权限").weak());
                        } else if shown == 0 {
                            ui.label(
                                egui::RichText::new(i18n::t(
                                    lang,
                                    "无匹配项",
                                    "無相符項目",
                                    "No matches",
                                ))
                                .weak(),
                            );
                        }
                    });
            });

        for t in to_load {
            self.load_namespaces(t);
        }
        for ns in to_load_ns {
            if !self.topics_cache.contains_key(&ns.full()) {
                self.load_topics(ns);
            }
        }
        if let Some(ns) = pick_ns {
            if self.selected_ns.as_ref() != Some(&ns) {
                self.selected_ns = Some(ns.clone());
                self.topics.clear();
                self.selected_topic = None;
                self.load_topics(ns);
            } else if self.selected_topic.is_some() {
                // 再次点击同一命名空间：回到 Topic 列表
                self.selected_topic = None;
            }
        }
        if let Some(t) = pick_topic {
            self.selected_topic = Some(t.clone());
            self.tab = Tab::Messages;
            self.load_topic_detail(t);
        }
        if want_new_ns {
            self.new_ns_name.clear();
            self.create_ns_dialog = true;
        }
        if want_new_tenant {
            self.new_tenant.clear();
            self.new_tenant_cluster = self.clusters.first().cloned().unwrap_or_default();
            self.tenant_dialog = Some(TenantDialog::Create);
        }
        if let Some(t) = delete_tenant {
            self.tenant_dialog = Some(TenantDialog::Delete(t));
        }
        if let Some(t) = edit_tenant {
            self.load_tenant_info(t);
        }
        if let Some(ns) = delete_ns {
            self.ns_dialog = Some(ns);
        }
    }

    fn topic_list(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let ns = self.selected_ns.clone();
        let mut open_topic: Option<TopicEntry> = None;
        let mut delete_topic: Option<TopicEntry> = None;
        let mut want_create = false;

        ui.horizontal(|ui| {
            match &ns {
                Some(ns) => {
                    ui.heading(ns.full());
                }
                None => {
                    ui.heading(i18n::t(
                        lang,
                        "选择命名空间",
                        "選擇命名空間",
                        "Select a namespace",
                    ));
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ns.is_some()
                    && ui
                        .button(i18n::t(lang, "新建 Topic", "新增 Topic", "New topic"))
                        .clicked()
                {
                    want_create = true;
                }
                ui.add(
                    egui::TextEdit::singleline(&mut self.topic_filter)
                        .hint_text(i18n::t(lang, "过滤", "篩選", "Filter"))
                        .desired_width(160.0),
                );
            });
        });
        ui.separator();

        if ns.is_none() {
            ui.label("从左侧资源树选择一个命名空间。");
            return;
        }

        let filter = self.topic_filter.to_lowercase();
        let topics: Vec<TopicEntry> = self
            .topics
            .iter()
            .filter(|t| filter.is_empty() || t.local_name.to_lowercase().contains(&filter))
            .cloned()
            .collect();

        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::remainder().at_least(200.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::initial(80.0))
                    .header(28.0, |mut header| {
                        header.col(|ui| {
                            ui.strong("Topic");
                        });
                        header.col(|ui| {
                            ui.strong(i18n::t(lang, "分区", "分區", "Partitioned"));
                        });
                        header.col(|ui| {
                            ui.strong(i18n::t(lang, "操作", "操作", "Actions"));
                        });
                    })
                    .body(|body| {
                        body.rows(26.0, topics.len(), |mut row| {
                            let idx = row.index();
                            let t = &topics[idx];
                            row.col(|ui| {
                                if ui.selectable_label(false, &t.local_name).clicked() {
                                    open_topic = Some(t.clone());
                                }
                            });
                            row.col(|ui| {
                                ui.label(if t.partitioned { "是" } else { "-" });
                            });
                            row.col(|ui| {
                                if ui
                                    .small_button(i18n::t(lang, "删除", "刪除", "Delete"))
                                    .clicked()
                                {
                                    delete_topic = Some(t.clone());
                                }
                            });
                        });
                    });
            });

        if want_create {
            self.new_topic_name.clear();
            self.create_topic_dialog = true;
        }
        if let Some(t) = open_topic {
            self.selected_topic = Some(t.clone());
            self.tab = Tab::Messages;
            self.load_topic_detail(t);
        }
        if let Some(t) = delete_topic {
            self.delete_topic(t);
        }
    }

    fn detail(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let Some(topic) = self.selected_topic.clone() else {
            return;
        };

        let mut want_refresh = false;
        let mut want_back = false;
        ui.horizontal_wrapped(|ui| {
            if ui
                .button(i18n::t(lang, "← 返回列表", "← 返回列表", "← Back"))
                .clicked()
            {
                want_back = true;
            }
            ui.heading(&topic.local_name);
            ui.label(egui::RichText::new(&topic.short).weak());
            if ui.button(i18n::t(lang, "刷新", "重新整理", "Refresh")).clicked() {
                want_refresh = true;
            }
        });
        if want_back {
            self.selected_topic = None;
            return;
        }
        if let Some(s) = &self.summary {
            ui.horizontal_wrapped(|ui| {
                ui.label(format!(
                    "{} {}",
                    i18n::t(lang, "生产者", "生產者", "Producers"),
                    s.producers
                ));
                ui.separator();
                ui.label(format!(
                    "{} {}",
                    i18n::t(lang, "订阅", "訂閱", "Subscriptions"),
                    s.subscriptions
                ));
                ui.separator();
                ui.label(format!(
                    "{} {:.1} msg/s",
                    i18n::t(lang, "写入", "寫入", "In"),
                    s.msg_rate_in
                ));
                ui.separator();
                ui.label(format!(
                    "{} {:.1} msg/s",
                    i18n::t(lang, "消费", "消費", "Out"),
                    s.msg_rate_out
                ));
                ui.separator();
                ui.label(format!(
                    "{} {}",
                    i18n::t(lang, "积压", "積壓", "Backlog"),
                    s.msg_backlog
                ));
                ui.separator();
                ui.label(format!(
                    "{} {}",
                    i18n::t(lang, "存储", "儲存", "Storage"),
                    human_size(s.storage_size)
                ));
            });
        }
        if !self.partitions.is_empty() {
            egui::CollapsingHeader::new(format!(
                "{} ({})",
                i18n::t(lang, "分区明细", "分區明細", "Partitions"),
                self.partitions.len()
            ))
            .id_salt("partitions")
            .show(ui, |ui| {
                egui::Grid::new("part_grid")
                    .num_columns(5)
                    .striped(true)
                    .spacing([16.0, 4.0])
                    .show(ui, |ui| {
                        ui.strong(i18n::t(lang, "分区", "分區", "Partition"));
                        ui.strong(i18n::t(lang, "写入 msg/s", "寫入 msg/s", "In msg/s"));
                        ui.strong(i18n::t(lang, "消费 msg/s", "消費 msg/s", "Out msg/s"));
                        ui.strong(i18n::t(lang, "积压", "積壓", "Backlog"));
                        ui.strong(i18n::t(lang, "存储", "儲存", "Storage"));
                        ui.end_row();
                        for p in &self.partitions {
                            ui.label(&p.name);
                            ui.label(format!("{:.1}", p.msg_rate_in));
                            ui.label(format!("{:.1}", p.msg_rate_out));
                            ui.label(p.backlog.to_string());
                            ui.label(human_size(p.storage));
                            ui.end_row();
                        }
                    });
            });
        }
        ui.separator();

        ui.horizontal_wrapped(|ui| {
            for (tab, name) in [
                (
                    Tab::Messages,
                    i18n::t(lang, "消息", "訊息", "Messages"),
                ),
                (
                    Tab::Subscriptions,
                    i18n::t(lang, "订阅", "訂閱", "Subscriptions"),
                ),
                (
                    Tab::Producers,
                    i18n::t(lang, "生产者", "生產者", "Producers"),
                ),
                (
                    Tab::Consumers,
                    i18n::t(lang, "消费者", "消費者", "Consumers"),
                ),
                (Tab::Schema, "Schema"),
                (
                    Tab::Storage,
                    i18n::t(lang, "存储", "儲存", "Storage"),
                ),
                (
                    Tab::Policies,
                    i18n::t(lang, "策略/权限", "策略/權限", "Policies"),
                ),
                (Tab::Stats, i18n::t(lang, "统计", "統計", "Stats")),
            ] {
                if ui.selectable_label(self.tab == tab, name).clicked() {
                    self.tab = tab;
                }
            }
        });
        ui.separator();

        match self.tab {
            Tab::Messages => self.messages_ui(ui),
            Tab::Subscriptions => self.subscriptions_ui(ui),
            Tab::Producers => self.producers_ui(ui),
            Tab::Consumers => self.consumers_ui(ui),
            Tab::Schema => self.schema_ui(ui),
            Tab::Storage => self.storage_ui(ui),
            Tab::Policies => self.policies_ui(ui),
            Tab::Stats => {
                let mut text = self.stats_raw.clone();
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut text)
                                .code_editor()
                                .desired_width(f32::INFINITY)
                                .desired_rows(30)
                                .interactive(false),
                        );
                    });
            }
        }

        if want_refresh {
            self.load_topic_detail(topic);
        }
    }

    fn messages_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let mut want_peek = false;
        let mut want_publish = false;
        let mut format_changed = false;
        let mut pick_msg: Option<usize> = None;
        let mut schema_changed = false;
        let mut want_copy = false;
        let mut want_export = false;
        let mut want_export_csv = false;
        let mut want_next = false;
        let mut want_search = false;
        let mut want_cancel_search = false;

        ui.horizontal_wrapped(|ui| {
            ui.label(i18n::t(lang, "条数", "筆數", "Count"));
            ui.add(egui::DragValue::new(&mut self.peek_count).range(1..=1000));
            let earliest = i18n::t(lang, "从最早", "從最早", "Earliest");
            let latest = i18n::t(lang, "从最新", "從最新", "Latest");
            let by_time = i18n::t(lang, "从时间", "從時間", "From time");
            let by_id = i18n::t(lang, "从消息 ID", "從訊息 ID", "From ID");
            egui::ComboBox::from_id_salt("pos")
                .selected_text(match self.start_mode {
                    StartMode::Earliest => earliest,
                    StartMode::Latest => latest,
                    StartMode::Time => by_time,
                    StartMode::MessageId => by_id,
                })
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.start_mode, StartMode::Earliest, earliest);
                    ui.selectable_value(&mut self.start_mode, StartMode::Latest, latest);
                    ui.selectable_value(&mut self.start_mode, StartMode::Time, by_time);
                    ui.selectable_value(&mut self.start_mode, StartMode::MessageId, by_id);
                });
            if ui
                .button(i18n::t(lang, "拉取消息", "拉取訊息", "Fetch"))
                .clicked()
            {
                want_peek = true;
            }
            if ui
                .button(i18n::t(lang, "发布消息", "發布訊息", "Publish"))
                .clicked()
            {
                want_publish = true;
            }
            if ui
                .button(i18n::t(lang, "下一页", "下一頁", "Next page"))
                .clicked()
            {
                want_next = true;
            }
            ui.separator();
            ui.label(i18n::t(lang, "解码", "解碼", "Decode"));
            let before = self.format;
            egui::ComboBox::from_id_salt("fmt")
                .selected_text(self.format.label())
                .show_ui(ui, |ui| {
                    for f in ALL_FORMATS {
                        ui.selectable_value(&mut self.format, *f, f.label());
                    }
                });
            format_changed = before != self.format;
        });

        if self.start_mode == StartMode::Time {
            ui.horizontal(|ui| {
                ui.label(i18n::t(lang, "时间戳(ms)", "時間戳(ms)", "Timestamp (ms)"));
                ui.add(egui::TextEdit::singleline(&mut self.start_time).desired_width(170.0));
                if ui
                    .small_button(i18n::t(lang, "现在", "現在", "Now"))
                    .clicked()
                {
                    self.start_time = now_millis().to_string();
                }
            });
        } else if self.start_mode == StartMode::MessageId {
            ui.horizontal(|ui| {
                ui.label("Ledger");
                ui.add(egui::TextEdit::singleline(&mut self.start_ledger).desired_width(90.0));
                ui.label("Entry");
                ui.add(egui::TextEdit::singleline(&mut self.start_entry).desired_width(90.0));
            });
        }

        ui.horizontal_wrapped(|ui| {
            ui.label(i18n::t(
                lang,
                "页内过滤",
                "頁內篩選",
                "Page filter",
            ));
            ui.add(
                egui::TextEdit::singleline(&mut self.msg_filter)
                    .hint_text(i18n::t(
                        lang,
                        "仅当前页 (key/内容)",
                        "僅目前頁 (key/內容)",
                        "current page only",
                    ))
                    .desired_width(180.0),
            );
            ui.separator();
            ui.label(i18n::t(lang, "时间(ms)", "時間(ms)", "Time (ms)"));
            ui.add(
                egui::TextEdit::singleline(&mut self.filter_from)
                    .hint_text("from")
                    .desired_width(120.0),
            );
            ui.label("~");
            ui.add(
                egui::TextEdit::singleline(&mut self.filter_to)
                    .hint_text("to")
                    .desired_width(120.0),
            );
            if ui
                .button(i18n::t(lang, "复制内容", "複製內容", "Copy"))
                .clicked()
            {
                want_copy = true;
            }
            if ui
                .button(i18n::t(lang, "导出 JSON", "匯出 JSON", "Export JSON"))
                .clicked()
            {
                want_export = true;
            }
            if ui
                .button(i18n::t(lang, "导出 CSV", "匯出 CSV", "Export CSV"))
                .clicked()
            {
                want_export_csv = true;
            }
        });

        ui.horizontal_wrapped(|ui| {
            ui.label(i18n::t(lang, "全库搜索", "全庫搜尋", "Search all"));
            ui.add(
                egui::TextEdit::singleline(&mut self.search_text)
                    .hint_text(i18n::t(
                        lang,
                        "整个 Topic 历史（payload / key / 属性）",
                        "整個 Topic 歷史（payload / key / 屬性）",
                        "whole topic history",
                    ))
                    .desired_width(240.0),
            );
            if self.search_active {
                if ui
                    .button(i18n::t(lang, "停止", "停止", "Stop"))
                    .clicked()
                {
                    want_cancel_search = true;
                }
                ui.spinner();
                ui.label(format!(
                    "{} {} / {} {}",
                    i18n::t(lang, "已扫描", "已掃描", "scanned"),
                    self.search_scanned,
                    i18n::t(lang, "命中", "命中", "hits"),
                    self.search_hits
                ));
            } else {
                if ui
                    .button(i18n::t(lang, "开始搜索", "開始搜尋", "Search"))
                    .clicked()
                {
                    want_search = true;
                }
                if self.search_scanned > 0 {
                    ui.label(
                        egui::RichText::new(format!(
                            "{} {} / {} {}{}",
                            i18n::t(lang, "已扫描", "已掃描", "scanned"),
                            self.search_scanned,
                            i18n::t(lang, "命中", "命中", "hits"),
                            self.search_hits,
                            if self.search_truncated {
                                i18n::t(lang, "（已截断）", "（已截斷）", " (truncated)")
                            } else {
                                ""
                            }
                        ))
                        .weak(),
                    );
                }
            }
        });

        if matches!(self.format, Format::AvroToJson | Format::AvroEmbeddedToJson) {
            ui.horizontal(|ui| {
                ui.label("Avro Schema");
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.avro_schema)
                        .hint_text(r#"{"type":"record","name":"X","fields":[...]}"#)
                        .desired_width(f32::INFINITY),
                );
                schema_changed = resp.changed();
            });
        }
        ui.separator();

        let messages = self.messages.clone();
        let needle = self.msg_filter.to_lowercase();
        let from = self.filter_from.trim().parse::<i64>().ok().map(normalize_ts);
        let to = self.filter_to.trim().parse::<i64>().ok().map(normalize_ts);
        let mut idxs: Vec<usize> = messages
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                let ts = normalize_ts(m.publish_time);
                let in_range = from.map(|f| ts >= f).unwrap_or(true)
                    && to.map(|t| ts <= t).unwrap_or(true);
                in_range
                    && (needle.is_empty()
                        || m.key.to_lowercase().contains(&needle)
                        || m.message_id.contains(&needle)
                        || String::from_utf8_lossy(&m.payload)
                            .to_lowercase()
                            .contains(&needle))
            })
            .map(|(i, _)| i)
            .collect();
        if let Some((col, desc)) = self.msg_sort {
            match col {
                1 => idxs.sort_by_key(|i| {
                    let mut it = messages[*i].message_id.split(':');
                    let ledger = it.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
                    let entry = it.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
                    (ledger, entry)
                }),
                2 => idxs.sort_by_key(|i| messages[*i].publish_time),
                3 => idxs.sort_by(|a, b| messages[*a].key.cmp(&messages[*b].key)),
                4 => idxs.sort_by_key(|i| messages[*i].payload.len()),
                5 => idxs.sort_by(|a, b| messages[*a].payload.cmp(&messages[*b].payload)),
                _ => {}
            }
            if desc {
                idxs.reverse();
            }
        }
        ui.horizontal(|ui| {
            ui.label(
                egui::RichText::new(format!("{}/{}", idxs.len(), messages.len()))
                    .weak()
                    .small(),
            );
            ui.label(
                egui::RichText::new(i18n::t(
                    lang,
                    "↑↓ 切换选中",
                    "↑↓ 切換選取",
                    "↑↓ to move",
                ))
                .weak()
                .small(),
            );
        });
        if !needle.is_empty() && idxs.is_empty() && !self.search_active {
            let needle_for_search = self.msg_filter.clone();
            ui.horizontal_wrapped(|ui| {
                ui.label(
                    egui::RichText::new(i18n::t(
                        lang,
                        "当前页没有匹配项，但历史消息里可能还有 →",
                        "目前頁面沒有符合項目，但歷史訊息裡可能還有 →",
                        "No match on this page — history may still contain it →",
                    ))
                    .weak(),
                );
                if ui
                    .button(i18n::t(
                        lang,
                        "在全库搜索这个关键字",
                        "在全庫搜尋這個關鍵字",
                        "Search all history",
                    ))
                    .clicked()
                {
                    self.search_text = needle_for_search;
                    want_search = true;
                }
            });
        }
        let height = (ui.available_height() * 0.42).max(130.0);
        // 方向键切换选中：在 ScrollArea 之前 consume_key，滚动区就收不到这两个按键
        let mut scroll_to: Option<usize> = None;
        if !idxs.is_empty() && ui.ctx().memory(|m| m.focused().is_none()) {
            let mut delta = 0i32;
            ui.input_mut(|i| {
                if i.consume_key(egui::Modifiers::NONE, egui::Key::ArrowDown) {
                    delta += 1;
                }
                if i.consume_key(egui::Modifiers::NONE, egui::Key::ArrowUp) {
                    delta -= 1;
                }
            });
            if delta != 0 {
                let cur = self
                    .selected_msg
                    .and_then(|sel| idxs.iter().position(|x| *x == sel))
                    .map(|p| p as i32)
                    .unwrap_or(if delta > 0 { -1 } else { idxs.len() as i32 });
                let next = (cur + delta).clamp(0, idxs.len() as i32 - 1) as usize;
                self.debug_log(&format!("kbd select row={} msg_row={}", next, idxs[next] + 1));
                pick_msg = Some(idxs[next]);
                scroll_to = Some(next);
            }
        }
        let mut click_sort: Option<u8> = None;
        let sort_state = self.msg_sort;
        egui::ScrollArea::vertical()
            .id_salt("msg_table_v2")
            .max_height(height)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                let mut table = egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .sense(egui::Sense::click())
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::initial(42.0))
                    .column(egui_extras::Column::initial(170.0))
                    .column(egui_extras::Column::initial(150.0))
                    .column(egui_extras::Column::initial(110.0))
                    .column(egui_extras::Column::initial(70.0))
                    .column(egui_extras::Column::remainder().at_least(180.0));
                if let Some(r) = scroll_to {
                    table = table.scroll_to_row(r, Some(egui::Align::Center));
                }
                table
                    .header(28.0, |mut header| {
                        let names = ["#", "消息 ID", "发布时间", "Key", "大小", "内容预览"];
                        for (ci, name) in names.iter().enumerate() {
                            header.col(|ui| {
                                let arrow = match sort_state {
                                    Some((c, desc)) if c as usize == ci => {
                                        if desc {
                                            " ▼"
                                        } else {
                                            " ▲"
                                        }
                                    }
                                    _ => "",
                                };
                                let text = format!("{name}{arrow}");
                                if ui
                                    .add(
                                        egui::Label::new(
                                            egui::RichText::new(text).strong(),
                                        )
                                        .sense(egui::Sense::click()),
                                    )
                                    .clicked()
                                {
                                    click_sort = Some(ci as u8);
                                }
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(26.0, idxs.len(), |mut row| {
                            let i = idxs[row.index()];
                            let m = &messages[i];
                            let selected = self.selected_msg == Some(i);
                            row.set_selected(selected);
                            let mut hit = false;
                            row.col(|ui| {
                                if cell_click(ui, egui::RichText::new(m.index.to_string())) {
                                    hit = true;
                                }
                            });
                            row.col(|ui| {
                                if cell_click(
                                    ui,
                                    egui::RichText::new(&m.message_id).monospace(),
                                ) {
                                    hit = true;
                                }
                            });
                            row.col(|ui| {
                                if cell_click(ui, egui::RichText::new(m.publish_time_text())) {
                                    hit = true;
                                }
                            });
                            row.col(|ui| {
                                if cell_click(ui, egui::RichText::new(&m.key)) {
                                    hit = true;
                                }
                            });
                            row.col(|ui| {
                                if cell_click(
                                    ui,
                                    egui::RichText::new(m.payload.len().to_string()),
                                ) {
                                    hit = true;
                                }
                            });
                            row.col(|ui| {
                                let preview = String::from_utf8_lossy(&m.payload)
                                    .replace('\n', " ")
                                    .chars()
                                    .take(80)
                                    .collect::<String>();
                                if cell_click(ui, egui::RichText::new(preview)) {
                                    hit = true;
                                }
                            });
                            if hit || row.response().clicked() {
                                pick_msg = Some(i);
                            }
                        });
                    });
            });

        if let Some(ci) = click_sort {
            self.msg_sort = match self.msg_sort {
                Some((c, desc)) if c == ci => Some((ci, !desc)),
                _ => Some((ci, false)),
            };
        }

        // 预计算元数据行，供右侧详情面板使用
        let mut meta_rows: Vec<(String, String)> = Vec::new();
        if let Some(i) = self.selected_msg {
            if let Some(m) = messages.get(i) {
                meta_rows.push(("ID".into(), m.message_id.clone()));
                meta_rows.push((
                    i18n::t(lang, "发布时间", "發布時間", "Publish time").to_string(),
                    m.publish_time_text(),
                ));
                meta_rows.push((
                    "Key".into(),
                    if m.key.is_empty() {
                        "-".into()
                    } else {
                        m.key.clone()
                    },
                ));
                meta_rows.push((
                    i18n::t(lang, "大小", "大小", "Size").to_string(),
                    format!("{} B", m.payload.len()),
                ));
                for (k, v) in &m.properties {
                    meta_rows.push((k.clone(), v.clone()));
                }
                if let Some(p) = m.message_id.rsplit(':').next() {
                    if p != "-1" {
                        meta_rows.push((
                            i18n::t(lang, "分区", "分區", "Partition").to_string(),
                            p.to_string(),
                        ));
                    }
                }
            }
        }
        let payload_text = self.decoded.clone();
        let avail_w = ui.available_width();
        let avail_h = (ui.available_height() - 6.0).max(160.0);
        let left_w = (avail_w * 0.66).max(320.0);
        let right_w = (avail_w - left_w - 14.0).max(200.0);

        ui.horizontal_top(|ui| {
            ui.allocate_ui_with_layout(
                egui::vec2(left_w, avail_h),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    ui.label(
                        egui::RichText::new(i18n::t(
                            lang,
                            "消息内容",
                            "訊息內容",
                            "Message payload",
                        ))
                        .strong(),
                    );
                    egui::ScrollArea::both()
                        .id_salt("payload")
                        .auto_shrink([false, false])
                        .max_height(avail_h - 24.0)
                        .show(ui, |ui| {
                            let mut text = payload_text.clone();
                            ui.add(
                                egui::TextEdit::multiline(&mut text)
                                    .code_editor()
                                    .desired_width(f32::INFINITY)
                                    .desired_rows(14)
                                    .interactive(false),
                            );
                        });
                },
            );
            ui.separator();
            ui.allocate_ui_with_layout(
                egui::vec2(right_w, avail_h),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    ui.label(
                        egui::RichText::new(i18n::t(
                            lang,
                            "消息详情",
                            "訊息詳情",
                            "Message details",
                        ))
                        .strong(),
                    );
                    egui::ScrollArea::vertical()
                        .id_salt("msg_meta")
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            if meta_rows.is_empty() {
                                ui.label(
                                    egui::RichText::new(i18n::t(
                                        lang,
                                        "未选择消息",
                                        "未選擇訊息",
                                        "No message selected",
                                    ))
                                    .weak(),
                                );
                            } else {
                                for (k, v) in &meta_rows {
                                    ui.label(egui::RichText::new(k).weak());
                                    ui.add(egui::Label::new(egui::RichText::new(v)).wrap());
                                    ui.add_space(4.0);
                                }
                            }
                        });
                },
            );
        });

        if format_changed || schema_changed {
            self.refresh_decoded();
        }
        if let Some(i) = pick_msg {
            self.selected_msg = Some(i);
            self.refresh_decoded();
        }
        if want_copy {
            ui.ctx().copy_text(self.decoded.clone());
            self.notify(i18n::t(self.lang, "已复制到剪贴板", "已複製到剪貼簿", "Copied"));
        }
        if want_export {
            self.export_messages();
        }
        if want_export_csv {
            self.export_messages_csv();
        }
        if want_peek {
            self.peek_messages();
        }
        if want_next {
            self.next_page();
        }
        if want_search {
            self.start_search();
        }
        if want_cancel_search {
            if let Some(c) = &self.search_cancel {
                c.store(true, Ordering::Relaxed);
            }
            self.notify(i18n::t(
                self.lang,
                "正在停止搜索 ...",
                "正在停止搜尋 ...",
                "Stopping search ...",
            ));
        }
        if want_publish {
            self.publish_dialog = true;
        }
    }

    fn export_messages_csv(&mut self) {
        let mut out = String::from("index,messageId,publishTime,key,size,properties,payloadUtf8\n");
        for m in self.messages.iter() {
            let props = m
                .properties
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(";");
            let payload = String::from_utf8_lossy(&m.payload)
                .replace('\r', " ")
                .replace('\n', " ");
            out.push_str(&format!(
                "{},{},{},{},{},{},{}\n",
                m.index,
                csv_escape(&m.message_id),
                csv_escape(&m.publish_time_text()),
                csv_escape(&m.key),
                m.payload.len(),
                csv_escape(&props),
                csv_escape(&payload)
            ));
        }
        let path = std::env::temp_dir()
            .join(format!("pulsar-studio-export-{}.csv", now_millis()));
        match std::fs::write(&path, out) {
            Ok(()) => self.notify(format!(
                "{} {} → {}",
                i18n::t(self.lang, "已导出", "已匯出", "Exported"),
                self.messages.len(),
                path.display()
            )),
            Err(e) => self.notify(format!("导出失败: {e}")),
        }
    }

    fn export_messages(&mut self) {
        use base64::Engine;
        let payload: Vec<serde_json::Value> = self
            .messages
            .iter()
            .map(|m| {
                let props: HashMap<String, String> = m.properties.iter().cloned().collect();
                serde_json::json!({
                    "index": m.index,
                    "messageId": m.message_id,
                    "publishTime": m.publish_time_text(),
                    "key": m.key,
                    "properties": props,
                    "size": m.payload.len(),
                    "payloadUtf8": String::from_utf8(m.payload.clone()).ok(),
                    "payloadBase64": base64::engine::general_purpose::STANDARD.encode(&m.payload),
                })
            })
            .collect();
        let path = std::env::temp_dir()
            .join(format!("pulsar-studio-export-{}.json", now_millis()));
        match std::fs::write(&path, serde_json::to_string_pretty(&payload).unwrap_or_default()) {
            Ok(()) => self.notify(format!("已导出 {} 条消息到 {}", payload.len(), path.display())),
            Err(e) => self.notify(format!("导出失败: {e}")),
        }
    }

    fn consumers_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        if self.consumers.is_empty() {
            ui.label(i18n::t(
                lang,
                "当前没有连接的消费者",
                "目前沒有連線的消費者",
                "No connected consumers",
            ));
            return;
        }
        let rows = self.consumers.clone();
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::initial(150.0))
                    .column(egui_extras::Column::initial(190.0))
                    .column(egui_extras::Column::initial(150.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::remainder().at_least(120.0))
                    .header(28.0, |mut header| {
                        for name in [
                            i18n::t(lang, "订阅", "訂閱", "Subscription"),
                            "Consumer",
                            i18n::t(lang, "地址", "位址", "Address"),
                            "msg/s",
                            i18n::t(lang, "未确认", "未確認", "Unacked"),
                            i18n::t(lang, "客户端", "用戶端", "Client"),
                        ] {
                            header.col(|ui| {
                                ui.strong(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(26.0, rows.len(), |mut row| {
                            let c = &rows[row.index()];
                            row.col(|ui| {
                                ui.label(&c.sub);
                            });
                            row.col(|ui| {
                                ui.label(egui::RichText::new(&c.name).monospace());
                            });
                            row.col(|ui| {
                                ui.label(&c.address);
                            });
                            row.col(|ui| {
                                ui.label(format!("{:.1}", c.rate_out));
                            });
                            row.col(|ui| {
                                ui.label(c.unacked.to_string());
                            });
                            row.col(|ui| {
                                ui.label(&c.version);
                            });
                        });
                    });
            });
    }

    fn storage_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let Some(st) = self.storage.clone() else {
            ui.label(i18n::t(
                lang,
                "暂无存储信息",
                "暫無儲存資訊",
                "No storage info",
            ));
            return;
        };
        egui::Grid::new("storage_summary")
            .num_columns(4)
            .spacing([18.0, 4.0])
            .show(ui, |ui| {
                ui.label(i18n::t(lang, "条目数", "條目數", "Entries"));
                ui.label(st.entries.to_string());
                ui.label(i18n::t(lang, "总大小", "總大小", "Total size"));
                ui.label(human_size(st.total_size));
                ui.end_row();
                ui.label(i18n::t(lang, "状态", "狀態", "State"));
                ui.label(&st.state);
                ui.label(i18n::t(lang, "最后确认", "最後確認", "Last confirmed"));
                ui.label(&st.last_confirmed);
                ui.end_row();
                ui.label(i18n::t(lang, "等待游标", "等待游標", "Waiting cursors"));
                ui.label(st.waiting_cursors.to_string());
                ui.label(i18n::t(lang, "Ledger 数", "Ledger 數", "Ledgers"));
                ui.label(st.ledgers.len().to_string());
                ui.end_row();
            });
        ui.separator();
        let ledgers = st.ledgers.clone();
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::initial(110.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::initial(100.0))
                    .column(egui_extras::Column::initial(90.0))
                    .column(egui_extras::Column::remainder().at_least(120.0))
                    .header(28.0, |mut header| {
                        for name in [
                            "Ledger",
                            i18n::t(lang, "条目", "條目", "Entries"),
                            i18n::t(lang, "大小", "大小", "Size"),
                            "Offloaded",
                            i18n::t(lang, "副本不足", "副本不足", "Under replicated"),
                        ] {
                            header.col(|ui| {
                                ui.strong(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(26.0, ledgers.len(), |mut row| {
                            let l = &ledgers[row.index()];
                            row.col(|ui| {
                                ui.label(l.id.to_string());
                            });
                            row.col(|ui| {
                                ui.label(l.entries.to_string());
                            });
                            row.col(|ui| {
                                ui.label(human_size(l.size));
                            });
                            row.col(|ui| {
                                ui.label(if l.offloaded { "yes" } else { "-" });
                            });
                            row.col(|ui| {
                                ui.label(if l.under_replicated { "yes" } else { "-" });
                            });
                        });
                    });
            });
    }

    fn schema_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let Some(info) = self.schema_info.clone() else {
            ui.label(i18n::t(
                lang,
                "该 Topic 未注册 Schema",
                "該 Topic 未註冊 Schema",
                "No schema registered for this topic",
            ));
            return;
        };
        let schema_type = info.get("type").and_then(|v| v.as_str()).unwrap_or("-");
        let version = info.get("version").and_then(|v| v.as_i64()).unwrap_or(0);
        ui.horizontal(|ui| {
            ui.label(format!("Type: {schema_type}"));
            ui.separator();
            ui.label(format!("Version: {version}"));
        });
        if let Some(props) = info.get("properties") {
            if props.as_object().map(|m| !m.is_empty()).unwrap_or(false) {
                ui.label(format!("Properties: {props}"));
            }
        }
        ui.separator();
        let mut text = info
            .get("schema")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if let Ok(v) = serde_json::from_str::<Value>(&text) {
            text = serde_json::to_string_pretty(&v).unwrap_or(text);
        }
        egui::ScrollArea::both()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.add(
                    egui::TextEdit::multiline(&mut text)
                        .code_editor()
                        .desired_width(f32::INFINITY)
                        .desired_rows(24)
                        .interactive(false),
                );
            });
    }

    fn producers_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        if self.producers.is_empty() {
            ui.label(i18n::t(
                lang,
                "当前没有连接的生产者",
                "目前沒有連線的生產者",
                "No connected producers",
            ));
            return;
        }
        let rows = self.producers.clone();
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::initial(220.0))
                    .column(egui_extras::Column::initial(85.0))
                    .column(egui_extras::Column::initial(95.0))
                    .column(egui_extras::Column::initial(130.0))
                    .column(egui_extras::Column::remainder().at_least(160.0))
                    .header(28.0, |mut header| {
                        for name in [
                            "Producer",
                            "msg/s",
                            "KB/s",
                            i18n::t(lang, "客户端", "用戶端", "Client"),
                            i18n::t(lang, "地址", "位址", "Address"),
                        ] {
                            header.col(|ui| {
                                ui.strong(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(26.0, rows.len(), |mut row| {
                            let p = &rows[row.index()];
                            row.col(|ui| {
                                ui.label(egui::RichText::new(&p.name).monospace());
                            });
                            row.col(|ui| {
                                ui.label(format!("{:.1}", p.rate_in));
                            });
                            row.col(|ui| {
                                ui.label(format!("{:.1}", p.throughput_in / 1024.0));
                            });
                            row.col(|ui| {
                                ui.label(&p.version);
                            });
                            row.col(|ui| {
                                ui.label(&p.address);
                            });
                        });
                    });
            });
    }

    fn subscriptions_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let mut want_create = false;
        let mut preview_sub: Option<String> = None;
        ui.horizontal(|ui| {
            if ui
                .button(i18n::t(lang, "新建订阅", "新增訂閱", "New subscription"))
                .clicked()
            {
                want_create = true;
            }
        });
        if want_create {
            self.new_sub_name.clear();
            self.new_sub_ledger.clear();
            self.new_sub_entry.clear();
            self.new_sub_pos = SubStart::Latest;
            self.sub_create_dialog = true;
        }
        ui.separator();
        if self.subs.is_empty() {
            ui.label(i18n::t(
                lang,
                "该 Topic 暂无订阅（消息面板拉取不会创建持久订阅）。",
                "該 Topic 暫無訂閱（訊息面板拉取不會建立持久訂閱）。",
                "No subscriptions on this topic (fetching messages does not create a durable one).",
            ));
            return;
        }
        let subs = self.subs.clone();
        let mut action: Option<(SubDialog, String, String)> = None;

        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .column(egui_extras::Column::initial(220.0))
                    .column(egui_extras::Column::initial(90.0))
                    .column(egui_extras::Column::initial(90.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::initial(80.0))
                    .column(egui_extras::Column::initial(80.0))
                    .header(28.0, |mut header| {
                        for name in ["订阅名", "类型", "出速 msg/s", "积压", "消费者", "操作"] {
                            header.col(|ui| {
                                ui.strong(name);
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(26.0, subs.len(), |mut row| {
                            let i = row.index();
                            let s = &subs[i];
                            row.col(|ui| {
                                ui.label(egui::RichText::new(&s.name).monospace());
                            });
                            row.col(|ui| {
                                ui.label(&s.sub_type);
                            });
                            row.col(|ui| {
                                ui.label(format!("{:.1}", s.msg_rate_out));
                            });
                            row.col(|ui| {
                                ui.label(s.backlog.to_string());
                            });
                            row.col(|ui| {
                                ui.label(s.consumers.to_string());
                            });
                            row.col(|ui| {
                                ui.menu_button("...", |ui| {
                                    let has_cursor = self.sub_cursors.contains_key(&s.name);
                                    if ui
                                        .add_enabled(
                                            has_cursor,
                                            egui::Button::new(i18n::t(
                                                lang,
                                                "预览积压（未消费）",
                                                "預覽積壓（未消費）",
                                                "Preview backlog",
                                            )),
                                        )
                                        .on_hover_text(i18n::t(
                                            lang,
                                            "从该订阅的游标位置只读读取消息，不影响消费者",
                                            "從該訂閱的游標位置唯讀讀取訊息，不影響消費者",
                                            "Read-only from the subscription cursor",
                                        ))
                                        .clicked()
                                    {
                                        preview_sub = Some(s.name.clone());
                                        ui.close_menu();
                                    }
                                    ui.separator();
                                    if ui
                                        .button(i18n::t(
                                            lang,
                                            "重置游标（按时间）",
                                            "重置游標（按時間）",
                                            "Reset cursor (time)",
                                        ))
                                        .clicked()
                                    {
                                        action = Some((
                                            SubDialog {
                                                sub: s.name.clone(),
                                                op: SubOp::ResetTime,
                                            },
                                            now_millis().to_string(),
                                            String::new(),
                                        ));
                                        ui.close_menu();
                                    }
                                    if ui
                                        .button(i18n::t(
                                            lang,
                                            "重置游标（按 ID）",
                                            "重置游標（按 ID）",
                                            "Reset cursor (ID)",
                                        ))
                                        .clicked()
                                    {
                                        action = Some((
                                            SubDialog {
                                                sub: s.name.clone(),
                                                op: SubOp::ResetId,
                                            },
                                            "0".into(),
                                            "0".into(),
                                        ));
                                        ui.close_menu();
                                    }
                                    if ui
                                        .button(i18n::t(lang, "跳过 N 条", "跳過 N 條", "Skip N"))
                                        .clicked()
                                    {
                                        action = Some((
                                            SubDialog {
                                                sub: s.name.clone(),
                                                op: SubOp::Skip,
                                            },
                                            "1".into(),
                                            String::new(),
                                        ));
                                        ui.close_menu();
                                    }
                                    if ui
                                        .button(i18n::t(
                                            lang,
                                            "过期 N 秒",
                                            "過期 N 秒",
                                            "Expire N sec",
                                        ))
                                        .clicked()
                                    {
                                        action = Some((
                                            SubDialog {
                                                sub: s.name.clone(),
                                                op: SubOp::Expire,
                                            },
                                            "3600".into(),
                                            String::new(),
                                        ));
                                        ui.close_menu();
                                    }
                                    ui.separator();
                                    if ui
                                        .button(i18n::t(
                                            lang,
                                            "取消订阅",
                                            "取消訂閱",
                                            "Unsubscribe",
                                        ))
                                        .clicked()
                                    {
                                        action = Some((
                                            SubDialog {
                                                sub: s.name.clone(),
                                                op: SubOp::Unsubscribe,
                                            },
                                            String::new(),
                                            String::new(),
                                        ));
                                        ui.close_menu();
                                    }
                                });
                            });
                        });
                    });
            });

        if let Some((dlg, a1, a2)) = action {
            self.sub_arg1 = a1;
            self.sub_arg2 = a2;
            self.sub_dialog = Some(dlg);
        }
        if let Some(sub) = preview_sub {
            self.preview_subscription(sub);
        }
    }

    fn policies_ui(&mut self, ui: &mut egui::Ui) {
        let lang = self.lang;
        let mut want_save = false;
        let mut want_grant = false;
        let mut revoke: Option<String> = None;

        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.heading("命名空间策略");
                ui.label(
                    egui::RichText::new(
                        "留空的字段不会提交；数值 -1 通常表示不限。保存后会自动刷新。",
                    )
                    .weak()
                    .small(),
                );
                ui.label(
                    egui::RichText::new(format!("Bundles: {}", self.bundles_count))
                        .weak()
                        .small(),
                );
                let num_w = 130.0;
                egui::Grid::new("policy_grid")
                    .num_columns(4)
                    .spacing([14.0, 8.0])
                    .min_col_width(110.0)
                    .show(ui, |ui| {
                        ui.label("消息 TTL (秒)");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.ttl)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("去重");
                        ui.checkbox(&mut self.pf.dedup, "启用");
                        ui.end_row();

                        ui.label("保留时间 (分钟)");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.ret_time)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("保留大小 (MB)");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.ret_size)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.end_row();

                        ui.label("积压配额 (bytes)");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.backlog_size)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("复制集群");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.replication)
                                .desired_width(num_w),
                        );
                        ui.end_row();

                        ui.label("发布速率 msg/s");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.publish_msg)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("发布速率 byte/s");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.publish_byte)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.end_row();

                        ui.label("订阅速率 msg/s");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.subscribe_msg)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("调度速率 msg/s");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.dispatch_msg)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.end_row();

                        ui.label("调度速率 byte/s");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.dispatch_byte)
                                .desired_width(num_w)
                                .hint_text("-1"),
                        );
                        ui.label("最大生产者");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.max_producers)
                                .desired_width(num_w)
                                .hint_text("0"),
                        );
                        ui.end_row();

                        ui.label("最大消费者");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.pf.max_consumers)
                                .desired_width(num_w)
                                .hint_text("0"),
                        );
                        ui.label("持久化 E/W/A");
                        ui.horizontal(|ui| {
                            ui.add(
                                egui::TextEdit::singleline(&mut self.pf.ensemble)
                                    .desired_width(44.0),
                            );
                            ui.add(
                                egui::TextEdit::singleline(&mut self.pf.write_q)
                                    .desired_width(44.0),
                            );
                            ui.add(
                                egui::TextEdit::singleline(&mut self.pf.ack_q).desired_width(44.0),
                            );
                        });
                        ui.end_row();
                    });
                ui.add_space(6.0);
                if ui
                    .button(i18n::t(lang, "保存策略", "儲存策略", "Save policies"))
                    .clicked()
                {
                    want_save = true;
                }

                ui.separator();
                ui.heading(i18n::t(
                    lang,
                    "命名空间权限",
                    "命名空間權限",
                    "Namespace permissions",
                ));
                egui::Grid::new("perm_table")
                    .num_columns(3)
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong(i18n::t(lang, "角色", "角色", "Role"));
                        ui.strong(i18n::t(lang, "权限", "權限", "Actions"));
                        ui.strong(i18n::t(lang, "操作", "操作", "Op"));
                        ui.end_row();
                        for (role, actions) in self.perms.clone() {
                            ui.label(&role);
                            ui.label(actions.join(", "));
                            if ui
                                .small_button(i18n::t(lang, "撤销", "撤銷", "Revoke"))
                                .clicked()
                            {
                                revoke = Some(role.clone());
                            }
                            ui.end_row();
                        }
                    });
                ui.horizontal_wrapped(|ui| {
                    ui.label(i18n::t(lang, "角色", "角色", "Role"));
                    ui.add(
                        egui::TextEdit::singleline(&mut self.perm_role)
                            .desired_width(160.0)
                            .hint_text("role name"),
                    );
                    ui.checkbox(&mut self.perm_produce, "produce");
                    ui.checkbox(&mut self.perm_consume, "consume");
                    ui.checkbox(&mut self.perm_functions, "functions");
                    if ui
                        .button(i18n::t(lang, "授予", "授予", "Grant"))
                        .clicked()
                    {
                        want_grant = true;
                    }
                });

                if let Some(pol) = self.policies.clone() {
                    ui.separator();
                    egui::CollapsingHeader::new(i18n::t(
                        lang,
                        "原始策略 JSON",
                        "原始策略 JSON",
                        "Raw policies JSON",
                    ))
                        .id_salt("raw_policies")
                        .show(ui, |ui| {
                            let mut text =
                                serde_json::to_string_pretty(&pol).unwrap_or_default();
                            ui.add(
                                egui::TextEdit::multiline(&mut text)
                                    .code_editor()
                                    .desired_width(f32::INFINITY)
                                    .desired_rows(16)
                                    .interactive(false),
                            );
                        });
                }
            });

        if want_save {
            self.save_policies();
        }
        if want_grant {
            self.grant_permission();
        }
        if let Some(role) = revoke {
            self.revoke_permission(role);
        }
    }

    fn dialogs(&mut self, ctx: &egui::Context) {
        let lang = self.lang;
        if self.server_dialog {
            let mut open = self.server_dialog;
            let title = if self.edit_server.is_some() {
                i18n::t(lang, "编辑连接", "編輯連線", "Edit connection")
            } else {
                i18n::t(lang, "新建连接", "新增連線", "New connection")
            };
            egui::Window::new(title)
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_width(460.0)
                .show(ctx, |ui| {
                    egui::Grid::new("server_form")
                        .num_columns(2)
                        .spacing([10.0, 8.0])
                        .show(ui, |ui| {
                            ui.label("名称");
                            ui.text_edit_singleline(&mut self.draft.name);
                            ui.end_row();
                            ui.label("Admin URL");
                            ui.text_edit_singleline(&mut self.draft.admin_url);
                            ui.end_row();
                            ui.label("Broker URL");
                            ui.text_edit_singleline(&mut self.draft.broker_url);
                            ui.end_row();
                            ui.label("认证");
                            egui::ComboBox::from_id_salt("auth")
                                .selected_text(match self.draft.auth {
                                    AuthKind::None => "无",
                                    AuthKind::Basic => "Basic",
                                    AuthKind::Jwt => "JWT Token",
                                })
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.draft.auth, AuthKind::None, "无");
                                    ui.selectable_value(
                                        &mut self.draft.auth,
                                        AuthKind::Basic,
                                        "Basic",
                                    );
                                    ui.selectable_value(
                                        &mut self.draft.auth,
                                        AuthKind::Jwt,
                                        "JWT Token",
                                    );
                                });
                            ui.end_row();
                            match self.draft.auth {
                                AuthKind::Basic => {
                                    ui.label("用户名");
                                    ui.text_edit_singleline(&mut self.draft.username);
                                    ui.end_row();
                                    ui.label("密码");
                                    ui.add(
                                        egui::TextEdit::singleline(&mut self.draft.password)
                                            .password(true),
                                    );
                                    ui.end_row();
                                }
                                AuthKind::Jwt => {
                                    ui.label("Token");
                                    ui.add(
                                        egui::TextEdit::multiline(&mut self.draft.token)
                                            .desired_rows(3)
                                            .desired_width(320.0),
                                    );
                                    ui.end_row();
                                }
                                AuthKind::None => {}
                            }
                        });
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui
                            .button(i18n::t(lang, "保存", "儲存", "Save"))
                            .clicked()
                        {
                            if let Some(i) = self.edit_server {
                                if i < self.cfg.servers.len() {
                                    self.cfg.servers[i] = self.draft.clone();
                                }
                            } else {
                                self.cfg.servers.push(self.draft.clone());
                                self.server_idx = self.cfg.servers.len() - 1;
                            }
                            let _ = config::save(&self.cfg);
                            self.notify(i18n::t(
                                lang,
                                "已保存连接配置",
                                "已儲存連線設定",
                                "Connection saved",
                            ));
                            self.server_dialog = false;
                            self.edit_server = None;
                        }
                        if ui
                            .button(i18n::t(lang, "取消", "取消", "Cancel"))
                            .clicked()
                        {
                            self.server_dialog = false;
                            self.edit_server = None;
                        }
                    });
                });
            if !open {
                self.server_dialog = false;
            }
        }

        if self.publish_dialog {
            let mut open = self.publish_dialog;
            egui::Window::new("发布消息")
                .open(&mut open)
                .collapsible(false)
                .default_width(560.0)
                .show(ctx, |ui| {
                    ui.label("消息内容");
                    ui.add(
                        egui::TextEdit::multiline(&mut self.publish_payload)
                            .desired_rows(10)
                            .desired_width(f32::INFINITY)
                            .code_editor(),
                    );
                    ui.horizontal(|ui| {
                        ui.label("Key");
                        ui.text_edit_singleline(&mut self.publish_key);
                    });
                    ui.label("属性（每行 key=value）");
                    ui.add(egui::TextEdit::multiline(&mut self.publish_props).desired_rows(3));
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.button("发送").clicked() {
                            self.publish();
                        }
                        if ui.button("关闭").clicked() {
                            self.publish_dialog = false;
                        }
                    });
                });
            if !open {
                self.publish_dialog = false;
            }
        }

        if self.create_topic_dialog {
            let mut open = self.create_topic_dialog;
            egui::Window::new("新建 Topic")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("名称");
                        ui.text_edit_singleline(&mut self.new_topic_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label(i18n::t(lang, "分区数", "分區數", "Partitions"));
                        ui.add(
                            egui::TextEdit::singleline(&mut self.new_topic_partitions)
                                .hint_text("0/1 = 普通")
                                .desired_width(90.0),
                        );
                    });
                    ui.horizontal(|ui| {
                        if ui.button("创建").clicked() {
                            self.create_topic();
                            self.create_topic_dialog = false;
                        }
                        if ui.button("取消").clicked() {
                            self.create_topic_dialog = false;
                        }
                    });
                });
            if !open {
                self.create_topic_dialog = false;
            }
        }

        if self.create_ns_dialog {
            let mut open = self.create_ns_dialog;
            egui::Window::new("新建命名空间")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("租户");
                        egui::ComboBox::from_id_salt("ns_tenant")
                            .selected_text(if self.new_tenant_name.is_empty() {
                                "选择".to_string()
                            } else {
                                self.new_tenant_name.clone()
                            })
                            .show_ui(ui, |ui| {
                                for t in self.tenants.clone() {
                                    ui.selectable_value(&mut self.new_tenant_name, t.clone(), t);
                                }
                            });
                    });
                    ui.horizontal(|ui| {
                        ui.label("名称");
                        ui.text_edit_singleline(&mut self.new_ns_name);
                    });
                    ui.horizontal(|ui| {
                        if ui.button("创建").clicked() {
                            if let (Some(admin), false) =
                                (self.admin.clone(), self.new_tenant_name.is_empty())
                            {
                                let ns = NamespaceRef::new(
                                    self.new_tenant_name.clone(),
                                    self.new_ns_name.trim(),
                                );
                                let tx = self.tx.clone();
                                let ctx = ctx.clone();
                                self.busy += 1;
                                self.spawn(async move {
                                    let ev = match admin.create_namespace(&ns).await {
                                        Ok(()) => Event::Status(format!("已创建 {}", ns.full())),
                                        Err(e) => Event::Error(format!("创建失败: {e}")),
                                    };
                                    let _ = tx.send(ev);
                                    let _ = tx.send(Event::Done("create_ns".into()));
                                    ctx.request_repaint();
                                });
                            }
                            self.ns_cache.clear();
                            self.create_ns_dialog = false;
                        }
                        if ui.button("取消").clicked() {
                            self.create_ns_dialog = false;
                        }
                    });
                });
            if !open {
                self.create_ns_dialog = false;
            }
        }

        if self.delete_server_confirm {
            let mut open = true;
            let name = self
                .cfg
                .servers
                .get(self.server_idx)
                .map(|s| s.name.clone())
                .unwrap_or_default();
            egui::Window::new(i18n::t(lang, "删除连接", "刪除連線", "Delete connection"))
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label(format!(
                        "{} {name} ?",
                        i18n::t(
                            lang,
                            "确认删除该连接配置",
                            "確認刪除該連線設定",
                            "Delete this connection"
                        )
                    ));
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .button(i18n::t(lang, "删除", "刪除", "Delete"))
                            .clicked()
                        {
                            if self.server_idx < self.cfg.servers.len() {
                                self.cfg.servers.remove(self.server_idx);
                            }
                            if self.cfg.servers.is_empty() {
                                self.cfg.servers.push(Server::default());
                            }
                            self.server_idx = 0;
                            let _ = config::save(&self.cfg);
                            self.connected = false;
                            self.admin = None;
                            self.tenants.clear();
                            self.ns_cache.clear();
                            self.topics_cache.clear();
                            self.topics.clear();
                            self.selected_topic = None;
                            self.selected_ns = None;
                            self.delete_server_confirm = false;
                        }
                        if ui
                            .button(i18n::t(lang, "取消", "取消", "Cancel"))
                            .clicked()
                        {
                            self.delete_server_confirm = false;
                        }
                    });
                });
            if !open {
                self.delete_server_confirm = false;
            }
        }

        if self.sub_preview.is_some() {
            let mut open = true;
            let sub_name = self
                .sub_preview
                .as_ref()
                .map(|p| p.sub.clone())
                .unwrap_or_default();
            let mut pick: Option<usize> = None;
            let mut reload = false;
            let backlog_hint = self
                .subs
                .iter()
                .find(|s| s.name == sub_name)
                .map(|s| s.backlog)
                .unwrap_or(0);
            let cursor_hint = self
                .sub_cursors
                .get(&sub_name)
                .map(|(l, e)| format!("{l}:{e}"))
                .unwrap_or_else(|| "-".into());
            egui::Window::new(format!(
                "{} · {sub_name}",
                i18n::t(lang, "订阅积压预览", "訂閱積壓預覽", "Backlog preview")
            ))
            .open(&mut open)
            .default_width(900.0)
            .default_height(560.0)
            .show(ctx, |ui| {
                if let Some(p) = self.sub_preview.as_mut() {
                    ui.horizontal(|ui| {
                        if ui
                            .button(i18n::t(lang, "重新读取", "重新讀取", "Reload"))
                            .clicked()
                        {
                            reload = true;
                        }
                        if p.loading {
                            ui.spinner();
                            ui.label(i18n::t(lang, "读取中 ...", "讀取中 ...", "Loading ..."));
                        } else {
                            ui.label(format!(
                                "{} {} · {}",
                                p.rows.len(),
                                i18n::t(lang, "条未消费消息", "條未消費訊息", "unconsumed"),
                                i18n::t(lang, "只读预览，不改游标", "唯讀預覽，不改游標", "read-only")
                            ));
                        }
                    });
                    ui.separator();
                    if !p.rows.is_empty() {
                        let rows = p.rows.clone();
                        egui::ScrollArea::vertical()
                            .id_salt("sub_preview")
                            .max_height(240.0)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                egui_extras::TableBuilder::new(ui)
                                    .striped(true)
                                    .resizable(true)
                                    .sense(egui::Sense::click())
                                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                                    .column(egui_extras::Column::initial(150.0))
                                    .column(egui_extras::Column::initial(150.0))
                                    .column(egui_extras::Column::initial(120.0))
                                    .column(egui_extras::Column::initial(60.0))
                                    .column(egui_extras::Column::remainder().at_least(160.0))
                                    .header(26.0, |mut h| {
                                        for name in ["ID", "时间", "Key", "大小", "预览"] {
                                            h.col(|ui| {
                                                ui.strong(name);
                                            });
                                        }
                                    })
                                    .body(|body| {
                                        body.rows(24.0, rows.len(), |mut row| {
                                            let i = row.index();
                                            let m = &rows[i];
                                            row.set_selected(p.selected == i);
                                            let mut hit = false;
                                            row.col(|ui| {
                                                if cell_click(
                                                    ui,
                                                    egui::RichText::new(&m.message_id)
                                                        .monospace(),
                                                ) {
                                                    hit = true;
                                                }
                                            });
                                            row.col(|ui| {
                                                if cell_click(
                                                    ui,
                                                    egui::RichText::new(m.publish_time_text()),
                                                ) {
                                                    hit = true;
                                                }
                                            });
                                            row.col(|ui| {
                                                if cell_click(
                                                    ui,
                                                    egui::RichText::new(&m.key),
                                                ) {
                                                    hit = true;
                                                }
                                            });
                                            row.col(|ui| {
                                                if cell_click(
                                                    ui,
                                                    egui::RichText::new(
                                                        m.payload.len().to_string(),
                                                    ),
                                                ) {
                                                    hit = true;
                                                }
                                            });
                                            row.col(|ui| {
                                                let preview = String::from_utf8_lossy(
                                                    &m.payload,
                                                )
                                                .replace('\n', " ")
                                                .chars()
                                                .take(60)
                                                .collect::<String>();
                                                if cell_click(
                                                    ui,
                                                    egui::RichText::new(preview),
                                                ) {
                                                    hit = true;
                                                }
                                            });
                                            if hit {
                                                pick = Some(i);
                                            }
                                        });
                                    });
                            });
                        ui.separator();
                        ui.label(
                            egui::RichText::new(i18n::t(
                                lang,
                                "消息内容",
                                "訊息內容",
                                "Payload",
                            ))
                            .strong(),
                        );
                        egui::ScrollArea::both()
                            .id_salt("sub_preview_payload")
                            .max_height(180.0)
                            .auto_shrink([false, false])
                            .show(ui, |ui| {
                                let mut text = p.decoded.clone();
                                ui.add(
                                    egui::TextEdit::multiline(&mut text)
                                        .code_editor()
                                        .desired_width(f32::INFINITY)
                                        .desired_rows(8)
                                        .interactive(false),
                                );
                            });
                    } else if !p.loading {
                        if backlog_hint > 0 {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{} {} / {} {}",
                                    i18n::t(lang, "读到 0 条，但订阅积压", "讀到 0 條，但訂閱積壓", "Read 0, but backlog is"),
                                    backlog_hint,
                                    i18n::t(lang, "游标", "游標", "cursor"),
                                    cursor_hint
                                ))
                                .weak(),
                            );
                            ui.label(
                                egui::RichText::new(i18n::t(
                                    lang,
                                    "常见原因：定位到老 ledger 超时，或该区间数据已被保留策略清理。可点「重新读取」重试；确认数据是否可读，请回消息页用「从最早」拉取。",
                                    "常見原因：定位到老 ledger 逾時，或該區間資料已被保留策略清理。可點「重新讀取」重試；確認資料是否可讀，請回訊息頁用「從最早」拉取。",
                                    "Likely a seek timeout on an old ledger, or the range was already trimmed by retention. Retry, or fetch from Earliest in the Messages tab to check readability.",
                                ))
                                .weak()
                                .small(),
                            );
                        } else {
                            ui.label(
                                egui::RichText::new(i18n::t(
                                    lang,
                                    "该订阅没有积压消息",
                                    "該訂閱沒有積壓訊息",
                                    "No backlog messages",
                                ))
                                .weak(),
                            );
                        }
                    }
                }
            });
            if let Some(i) = pick {
                let payload = self
                    .sub_preview
                    .as_ref()
                    .and_then(|p| p.rows.get(i).map(|m| m.payload.clone()));
                if let Some(payload) = payload {
                    let text = decode::decode(&payload, self.format, Some(&self.avro_schema));
                    if let Some(p) = self.sub_preview.as_mut() {
                        p.selected = i;
                        p.decoded = text;
                    }
                }
            }
            if reload {
                if let Some(sub) = self.sub_preview.as_ref().map(|p| p.sub.clone()) {
                    self.preview_subscription(sub);
                }
            }
            if !open {
                self.sub_preview = None;
            }
        }

        if self.settings_window {
            let mut open = self.settings_window;
            let mut changed = false;
            egui::Window::new(i18n::t(lang, "设置", "設定", "Settings"))
                .open(&mut open)
                .collapsible(false)
                .default_width(480.0)
                .show(ctx, |ui| {
                    egui::Grid::new("settings_grid")
                        .num_columns(2)
                        .spacing([14.0, 8.0])
                        .show(ui, |ui| {
                            ui.label(i18n::t(lang, "语言", "語言", "Language"));
                            egui::ComboBox::from_id_salt("set_lang")
                                .selected_text(self.lang.label())
                                .show_ui(ui, |ui| {
                                    for l in [Lang::Zh, Lang::ZhHant, Lang::En] {
                                        if ui
                                            .selectable_label(self.lang == l, l.label())
                                            .clicked()
                                        {
                                            self.lang = l;
                                            self.cfg.language = l.code().to_string();
                                            changed = true;
                                        }
                                    }
                                });
                            ui.end_row();
                            ui.label(i18n::t(lang, "主题", "主題", "Theme"));
                            if ui
                                .checkbox(
                                    &mut self.dark,
                                    i18n::t(lang, "深色模式", "深色模式", "Dark mode"),
                                )
                                .changed()
                            {
                                self.cfg.dark_theme = self.dark;
                                changed = true;
                            }
                            ui.end_row();
                            ui.label(i18n::t(
                                lang,
                                "默认拉取条数",
                                "預設拉取筆數",
                                "Default fetch count",
                            ));
                            if ui
                                .add(
                                    egui::DragValue::new(&mut self.peek_count)
                                        .range(1..=5000),
                                )
                                .changed()
                            {
                                self.cfg.peek_count = self.peek_count;
                                changed = true;
                            }
                            ui.end_row();
                            ui.label(i18n::t(lang, "界面缩放", "介面縮放", "UI scale"));
                            let mut scale = self.cfg.font_scale;
                            if ui
                                .add(egui::Slider::new(&mut scale, 0.8..=1.6).step_by(0.05))
                                .changed()
                            {
                                self.cfg.font_scale = scale;
                                ctx.set_zoom_factor(scale);
                                changed = true;
                            }
                            ui.end_row();
                        });
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.label(i18n::t(lang, "配置文件", "設定檔", "Config file"));
                        ui.label(
                            egui::RichText::new(
                                crate::config::config_path().display().to_string(),
                            )
                            .weak()
                            .small(),
                        );
                    });
                    if ui
                        .button(i18n::t(
                            lang,
                            "打开配置目录",
                            "開啟設定目錄",
                            "Open config folder",
                        ))
                        .clicked()
                    {
                        let _ = std::process::Command::new("explorer")
                            .arg(crate::config::config_dir())
                            .spawn();
                    }
                    ui.separator();
                    ui.label(format!(
                        "Pulsar Studio {}",
                        env!("CARGO_PKG_VERSION")
                    ));
                });
            if changed {
                let _ = config::save(&self.cfg);
            }
            if !open {
                self.settings_window = false;
            }
        }

        if let Some(dlg) = self.cluster_dialog.clone() {
            let mut open = true;
            let editing = !matches!(dlg, ClusterDialog::Create);
            let title = if editing {
                i18n::t(lang, "编辑集群", "編輯叢集", "Edit cluster")
            } else {
                i18n::t(lang, "新建集群", "新增叢集", "New cluster")
            };
            egui::Window::new(title)
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    egui::Grid::new("cluster_form")
                        .num_columns(2)
                        .spacing([10.0, 6.0])
                        .show(ui, |ui| {
                            ui.label(i18n::t(lang, "名称", "名稱", "Name"));
                            ui.add_enabled(
                                !editing,
                                egui::TextEdit::singleline(&mut self.cluster_name),
                            );
                            ui.end_row();
                            ui.label("Service URL");
                            ui.text_edit_singleline(&mut self.cluster_service);
                            ui.end_row();
                            ui.label("Broker URL");
                            ui.text_edit_singleline(&mut self.cluster_broker);
                            ui.end_row();
                            ui.label(i18n::t(lang, "对等集群", "對等叢集", "Peers"));
                            ui.add(
                                egui::TextEdit::singleline(&mut self.cluster_peers)
                                    .hint_text("a,b"),
                            );
                            ui.end_row();
                        });
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .button(i18n::t(lang, "保存", "儲存", "Save"))
                            .clicked()
                        {
                            self.save_cluster();
                            self.cluster_dialog = None;
                        }
                        if ui
                            .button(i18n::t(lang, "取消", "取消", "Cancel"))
                            .clicked()
                        {
                            self.cluster_dialog = None;
                        }
                    });
                });
            if !open {
                self.cluster_dialog = None;
            }
        }

        if self.global_window {
            let mut open = self.global_window;
            let needle = self.global_filter.to_lowercase();
            let rows: Vec<crate::model::GlobalTopicRow> = self
                .global_rows
                .iter()
                .filter(|r| needle.is_empty() || r.topic.to_lowercase().contains(&needle))
                .take(600)
                .cloned()
                .collect();
            let total = self.global_rows.len();
            egui::Window::new(i18n::t(
                lang,
                "全局 Topic 概览",
                "全域 Topic 概覽",
                "All topics",
            ))
            .open(&mut open)
            .default_width(900.0)
            .default_height(560.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.global_filter)
                            .hint_text(i18n::t(lang, "过滤", "篩選", "Filter"))
                            .desired_width(240.0),
                    );
                    ui.label(format!("{}/{}", rows.len(), total));
                });
                ui.separator();
                egui::ScrollArea::both()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        egui_extras::TableBuilder::new(ui)
                            .striped(true)
                            .resizable(true)
                            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                            .column(egui_extras::Column::remainder().at_least(280.0))
                            .column(egui_extras::Column::initial(90.0))
                            .column(egui_extras::Column::initial(90.0))
                            .column(egui_extras::Column::initial(90.0))
                            .column(egui_extras::Column::initial(100.0))
                            .header(28.0, |mut header| {
                                for name in [
                                    "Topic",
                                    "In msg/s",
                                    "Out msg/s",
                                    i18n::t(lang, "积压", "積壓", "Backlog"),
                                    i18n::t(lang, "存储", "儲存", "Storage"),
                                ] {
                                    header.col(|ui| {
                                        ui.strong(name);
                                    });
                                }
                            })
                            .body(|body| {
                                body.rows(26.0, rows.len(), |mut row| {
                                    let r = &rows[row.index()];
                                    row.col(|ui| {
                                        ui.label(&r.topic);
                                    });
                                    row.col(|ui| {
                                        ui.label(format!("{:.1}", r.rate_in));
                                    });
                                    row.col(|ui| {
                                        ui.label(format!("{:.1}", r.rate_out));
                                    });
                                    row.col(|ui| {
                                        ui.label(r.backlog.to_string());
                                    });
                                    row.col(|ui| {
                                        ui.label(human_size(r.storage));
                                    });
                                });
                            });
                    });
            });
            if !open {
                self.global_window = false;
            }
        }

        if self.cluster_window {
            let mut open = self.cluster_window;
            let mut want_refresh = false;
            let mut want_new = false;
            let mut edit_cluster: Option<String> = None;
            let mut delete_cluster: Option<String> = None;
            egui::Window::new(i18n::t(lang, "集群管理", "叢集管理", "Clusters"))
                .open(&mut open)
                .default_width(780.0)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(i18n::t(lang, "Broker 版本", "Broker 版本", "Broker version"));
                        ui.label(if self.broker_version.is_empty() {
                            "-".to_string()
                        } else {
                            self.broker_version.clone()
                        });
                        ui.separator();
                        if ui
                            .button(i18n::t(lang, "刷新", "重新整理", "Refresh"))
                            .clicked()
                        {
                            want_refresh = true;
                        }
                        if ui
                            .button(i18n::t(lang, "新建集群", "新增叢集", "New cluster"))
                            .clicked()
                        {
                            want_new = true;
                        }
                    });
                    ui.separator();
                    egui::ScrollArea::both()
                        .auto_shrink([false, false])
                        .max_height(420.0)
                        .show(ui, |ui| {
                    egui::Grid::new("cluster_grid")
                        .num_columns(5)
                        .striped(true)
                        .spacing([14.0, 4.0])
                        .show(ui, |ui| {
                            ui.strong(i18n::t(lang, "集群", "叢集", "Cluster"));
                            ui.strong("Service URL");
                            ui.strong("Broker URL");
                            ui.strong(i18n::t(lang, "对等集群", "對等叢集", "Peers"));
                            ui.strong(i18n::t(lang, "操作", "操作", "Actions"));
                            ui.end_row();
                            for row in &self.cluster_rows {
                                ui.label(&row.name);
                                ui.label(&row.service_url);
                                ui.label(&row.broker_url);
                                ui.label(if row.peers.is_empty() {
                                    "-".to_string()
                                } else {
                                    row.peers.join(",")
                                });
                                ui.horizontal(|ui| {
                                    if ui
                                        .small_button(i18n::t(
                                            lang, "编辑", "編輯", "Edit",
                                        ))
                                        .clicked()
                                    {
                                        edit_cluster = Some(row.name.clone());
                                    }
                                    if ui
                                        .small_button(i18n::t(
                                            lang, "删除", "刪除", "Delete",
                                        ))
                                        .clicked()
                                    {
                                        delete_cluster = Some(row.name.clone());
                                    }
                                });
                                ui.end_row();
                            }
                        });
                    if self.cluster_rows.is_empty() {
                        ui.label(
                            egui::RichText::new(i18n::t(
                                lang,
                                "暂无数据（点击刷新重试）",
                                "暫無資料（點擊重新整理重試）",
                                "No data yet (try refresh)",
                            ))
                            .weak(),
                        );
                    }
                        });
                });
            if want_refresh {
                self.load_cluster_info();
            }
            if want_new {
                self.cluster_name.clear();
                self.cluster_service.clear();
                self.cluster_broker.clear();
                self.cluster_peers.clear();
                self.cluster_dialog = Some(ClusterDialog::Create);
            }
            if let Some(name) = edit_cluster {
                if let Some(row) = self
                    .cluster_rows
                    .iter()
                    .find(|r| r.name == name)
                    .cloned()
                {
                    self.cluster_name = row.name;
                    self.cluster_service = row.service_url;
                    self.cluster_broker = row.broker_url;
                    self.cluster_peers = row.peers.join(",");
                }
                self.cluster_dialog = Some(ClusterDialog::Edit(name));
            }
            if let Some(name) = delete_cluster {
                self.delete_cluster(name);
            }
            if !open {
                self.cluster_window = false;
            }
        }

        if let Some(ns) = self.ns_dialog.clone() {
            let mut open = true;
            egui::Window::new(i18n::t(
                lang,
                "删除命名空间",
                "刪除命名空間",
                "Delete namespace",
            ))
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(format!(
                    "{} {} ?",
                    i18n::t(
                        lang,
                        "确认删除（含全部 Topic 与订阅）",
                        "確認刪除（含全部 Topic 與訂閱）",
                        "Delete (all topics and subscriptions)",
                    ),
                    ns.full()
                ));
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    if ui
                        .button(i18n::t(lang, "删除", "刪除", "Delete"))
                        .clicked()
                    {
                        self.delete_namespace(ns.clone());
                        self.ns_dialog = None;
                    }
                    if ui
                        .button(i18n::t(lang, "取消", "取消", "Cancel"))
                        .clicked()
                    {
                        self.ns_dialog = None;
                    }
                });
            });
            if !open {
                self.ns_dialog = None;
            }
        }

        if let Some(dlg) = self.tenant_dialog.clone() {
            let mut open = true;
            match dlg {
                TenantDialog::Create => {
                    egui::Window::new(i18n::t(lang, "新建租户", "新增租戶", "New tenant"))
                        .open(&mut open)
                        .collapsible(false)
                        .resizable(false)
                        .show(ctx, |ui| {
                            ui.horizontal(|ui| {
                                ui.label(i18n::t(lang, "名称", "名稱", "Name"));
                                ui.text_edit_singleline(&mut self.new_tenant);
                            });
                            ui.horizontal(|ui| {
                                ui.label(i18n::t(lang, "允许集群", "允許叢集", "Allowed cluster"));
                                ui.text_edit_singleline(&mut self.new_tenant_cluster);
                            });
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                if ui
                                    .button(i18n::t(lang, "创建", "建立", "Create"))
                                    .clicked()
                                {
                                    self.create_tenant();
                                    self.tenant_dialog = None;
                                }
                                if ui
                                    .button(i18n::t(lang, "取消", "取消", "Cancel"))
                                    .clicked()
                                {
                                    self.tenant_dialog = None;
                                }
                            });
                        });
                }
                TenantDialog::Edit(t) => {
                    egui::Window::new(i18n::t(lang, "编辑租户", "編輯租戶", "Edit tenant"))
                        .open(&mut open)
                        .collapsible(false)
                        .resizable(false)
                        .show(ctx, |ui| {
                            ui.label(format!("{}: {t}", i18n::t(lang, "租户", "租戶", "Tenant")));
                            ui.horizontal(|ui| {
                                ui.label(i18n::t(lang, "允许集群", "允許叢集", "Allowed clusters"));
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.new_tenant_cluster)
                                        .desired_width(280.0),
                                );
                            });
                            ui.horizontal(|ui| {
                                ui.label(i18n::t(lang, "管理员角色", "管理員角色", "Admin roles"));
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.new_tenant_roles)
                                        .desired_width(280.0),
                                );
                            });
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                if ui
                                    .button(i18n::t(lang, "保存", "儲存", "Save"))
                                    .clicked()
                                {
                                    self.update_tenant(t.clone());
                                    self.tenant_dialog = None;
                                }
                                if ui
                                    .button(i18n::t(lang, "取消", "取消", "Cancel"))
                                    .clicked()
                                {
                                    self.tenant_dialog = None;
                                }
                            });
                        });
                }
                TenantDialog::Delete(t) => {
                    egui::Window::new(i18n::t(lang, "删除租户", "刪除租戶", "Delete tenant"))
                        .open(&mut open)
                        .collapsible(false)
                        .resizable(false)
                        .show(ctx, |ui| {
                            ui.label(format!(
                                "{} {t} ?",
                                i18n::t(
                                    lang,
                                    "确认删除该租户（含全部命名空间）",
                                    "確認刪除該租戶（含全部命名空間）",
                                    "Delete this tenant and all its namespaces",
                                )
                            ));
                            ui.add_space(6.0);
                            ui.horizontal(|ui| {
                                if ui
                                    .button(i18n::t(lang, "删除", "刪除", "Delete"))
                                    .clicked()
                                {
                                    self.delete_tenant(t.clone());
                                    self.tenant_dialog = None;
                                }
                                if ui
                                    .button(i18n::t(lang, "取消", "取消", "Cancel"))
                                    .clicked()
                                {
                                    self.tenant_dialog = None;
                                }
                            });
                        });
                }
            }
            if !open {
                self.tenant_dialog = None;
            }
        }

        if self.sub_create_dialog {
            let mut open = self.sub_create_dialog;
            egui::Window::new(i18n::t(lang, "新建订阅", "新增訂閱", "New subscription"))
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(i18n::t(lang, "名称", "名稱", "Name"));
                        ui.text_edit_singleline(&mut self.new_sub_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label(i18n::t(lang, "起点", "起點", "Start at"));
                        let latest = i18n::t(lang, "最新", "最新", "Latest");
                        let earliest = i18n::t(lang, "最早", "最早", "Earliest");
                        let by_id = i18n::t(lang, "指定 ID", "指定 ID", "Message ID");
                        egui::ComboBox::from_id_salt("sub_start")
                            .selected_text(match self.new_sub_pos {
                                SubStart::Latest => latest,
                                SubStart::Earliest => earliest,
                                SubStart::MessageId => by_id,
                            })
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut self.new_sub_pos,
                                    SubStart::Latest,
                                    latest,
                                );
                                ui.selectable_value(
                                    &mut self.new_sub_pos,
                                    SubStart::Earliest,
                                    earliest,
                                );
                                ui.selectable_value(
                                    &mut self.new_sub_pos,
                                    SubStart::MessageId,
                                    by_id,
                                );
                            });
                    });
                    if self.new_sub_pos == SubStart::MessageId {
                        ui.horizontal(|ui| {
                            ui.label("Ledger");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.new_sub_ledger)
                                    .desired_width(90.0),
                            );
                            ui.label("Entry");
                            ui.add(
                                egui::TextEdit::singleline(&mut self.new_sub_entry)
                                    .desired_width(90.0),
                            );
                        });
                    }
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui
                            .button(i18n::t(lang, "创建", "建立", "Create"))
                            .clicked()
                        {
                            self.create_subscription();
                            self.sub_create_dialog = false;
                        }
                        if ui
                            .button(i18n::t(lang, "取消", "取消", "Cancel"))
                            .clicked()
                        {
                            self.sub_create_dialog = false;
                        }
                    });
                });
            if !open {
                self.sub_create_dialog = false;
            }
        }

        if let Some(dlg) = self.sub_dialog.clone() {
            let mut open = true;
            let title = match dlg.op {
                SubOp::ResetTime => "重置游标（按时间）",
                SubOp::ResetId => "重置游标（按消息 ID）",
                SubOp::Skip => "跳过消息",
                SubOp::Expire => "过期消息",
                SubOp::Unsubscribe => "取消订阅",
            };
            egui::Window::new(title)
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .show(ctx, |ui| {
                    ui.label(format!("订阅：{}", dlg.sub));
                    match dlg.op {
                        SubOp::ResetTime => {
                            ui.label("时间戳（Unix 毫秒）");
                            ui.text_edit_singleline(&mut self.sub_arg1);
                        }
                        SubOp::ResetId => {
                            ui.horizontal(|ui| {
                                ui.label("Ledger");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.sub_arg1)
                                        .desired_width(90.0),
                                );
                                ui.label("Entry");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.sub_arg2)
                                        .desired_width(90.0),
                                );
                            });
                        }
                        SubOp::Skip => {
                            ui.label("跳过条数");
                            ui.text_edit_singleline(&mut self.sub_arg1);
                        }
                        SubOp::Expire => {
                            ui.label("过期秒数");
                            ui.text_edit_singleline(&mut self.sub_arg1);
                        }
                        SubOp::Unsubscribe => {
                            ui.label(
                                egui::RichText::new("该操作会删除订阅及其游标，不可恢复。")
                                    .color(egui::Color32::from_rgb(200, 120, 60)),
                            );
                        }
                    }
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("执行").clicked() {
                            self.run_sub_action();
                        }
                        if ui.button("取消").clicked() {
                            self.sub_dialog = None;
                        }
                    });
                });
            if !open {
                self.sub_dialog = None;
            }
        }
    }
}

impl eframe::App for PulsarStudio {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.dark {
            ctx.set_visuals(egui::Visuals::dark());
        } else {
            ctx.set_visuals(egui::Visuals::light());
        }

        self.drain_events();

        if !self.auto_connected {
            self.auto_connected = true;
            if !self.cfg.servers.is_empty() {
                self.connect();
            }
        }

        self.top_bar(ctx);
        self.tree_panel(ctx);

        egui::TopBottomPanel::bottom("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label(&self.status).on_hover_text(&self.status);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        egui::RichText::new(format!(
                            "Pulsar Studio {}",
                            env!("CARGO_PKG_VERSION")
                        ))
                        .weak(),
                    );
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_topic.is_some() {
                self.detail(ui);
            } else {
                self.topic_list(ui);
            }
        });

        self.dialogs(ctx);

        ctx.request_repaint_after(Duration::from_millis(250));
    }
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn normalize_ts(t: i64) -> i64 {
    if t <= 0 {
        0
    } else if t > 1_000_000_000_000 {
        t
    } else {
        t * 1000
    }
}

/// 表格单元格：铺满整格的可点击标签，返回是否被点击
fn cell_click(ui: &mut egui::Ui, text: egui::RichText) -> bool {
    let size = egui::vec2(ui.available_width(), ui.available_height());
    ui.add_sized(
        size,
        egui::Label::new(text).sense(egui::Sense::click()).truncate(),
    )
    .clicked()
}

fn csv_escape(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn human_size(bytes: i64) -> String {
    let b = bytes as f64;
    if b < 1024.0 {
        format!("{b:.0} B")
    } else if b < 1024.0 * 1024.0 {
        format!("{:.1} KB", b / 1024.0)
    } else if b < 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} MB", b / 1024.0 / 1024.0)
    } else {
        format!("{:.2} GB", b / 1024.0 / 1024.0 / 1024.0)
    }
}
