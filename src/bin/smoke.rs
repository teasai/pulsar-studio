use pulsar_studio::admin::AdminClient;
use pulsar::{ConsumerOptions, SubType};
use pulsar_studio::messaging::{self, StartPosition};
use pulsar_studio::model::{NamespaceRef, Server, TopicEntry};

fn main() {
    let handle = std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(|| {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(run())
        })
        .expect("spawn");
    if let Err(e) = handle.join() {
        eprintln!("panicked: {e:?}");
    }
}

async fn run() -> anyhow::Result<()> {
    // 用环境变量指定测试集群，默认本机 standalone
    let admin_url = std::env::var("PS_ADMIN_URL")
        .unwrap_or_else(|_| "http://localhost:8080".into());
    let broker_url = std::env::var("PS_BROKER_URL")
        .unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let server = Server {
        name: "smoke".into(),
        admin_url,
        broker_url,
        ..Default::default()
    };

    let admin = AdminClient::new(&server)?;
    println!("clusters   : {:?}", admin.clusters().await?);
    let tenants = admin.tenants().await?;
    println!("tenants    : {:?}", tenants);

    let namespaces = admin.namespaces("public").await?;
    println!("namespaces : {:?}", namespaces);

    let ns = NamespaceRef::new("public", "default");
    let topics = admin.topics(&ns).await?;
    println!("topics     : {} total", topics.len());
    for t in topics.iter().take(5) {
        println!("  - {}", t.short);
    }

    if let Some(t) = topics.first() {
        match admin.topic_stats(t).await {
            Ok(s) => println!(
                "stats      : {} producers={} subs={} backlog={} storage={}B",
                t.short, s.producers, s.subscriptions, s.msg_backlog, s.storage_size
            ),
            Err(e) => println!("stats err  : {e}"),
        }
    }

    let test_topic_full = std::env::var("PS_SMOKE_TOPIC")
        .unwrap_or_else(|_| "persistent://public/default/user-registered".into());
    let test_topic = TopicEntry::from_full(&test_topic_full);
    println!("test topic : {} (read only)", test_topic.short);

    let client = messaging::connect(&server).await?;
    println!("broker     : connected");

    let mut peeked_last: Option<String> = None;
    match messaging::peek(&client, &test_topic.full, 5, StartPosition::Earliest).await {
        Ok(rows) => {
            println!("peek       : {} messages", rows.len());
            for r in &rows {
                println!(
                    "  #{} id={} key='{}' size={} payload={}",
                    r.index,
                    r.message_id,
                    r.key,
                    r.payload.len(),
                    String::from_utf8_lossy(&r.payload)
                );
            }
            peeked_last = rows.last().map(|r| r.message_id.clone());
        }
        Err(e) => println!("peek err   : {e}"),
    }

    // ---- 生产者在线验证（PS_HOLD_PRODUCER=1 时保持连接 10 秒）----
    if std::env::var("PS_HOLD_PRODUCER").is_ok() {
        println!("hold       : building producer ...");
        let mut hold = client.producer().with_topic(&test_topic.full).build().await?;
        for i in 0..5 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            if let Ok(raw) = admin.topic_stats_raw(&test_topic).await {
                let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
                let pubs = v
                    .get("publishers")
                    .and_then(|x| x.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                let rows = pulsar_studio::model::ProducerRow::collect(&v);
                println!(
                    "hold       : t={i} publishers={pubs} rows={} summary={}",
                    rows.len(),
                    pulsar_studio::admin::summarize_stats(&v).producers
                );
                if i == 0 {
                    for r in &rows {
                        println!("  producer: {} rate={:.1} addr={}", r.name, r.rate_in, r.address);
                    }
                }
            }
        }
        let _ = hold.close().await;

        // 同时挂一个非持久消费者，验证「消费者」页解析
        println!("hold       : building consumer ...");
        let probe_sub = format!("pulsar-studio-probe-{}", std::process::id());
        let mut cons = client
            .consumer()
            .with_topic(&test_topic.full)
            .with_consumer_name("pulsar-studio-probe")
            .with_subscription(&probe_sub)
            .with_subscription_type(SubType::Exclusive)
            .with_options(ConsumerOptions::default().durable(false))
            .build::<Vec<u8>>()
            .await?;
        for i in 0..4 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            if let Ok(raw) = admin.topic_stats_raw(&test_topic).await {
                let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
                let rows = pulsar_studio::model::ConsumerRow::collect(&v);
                println!("hold       : c={i} consumers={}", rows.len());
                if i == 0 {
                    for r in &rows {
                        println!(
                            "  consumer: sub={} name={} addr={} ver={}",
                            r.sub, r.name, r.address, r.version
                        );
                    }
                }
            }
        }
        let _ = cons.close().await;
    }

    // ---- 订阅创建 / 删除 ----
    let tmp_sub = "pulsar-studio-tmp-sub";
    match admin.create_subscription(&test_topic, tmp_sub, None).await {
        Ok(()) => println!("sub cr     : {tmp_sub} ok"),
        Err(e) => println!("sub cr     : {e}"),
    }
    match admin.subscriptions(&test_topic).await {
        Ok(list) => println!("sub list   : {list:?}"),
        Err(e) => println!("sub list   : {e}"),
    }
    match admin.unsubscribe(&test_topic, tmp_sub).await {
        Ok(()) => println!("sub del    : {tmp_sub} ok"),
        Err(e) => println!("sub del    : {e}"),
    }

    // ---- 按时间定位 ----
    let ts = chrono::Utc::now().timestamp_millis() - 600_000;
    match admin.message_id_by_timestamp(&test_topic, ts).await {
        Ok((l, e)) => {
            println!("msgid@ts   : {l}:{e}");
            match messaging::peek(&client, &test_topic.full, 3, StartPosition::MessageId(l, e)).await {
                Ok(rows) => println!(
                    "peek@id    : {} messages, first={}",
                    rows.len(),
                    rows.first().map(|r| r.message_id.clone()).unwrap_or_default()
                ),
                Err(e) => println!("peek@id err: {e}"),
            }
        }
        Err(e) => println!("msgid err  : {e}"),
    }

    // ---- 分页：从最后一条之后继续读 ----
    if let Some(prev) = peeked_last.as_ref() {
        let parts: Vec<&str> = prev.split(':').collect();
        if parts.len() >= 2 {
            if let (Ok(l), Ok(e)) = (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
                match messaging::peek(
                    &client,
                    &test_topic.full,
                    3,
                    StartPosition::MessageId(l, e + 1),
                )
                .await
                {
                    Ok(rows) => println!(
                        "page 2     : {} messages, first={}",
                        rows.len(),
                        rows.first().map(|r| r.message_id.clone()).unwrap_or_default()
                    ),
                    Err(e) => println!("page 2 err : {e}"),
                }
            }
        }
    }

    // ---- 指定位置读取（PS_PEEK_AT="ledger:entry"）----
    if let Ok(at) = std::env::var("PS_PEEK_AT") {
        let mut it = at.split(':');
        let l = it.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
        let e = it.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
        println!("peek@      : {l}:{e} on {}", test_topic.full);
        match messaging::peek(&client, &test_topic.full, 5, StartPosition::MessageId(l, e)).await {
            Ok(rows) => {
                println!("peek@ rows : {}", rows.len());
                for r in rows.iter().take(3) {
                    println!("  at       : {} size={}", r.message_id, r.payload.len());
                }
            }
            Err(e) => println!("peek@ err  : {e}"),
        }
    }

    // ---- 订阅管理 ----
    let sub_topic = TopicEntry::from_full("persistent://big-data/go/kyb-main_all_data");
    match admin.subscriptions(&sub_topic).await {
        Ok(subs) => println!("subs       : {subs:?}"),
        Err(e) => println!("subs err   : {e}"),
    }
    match admin.topic_stats_raw(&sub_topic).await {
        Ok(raw) => {
            let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
            let rows = pulsar_studio::model::SubRow::collect(&v);
            for r in &rows {
                println!(
                    "sub row    : {} type={} backlog={} consumers={}",
                    r.name, r.sub_type, r.backlog, r.consumers
                );
            }
            if rows.is_empty() {
                println!("sub row    : <none>");
            }
        }
        Err(e) => println!("sub stats err: {e}"),
    }

    // ---- 策略读写 ----
    match admin
        .set_namespace_policy(&ns, "messageTTL", serde_json::json!(0))
        .await
    {
        Ok(()) => println!("policy w   : messageTTL=0 ok"),
        Err(e) => println!("policy err : {e}"),
    }
    match admin.namespace_policies(&ns).await {
        Ok(p) => println!(
            "policies   : retention={:?} dedup={:?}",
            p.get("retention_policies"),
            p.get("deduplicationEnabled")
        ),
        Err(e) => println!("policies err: {e}"),
    }

    // ---- 命名空间创建/删除 ----
    let test_ns = NamespaceRef::new("public", "pulsar-studio-ns-test");
    match admin.create_namespace(&test_ns).await {
        Ok(()) => println!("ns cr      : {} ok", test_ns.full()),
        Err(e) => println!("ns cr      : {e}"),
    }
    match admin.delete_namespace(&test_ns).await {
        Ok(()) => println!("ns del     : {} ok", test_ns.full()),
        Err(e) => println!("ns del     : {e}"),
    }

    // ---- 内部统计（存储）----
    match admin.internal_stats(&test_topic).await {
        Ok(v) => {
            let st = pulsar_studio::model::StorageSummary::from_value(&v);
            println!(
                "storage    : entries={} size={} ledgers={} state={} last={}",
                st.entries,
                st.total_size,
                st.ledgers.len(),
                st.state,
                st.last_confirmed
            );
        }
        Err(e) => println!("storage err: {e}"),
    }

    // ---- 租户信息 / 更新 ----
    match admin.tenant_info("public").await {
        Ok(v) => println!("tenant info: {v}"),
        Err(e) => println!("tenant err : {e}"),
    }
    match admin
        .update_tenant("public", vec!["standalone".to_string()], vec![])
        .await
    {
        Ok(()) => println!("tenant upd : public ok"),
        Err(e) => println!("tenant upd : {e}"),
    }

    // ---- 集群创建 / 删除 ----
    let tmp_cluster = "pulsar-studio-tmp";
    match admin
        .create_cluster(
            tmp_cluster,
            &server.admin_url,
            &server.broker_url,
            vec![],
        )
        .await
    {
        Ok(()) => println!("cluster cr : {tmp_cluster} ok"),
        Err(e) => println!("cluster cr : {e}"),
    }
    match admin.bundles(&ns).await {
        Ok(n) => println!("bundles    : {n}"),
        Err(e) => println!("bundles err: {e}"),
    }
    match admin.delete_cluster(tmp_cluster).await {
        Ok(()) => println!("cluster del: {tmp_cluster} ok"),
        Err(e) => println!("cluster del: {e}"),
    }

    // ---- 全库搜索 ----
    let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    match messaging::search(
        &client,
        &test_topic.full,
        messaging::SearchOptions {
            needle: "603535174054384".into(),
            from: StartPosition::Earliest,
            max_scan: 5000,
            max_hits: 10,
            time_limit_secs: 30,
        },
        cancel,
        |_s, _h| {},
    )
    .await
    {
        Ok((rows, scanned, truncated)) => {
            println!(
                "search     : scanned={scanned} hits={} truncated={truncated}",
                rows.len()
            );
            for r in rows.iter().take(3) {
                println!("  hit      : {} key={}", r.message_id, r.key);
            }
        }
        Err(e) => println!("search err : {e}"),
    }

    // ---- 全局 Topic 统计 ----
    match admin.all_broker_topics().await {
        Ok(v) => {
            let rows = pulsar_studio::model::GlobalTopicRow::collect(&v);
            println!("global     : {} topics", rows.len());
            for r in rows.iter().take(3) {
                println!(
                    "  top: {} backlog={} in={:.1} storage={}",
                    r.topic, r.backlog, r.rate_in, r.storage
                );
            }
        }
        Err(e) => println!("global err : {e}"),
    }

    // ---- Schema ----
    match admin.topic_schema(&test_topic).await {
        Ok(v) => println!("schema     : {}", v.get("type").and_then(|x| x.as_str()).unwrap_or("-")),
        Err(e) => println!("schema     : {e}"),
    }

    // ---- 租户创建/删除 ----
    let tname = "pulsar-studio-test";
    match admin
        .create_tenant(tname, vec!["standalone".to_string()])
        .await
    {
        Ok(()) => println!("tenant cr  : {tname} ok"),
        Err(e) => println!("tenant cr  : {e}"),
    }
    match admin.delete_tenant(tname).await {
        Ok(()) => println!("tenant del : {tname} ok"),
        Err(e) => println!("tenant del : {e}"),
    }

    // ---- 集群信息 ----
    match admin.cluster_info("standalone").await {
        Ok(v) => println!(
            "cluster    : serviceUrl={}",
            v.get("serviceUrl").and_then(|x| x.as_str()).unwrap_or("-")
        ),
        Err(e) => println!("cluster err: {e}"),
    }
    match admin.broker_version().await {
        Ok(v) => println!("broker ver : {v}"),
        Err(e) => println!("version err: {e}"),
    }

    // ---- 权限 ----
    match admin.namespace_permissions(&ns).await {
        Ok(p) => println!("permissions: {p}"),
        Err(e) => println!("perms err  : {e}"),
    }

    // ---- Avro 解码自测 ----
    use pulsar_studio::decode::{decode, Format};
    let schema_str = r#"{"type":"record","name":"T","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    let schema = apache_avro::Schema::parse_str(schema_str)?;
    let mut rec = apache_avro::types::Record::new(&schema).unwrap();
    rec.put("a", 7);
    rec.put("b", "hello-avro");
    let bytes = apache_avro::to_avro_datum(&schema, rec)?;
    println!("avro       : {}", decode(&bytes, Format::AvroToJson, Some(schema_str)));

    let mut embedded = vec![0u8];
    embedded.extend_from_slice(&42i64.to_be_bytes());
    embedded.extend_from_slice(&bytes);
    println!(
        "avro(emb)  : {}",
        decode(&embedded, Format::AvroEmbeddedToJson, Some(schema_str))
    );

    Ok(())
}
