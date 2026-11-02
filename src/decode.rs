use anyhow::{anyhow, Result};
use chrono::{DateTime, Local, TimeZone};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Text,
    Json,
    Xml,
    Hex,
    MsgPack,
    Yaml,
    Short,
    UShort,
    Int,
    UInt,
    Long,
    ULong,
    Float,
    Double,
    Timestamp,
    Date,
    Time,
    AvroToJson,
    AvroEmbeddedToJson,
}

pub const ALL_FORMATS: &[Format] = &[
    Format::Text,
    Format::Json,
    Format::Xml,
    Format::Hex,
    Format::MsgPack,
    Format::Yaml,
    Format::Short,
    Format::UShort,
    Format::Int,
    Format::UInt,
    Format::Long,
    Format::ULong,
    Format::Float,
    Format::Double,
    Format::Timestamp,
    Format::Date,
    Format::Time,
    Format::AvroToJson,
    Format::AvroEmbeddedToJson,
];

impl Format {
    pub fn label(&self) -> &'static str {
        match self {
            Format::Text => "Text",
            Format::Json => "JSON",
            Format::Xml => "XML",
            Format::Hex => "Hex",
            Format::MsgPack => "MsgPack",
            Format::Yaml => "YAML",
            Format::Short => "Short (int16)",
            Format::UShort => "Unsigned Short",
            Format::Int => "Int (int32)",
            Format::UInt => "Unsigned Int",
            Format::Long => "Long (int64)",
            Format::ULong => "Unsigned Long",
            Format::Float => "Float (f32)",
            Format::Double => "Double (f64)",
            Format::Timestamp => "Timestamp",
            Format::Date => "Date",
            Format::Time => "Time",
            Format::AvroToJson => "Avro → JSON",
            Format::AvroEmbeddedToJson => "Avro (Embedded) → JSON",
        }
    }
}

pub fn decode(data: &[u8], format: Format, schema: Option<&str>) -> String {
    if data.is_empty() {
        return "<empty payload>".into();
    }
    match try_decode(data, format, schema) {
        Ok(s) => s,
        Err(e) => format!(
            "[{:?} 解码失败] {}\n\n---- 原始数据(hex) ----\n{}",
            format,
            e,
            hex_dump(data)
        ),
    }
}

fn try_decode(data: &[u8], format: Format, schema: Option<&str>) -> Result<String> {
    match format {
        Format::Text => Ok(decode_text(data)),
        Format::Json => {
            let v: serde_json::Value = serde_json::from_slice(data)?;
            Ok(serde_json::to_string_pretty(&v)?)
        }
        Format::Xml => Ok(pretty_xml(&decode_text(data))),
        Format::Hex => Ok(hex_dump(data)),
        Format::MsgPack => {
            let v: serde_json::Value = rmp_serde::from_slice(data)?;
            Ok(serde_json::to_string_pretty(&v)?)
        }
        Format::Yaml => {
            let v: serde_json::Value = serde_yaml::from_slice(data)?;
            Ok(serde_yaml::to_string(&v)?)
        }
        Format::Short => Ok(read_int::<2, i16>(data)?.to_string()),
        Format::UShort => Ok(read_int::<2, u16>(data)?.to_string()),
        Format::Int => Ok(read_int::<4, i32>(data)?.to_string()),
        Format::UInt => Ok(read_int::<4, u32>(data)?.to_string()),
        Format::Long => Ok(read_int::<8, i64>(data)?.to_string()),
        Format::ULong => Ok(read_int::<8, u64>(data)?.to_string()),
        Format::Float => Ok(read_int::<4, f32>(data)?.to_string()),
        Format::Double => Ok(read_int::<8, f64>(data)?.to_string()),
        Format::Timestamp => {
            let millis = read_int::<8, i64>(data)?;
            let millis = if millis > 1_000_000_000_000 { millis } else { millis * 1000 };
            let dt: DateTime<Local> = Local
                .timestamp_millis_opt(millis)
                .single()
                .ok_or_else(|| anyhow!("时间戳超出范围"))?;
            Ok(format!("{}\n(unix ms: {})", dt.format("%Y-%m-%d %H:%M:%S%.3f"), millis))
        }
        Format::Date => {
            let days = read_int::<4, i32>(data)?;
            let dt = Local
                .timestamp_opt(days as i64 * 86_400, 0)
                .single()
                .ok_or_else(|| anyhow!("日期超出范围"))?;
            Ok(format!("{}\n(days since epoch: {})", dt.format("%Y-%m-%d"), days))
        }
        Format::Time => {
            let ms = read_int::<4, i32>(data)?;
            let total = ms as i64;
            Ok(format!(
                "{:02}:{:02}:{:02}.{:03}\n(ms since midnight: {})",
                total / 3_600_000,
                (total / 60_000) % 60,
                (total / 1000) % 60,
                total % 1000,
                ms
            ))
        }
        Format::AvroToJson => avro_to_json(data, schema, false),
        Format::AvroEmbeddedToJson => avro_to_json(data, schema, true),
    }
}

fn avro_to_json(data: &[u8], schema: Option<&str>, embedded: bool) -> Result<String> {
    let schema_str = schema
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow!("需要 Avro Schema（在消息面板下方填写）"))?;
    let parsed = apache_avro::Schema::parse_str(schema_str)?;
    let (body, schema_id) = if embedded {
        if data.len() < 10 {
            return Err(anyhow!("Embedded Avro 数据长度不足 10 字节"));
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&data[1..9]);
        (data.get(9..).unwrap_or(&[]), i64::from_be_bytes(id))
    } else {
        (data, -1)
    };
    let mut slice = body;
    let value = apache_avro::from_avro_datum(&parsed, &mut slice, None)?;
    let json = avro_value_to_json(&value);
    let mut out = serde_json::to_string_pretty(&json)?;
    if embedded {
        out.push_str(&format!("\n\n(schema id: {schema_id})"));
    }
    Ok(out)
}

fn avro_value_to_json(v: &apache_avro::types::Value) -> serde_json::Value {
    use apache_avro::types::Value as AV;
    use serde_json::{json, Value};
    match v {
        AV::Null => Value::Null,
        AV::Boolean(b) => json!(b),
        AV::Int(i) => json!(i),
        AV::Long(l) => json!(l),
        AV::Float(f) => json!(f),
        AV::Double(d) => json!(d),
        AV::String(s) => json!(s),
        AV::Bytes(b) => json!(String::from_utf8_lossy(b)),
        AV::Array(items) => Value::Array(items.iter().map(avro_value_to_json).collect()),
        AV::Map(m) => Value::Object(m.iter().map(|(k, v)| (k.clone(), avro_value_to_json(v))).collect()),
        AV::Record(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), avro_value_to_json(v)))
                .collect(),
        ),
        AV::Union(_, inner) => avro_value_to_json(inner),
        other => json!(format!("{other:?}")),
    }
}

fn decode_text(data: &[u8]) -> String {
    String::from_utf8(data.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(data).to_string())
}

fn read_int<const N: usize, T>(data: &[u8]) -> Result<T>
where
    T: FromBeBytes<N>,
{
    if data.len() < N {
        return Err(anyhow!("需要 {} 字节，实际 {} 字节", N, data.len()));
    }
    let mut buf = [0u8; N];
    buf.copy_from_slice(&data[..N]);
    Ok(T::from_be_bytes(buf))
}

pub trait FromBeBytes<const N: usize> {
    fn from_be_bytes(b: [u8; N]) -> Self;
}

macro_rules! impl_be {
    ($($t:ty => $n:literal),* $(,)?) => {
        $(impl FromBeBytes<$n> for $t {
            fn from_be_bytes(b: [u8; $n]) -> Self {
                <$t>::from_be_bytes(b)
            }
        })*
    };
}

impl_be!(i16 => 2, u16 => 2, i32 => 4, u32 => 4, i64 => 8, u64 => 8, f32 => 4, f64 => 8);

pub fn hex_dump(bytes: &[u8]) -> String {
    let mut out = String::new();
    out.push_str("Offset   | 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F | Decoded\n");
    out.push_str("---------+-------------------------------------------------+----------------\n");
    for (i, chunk) in bytes.chunks(16).enumerate() {
        let offset = i * 16;
        let mut hex = String::new();
        let mut ascii = String::new();
        for (j, b) in chunk.iter().enumerate() {
            hex.push_str(&format!("{:02X} ", b));
            if j == 7 {
                hex.push(' ');
            }
            ascii.push(if (33..=126).contains(b) { *b as char } else { '.' });
        }
        out.push_str(&format!("{:08X} | {:<49} | {}\n", offset, hex.trim_end(), ascii));
    }
    out
}

/// Minimal XML pretty printer: validates with quick-xml and re-indents.
fn pretty_xml(input: &str) -> String {
    let mut reader = quick_xml::Reader::from_str(input);
    let mut out = String::new();
    let mut depth: usize = 0;
    loop {
        match reader.read_event() {
            Ok(quick_xml::events::Event::Start(e)) => {
                out.push_str(&format!(
                    "{}{}\n",
                    "  ".repeat(depth),
                    String::from_utf8_lossy(e.name().as_ref())
                ));
                depth += 1;
            }
            Ok(quick_xml::events::Event::End(e)) => {
                depth = depth.saturating_sub(1);
                out.push_str(&format!(
                    "{}</{}>\n",
                    "  ".repeat(depth),
                    String::from_utf8_lossy(e.name().as_ref())
                ));
            }
            Ok(quick_xml::events::Event::Text(t)) => {
                if let Ok(s) = t.unescape() {
                    let s = s.trim();
                    if !s.is_empty() {
                        out.push_str(&format!("{}{}\n", "  ".repeat(depth), s));
                    }
                }
            }
            Ok(quick_xml::events::Event::Empty(e)) => {
                out.push_str(&format!(
                    "{}<{}/>\n",
                    "  ".repeat(depth),
                    String::from_utf8_lossy(e.name().as_ref())
                ));
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return format!("XML 解析错误: {e}\n\n{input}"),
            _ => {}
        }
    }
    out
}
