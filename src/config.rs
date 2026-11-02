use crate::model::Server;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub servers: Vec<Server>,
    #[serde(default = "default_lang")]
    pub language: String,
    #[serde(default)]
    pub dark_theme: bool,
    #[serde(default = "default_peek")]
    pub peek_count: usize,
    #[serde(default = "default_scale")]
    pub font_scale: f32,
}

fn default_lang() -> String {
    "zh".into()
}

fn default_peek() -> usize {
    50
}

fn default_scale() -> f32 {
    1.0
}

impl Default for Config {
    fn default() -> Self {
        Self {
            servers: vec![Server::default()],
            language: default_lang(),
            dark_theme: false,
            peek_count: 50,
            font_scale: 1.0,
        }
    }
}

pub fn config_dir() -> PathBuf {
    let base = std::env::var("USERPROFILE")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| ".".into());
    PathBuf::from(base).join(".pulsar-studio")
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.json")
}

pub fn load() -> Config {
    let path = config_path();
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save(cfg: &Config) -> std::io::Result<()> {
    std::fs::create_dir_all(config_dir())?;
    let json = serde_json::to_string_pretty(cfg).unwrap_or_default();
    std::fs::write(config_path(), json)
}
