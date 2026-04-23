use std::{fs, path::PathBuf};

use dirs::config_dir;

use crate::models::AppConfig;

pub fn config_path() -> PathBuf {
    config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("mesh-bc-tester-rs")
        .join("state.json")
}

pub fn load_config() -> AppConfig {
    let path = config_path();
    match fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
        Err(_) => AppConfig::default(),
    }
}

pub fn save_config(config: &AppConfig) -> Result<(), String> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let text = serde_json::to_string_pretty(&config.sanitized_for_disk())
        .map_err(|err| err.to_string())?;
    fs::write(path, text).map_err(|err| err.to_string())
}
