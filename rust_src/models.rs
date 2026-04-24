use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerProfile {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub client_id: String,
    pub keepalive_secs: u16,
    pub use_tls: bool,
}

impl Default for BrokerProfile {
    fn default() -> Self {
        Self {
            name: "默认 Broker".into(),
            host: "127.0.0.1".into(),
            port: 1883,
            username: String::new(),
            password: String::new(),
            client_id: "mesh-bc-tester-rs".into(),
            keepalive_secs: 60,
            use_tls: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceProfile {
    pub local_id: u64,
    pub name: String,
    pub device_id: String,
    pub up_topic: String,
    pub down_topic: String,
    pub mesh_dev_type: u8,
    pub default_dest_addr: u16,
    pub subscribe_enabled: bool,
}

impl DeviceProfile {
    pub fn default_topics(device_id: &str) -> (String, String) {
        let root = format!("/application/AP-C-BM/device/{device_id}");
        (format!("{root}/up"), format!("{root}/down"))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceEditor {
    pub editing_id: Option<u64>,
    pub name: String,
    pub device_id: String,
    pub up_topic: String,
    pub down_topic: String,
    pub mesh_dev_type: u8,
    pub default_dest_addr: u16,
    pub subscribe_enabled: bool,
}

impl DeviceEditor {
    pub fn from_device(device: &DeviceProfile) -> Self {
        Self {
            editing_id: Some(device.local_id),
            name: device.name.clone(),
            device_id: device.device_id.clone(),
            up_topic: device.up_topic.clone(),
            down_topic: device.down_topic.clone(),
            mesh_dev_type: device.mesh_dev_type,
            default_dest_addr: device.default_dest_addr,
            subscribe_enabled: device.subscribe_enabled,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceRuntimeState {
    pub online: bool,
    pub last_seen: String,
    pub tx_count: u64,
    pub rx_count: u64,
    pub pending_count: u32,
    pub last_opcode: String,
    pub last_result_label: String,
    pub last_result: String,
    pub last_rtt_ms: String,
    pub last_summary: String,
    pub last_version: String,
    pub last_device_model: String,
    pub last_mesh_addr: String,
    pub last_switch_state: String,
    pub last_run_mode: String,
    pub last_remote_network_enable: String,
    pub last_heartbeat_interval: String,
    pub last_group_linkage: String,
    pub last_linkage_mode: String,
    pub last_microwave_setting: String,
    pub last_linkage_group_state: String,
    pub last_scene_id: String,
    pub last_energy_kwh: String,
    pub last_power_w: String,
    pub last_a_light_total: String,
    pub last_a_light_preview: String,
    pub last_group_info: String,
    pub last_scene_summary: String,
    pub last_motion_event: String,
    pub last_mac_addr: String,
    pub last_partition_addr: String,
    pub last_lane_group_addr: String,
    pub last_adjacent_group_addr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogDirection {
    Tx,
    Rx,
    System,
}

impl LogDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Tx => "TX",
            Self::Rx => "RX",
            Self::System => "系统",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub direction: LogDirection,
    pub device_name: String,
    pub device_id: String,
    pub topic: String,
    pub opcode: String,
    pub status: String,
    pub summary: String,
    pub payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferKind {
    BcOta,
    AOta,
    VoiceFile,
    RealtimeVoice,
}

impl TransferKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::BcOta => "BC灯 OTA",
            Self::AOta => "A灯 OTA",
            Self::VoiceFile => "声音文件",
            Self::RealtimeVoice => "实时声音",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub broker: BrokerProfile,
    pub devices: Vec<DeviceProfile>,
    pub next_device_id: u64,
    pub transfer_packet_delay_ms: u64,
    pub transfer_ack_timeout_secs: u64,
    pub transfer_max_retries: u8,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            broker: BrokerProfile::default(),
            devices: Vec::new(),
            next_device_id: 0,
            transfer_packet_delay_ms: 15,
            transfer_ack_timeout_secs: 10,
            transfer_max_retries: 2,
        }
    }
}

impl AppConfig {
    pub fn sanitized_for_disk(&self) -> Self {
        let mut clone = self.clone();
        clone.broker.password.clear();
        clone
    }
}

pub fn now_display() -> String {
    let now: DateTime<Local> = Local::now();
    now.format("%Y-%m-%d %H:%M:%S").to_string()
}
