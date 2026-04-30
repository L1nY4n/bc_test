use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use eframe::egui::{self, FontData, FontDefinitions, FontFamily, FontTweak, RichText, TextEdit};
use egui_extras::{Column, TableBuilder};
use rfd::FileDialog;
use serde_json::Value;

#[cfg(test)]
use crate::models::ActiveTransfer;
use crate::models::{
    AppConfig, BrokerProfile, DeviceEditor, DeviceProfile, DeviceRuntimeState, LogDirection,
    LogEntry, OperationRecord, TransferKind, TransferSnapshot, UiThemeMode, now_display,
};
use crate::mqtt::{MqttEvent, MqttRuntime};
use crate::protocol::{
    COMMANDS, FieldKind, build_command_payload, build_transfer_packets, classify_execution_result,
    command_by_key, current_time_stamp, decode_payload_details, expected_response_opcode,
    parse_opcode, redact_json, response_can_omit_timestamp, summarize_payload, transfer_preview,
};
use crate::store::{load_config, save_config};
use crate::transfer_engine::{
    TransferEngine, TransferEngineCommand, TransferEngineEvent, TransferEngineSettings,
};

pub struct MeshBcTesterApp {
    config: AppConfig,
    broker_editor: BrokerProfile,
    device_editor: DeviceEditor,
    device_editor_open: bool,
    selected_devices: BTreeSet<u64>,
    auto_discovery_enabled: bool,
    auto_import_discovered: bool,
    discovered_devices: Vec<DiscoveredDevice>,
    discovery_section_had_candidates: bool,
    selected_log_index: Option<usize>,
    command_key: String,
    command_form: BTreeMap<String, String>,
    raw_json_text: String,
    raw_expected_opcode: String,
    connection_status: String,
    broker_connected: bool,
    mqtt: MqttRuntime,
    transfer_engine: TransferEngine,
    runtime_states: HashMap<u64, DeviceRuntimeState>,
    logs: Vec<LogEntry>,
    recent_operations: Vec<OperationRecord>,
    show_selected_logs_only: bool,
    follow_latest_logs: bool,
    log_filter_text: String,
    ota_transfer_kind: TransferKind,
    ota_transfer_file: String,
    voice_transfer_kind: TransferKind,
    voice_transfer_file: String,
    transfer_version: u8,
    transfer_voice_name: String,
    transfer_packet_delay_ms: u64,
    transfer_ack_timeout_secs: u64,
    bc_ota_start_ack_timeout_secs: u64,
    transfer_max_retries: u8,
    voice_transfer_packet_delay_ms: u64,
    voice_transfer_ack_timeout_secs: u64,
    voice_transfer_max_retries: u8,
    system_notice: String,
    needs_connection_recovery: bool,
    pending_confirmation: Option<PendingConfirmation>,
    pending_file_dialog: Option<PendingFileDialog>,
    pending_requests: Vec<PendingRequest>,
    transfer_snapshots: Vec<TransferSnapshot>,
    #[cfg(test)]
    active_transfers: Vec<ActiveTransfer>,
    device_last_seen_at: HashMap<u64, Instant>,
}

struct PendingRequest {
    device_local_id: u64,
    device_name: String,
    device_id: String,
    topic: String,
    opcode: u32,
    expected_opcode: u32,
    sent_at: Instant,
    time_stamp: Option<u64>,
}

#[derive(Clone)]
struct DiscoveredDevice {
    device_id: String,
    up_topic: String,
    down_topic: String,
    suggested_name: String,
    dev_model: String,
    version: String,
    mesh_dev_type: u8,
    default_dest_addr: Option<u16>,
    last_opcode: String,
    last_summary: String,
    discovery_reason: String,
    first_seen: String,
    last_seen: String,
    last_topic: String,
    seen_count: u32,
}

struct PendingConfirmation {
    title: String,
    detail: String,
    action: PendingAction,
}

#[derive(Clone, Copy)]
enum ChipTone {
    Warning,
}

enum PendingAction {
    PresetSend {
        items: Vec<(DeviceProfile, Value)>,
    },
    RawSend {
        items: Vec<(DeviceProfile, Value, Option<u32>)>,
    },
    TransferQueue {
        devices: Vec<DeviceProfile>,
        packets: Vec<Value>,
        preview: Value,
        byte_size: usize,
        kind: TransferKind,
    },
}

struct PendingFileDialog {
    kind: PendingFileDialogKind,
    rx: Receiver<Option<PathBuf>>,
}

enum PendingFileDialogKind {
    OtaTransferFile,
    VoiceTransferFile,
    EvidenceExport,
}

const TRANSFER_PACKET_DELAY_MS: u64 = 15;
const TRANSFER_ACK_TIMEOUT_SECS: u64 = 10;
const BC_OTA_START_ACK_TIMEOUT_SECS: u64 = 20;
const TRANSFER_MAX_RETRIES: u8 = 2;
const MAX_TRANSFER_BYTES: usize = 10 * 1024 * 1024;
const DEVICE_OFFLINE_TIMEOUT_SECS: u64 = 120;
const MAX_OPERATION_HISTORY: usize = 300;
const MAX_DISCOVERED_DEVICES: usize = 128;
const MAX_MQTT_EVENTS_PER_FRAME: usize = 250;
#[cfg(test)]
const TRANSFER_PACKET_LOG_INTERVAL: usize = 100;
const COMPACT_CONTROL_HEIGHT: f32 = 22.0;
const COMPACT_LABEL_HEIGHT: f32 = 20.0;
const COMPACT_ICON_BUTTON_SIZE: egui::Vec2 = egui::vec2(28.0, 24.0);
const TABLE_ROW_HEIGHT: f32 = COMPACT_CONTROL_HEIGHT;
const TOP_TOOLBAR_HEIGHT: f32 = 34.0;
const TOP_TOOLBAR_NOTICE_HEIGHT: f32 = 24.0;
const TOP_STATUS_WIDTH: f32 = 160.0;
const MENU_FIELD_LABEL_WIDTH: f32 = 62.0;
const ACTION_FIELD_LABEL_WIDTH: f32 = 72.0;
const ACTION_BUTTON_INDENT_WIDTH: f32 = 76.0;
const DISCOVERED_ROW_ACTION_SLOT_WIDTH: f32 = 138.0;
const DEVICE_ROW_CONTENT_CLICK_INSET: f32 = 46.0;
const DISCOVERED_ROW_CONTENT_CLICK_INSET: f32 = 14.0;
const REQUEST_STATUS_TABLE_HEIGHT: f32 = 90.0;
const TRANSFER_STATUS_TABLE_HEIGHT: f32 = 120.0;
const RECENT_OPERATIONS_TABLE_HEIGHT: f32 = 120.0;
const DISCOVERY_UP_TOPIC_FILTERS: [&str; 2] = [
    "/application/AP-C-BM/device/+/up",
    "/application/AP-C-BM/device/+/up/#",
];

impl MeshBcTesterApp {
    fn blank_device_editor() -> DeviceEditor {
        DeviceEditor {
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
            ..Default::default()
        }
    }

    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        configure_chinese_fonts(&cc.egui_ctx);
        let mut config = load_config();
        if config.next_device_id == 0 {
            config.next_device_id = 1;
        }
        let broker_editor = config.broker.clone();
        let transfer_packet_delay_ms = if config.transfer_packet_delay_ms == 0 {
            TRANSFER_PACKET_DELAY_MS
        } else {
            config.transfer_packet_delay_ms
        };
        let transfer_ack_timeout_secs = if config.transfer_ack_timeout_secs == 0 {
            TRANSFER_ACK_TIMEOUT_SECS
        } else {
            config.transfer_ack_timeout_secs
        };
        let bc_ota_start_ack_timeout_secs = if config.bc_ota_start_ack_timeout_secs == 0 {
            BC_OTA_START_ACK_TIMEOUT_SECS
        } else {
            config.bc_ota_start_ack_timeout_secs
        };
        let transfer_max_retries = if config.transfer_max_retries == 0 {
            TRANSFER_MAX_RETRIES
        } else {
            config.transfer_max_retries
        };
        let voice_transfer_packet_delay_ms = if config.voice_transfer_packet_delay_ms == 0 {
            transfer_packet_delay_ms
        } else {
            config.voice_transfer_packet_delay_ms
        };
        let voice_transfer_ack_timeout_secs = if config.voice_transfer_ack_timeout_secs == 0 {
            transfer_ack_timeout_secs
        } else {
            config.voice_transfer_ack_timeout_secs
        };
        let voice_transfer_max_retries = if config.voice_transfer_max_retries == 0 {
            transfer_max_retries
        } else {
            config.voice_transfer_max_retries
        };
        let command_key = COMMANDS
            .first()
            .map(|spec| spec.key.to_string())
            .unwrap_or_else(|| "query_bc_info".into());
        let command_form = default_form_for_command(&command_key);
        let runtime_states = config
            .devices
            .iter()
            .map(|device| (device.local_id, DeviceRuntimeState::default()))
            .collect();
        let transfer_engine = TransferEngine::new();
        let mut mqtt = MqttRuntime::default();
        mqtt.set_event_mirror(Some(transfer_engine.mqtt_event_sender()));

        Self {
            config,
            broker_editor,
            device_editor: Self::blank_device_editor(),
            device_editor_open: false,
            selected_devices: BTreeSet::new(),
            auto_discovery_enabled: true,
            auto_import_discovered: false,
            discovered_devices: Vec::new(),
            discovery_section_had_candidates: false,
            selected_log_index: None,
            command_key,
            command_form,
            raw_json_text: serde_json::to_string_pretty(&serde_json::json!({
                "opcode": 70,
                "value": 0,
                "time_stamp": current_time_stamp(),
            }))
            .unwrap(),
            raw_expected_opcode: String::new(),
            connection_status: "未连接".into(),
            broker_connected: false,
            mqtt,
            transfer_engine,
            runtime_states,
            logs: Vec::new(),
            recent_operations: Vec::new(),
            show_selected_logs_only: false,
            follow_latest_logs: true,
            log_filter_text: String::new(),
            ota_transfer_kind: TransferKind::BcOta,
            ota_transfer_file: String::new(),
            voice_transfer_kind: TransferKind::VoiceFile,
            voice_transfer_file: String::new(),
            transfer_version: 1,
            transfer_voice_name: "voice.adpcm".into(),
            transfer_packet_delay_ms,
            transfer_ack_timeout_secs,
            bc_ota_start_ack_timeout_secs,
            transfer_max_retries,
            voice_transfer_packet_delay_ms,
            voice_transfer_ack_timeout_secs,
            voice_transfer_max_retries,
            system_notice: String::new(),
            needs_connection_recovery: false,
            pending_confirmation: None,
            pending_file_dialog: None,
            pending_requests: Vec::new(),
            transfer_snapshots: Vec::new(),
            #[cfg(test)]
            active_transfers: Vec::new(),
            device_last_seen_at: HashMap::new(),
        }
    }

    fn append_log(&mut self, entry: LogEntry) {
        self.logs.push(entry);
        if self.logs.len() > 2000 {
            self.logs.drain(0..self.logs.len().saturating_sub(2000));
        }
    }

    fn append_operation(&mut self, record: OperationRecord) {
        self.recent_operations.push(record);
        if self.recent_operations.len() > MAX_OPERATION_HISTORY {
            self.recent_operations.drain(
                0..self
                    .recent_operations
                    .len()
                    .saturating_sub(MAX_OPERATION_HISTORY),
            );
        }
    }

    fn set_device_result(state: &mut DeviceRuntimeState, label: &str, text: String) {
        state.last_result_label = label.to_string();
        state.last_result = text;
    }

    fn selected_device_refs(&self) -> Vec<&DeviceProfile> {
        self.config
            .devices
            .iter()
            .filter(|device| self.selected_devices.contains(&device.local_id))
            .collect()
    }

    fn device_by_topic(&self, topic: &str) -> Option<&DeviceProfile> {
        self.config
            .devices
            .iter()
            .find(|device| topic_matches_device_up_topic(&device.up_topic, topic))
    }

    fn process_mqtt_events(&mut self) {
        self.collect_pending_timeouts();
        self.collect_device_offline_timeouts();
        for _ in 0..MAX_MQTT_EVENTS_PER_FRAME {
            let Ok(event) = self.mqtt.events_rx.try_recv() else {
                break;
            };
            match event {
                MqttEvent::Connection {
                    generation,
                    connected,
                    reconnecting,
                    message,
                } => {
                    if !self.mqtt.is_current_generation(generation) {
                        continue;
                    }
                    self.handle_mqtt_connection_state(connected, reconnecting, &message);
                    self.append_log(LogEntry {
                        timestamp: now_display(),
                        direction: LogDirection::System,
                        device_name: "系统".into(),
                        device_id: "-".into(),
                        topic: "-".into(),
                        opcode: "-".into(),
                        status: "信息".into(),
                        summary: message.clone(),
                        payload: message,
                    });
                }
                MqttEvent::Message {
                    generation,
                    topic,
                    payload,
                } => {
                    if !self.mqtt.is_current_generation(generation) {
                        continue;
                    }
                    let device = self.device_by_topic(&topic).cloned();
                    let parsed: Option<Value> = serde_json::from_str(&payload).ok();
                    if device.is_none() {
                        self.observe_discovered_message(&topic, parsed.as_ref(), &payload);
                    }
                    let mut rx_status = "接收".to_string();
                    if let (Some(device), Some(parsed)) = (&device, &parsed) {
                        self.resolve_pending_request(device, parsed);
                        if let Some((status, _)) = classify_execution_result(parsed) {
                            rx_status = status.to_string();
                        } else if parsed
                            .get("opcode")
                            .and_then(Value::as_u64)
                            .map(|opcode| opcode == 0x10 || opcode == 0x1B)
                            .unwrap_or(false)
                        {
                            rx_status = "事件".into();
                        }
                    }
                    let (device_name, device_id, opcode, summary) = if let Some(device) = &device {
                        let opcode = parsed
                            .as_ref()
                            .and_then(|value| value.get("opcode"))
                            .and_then(Value::as_u64)
                            .map(|value| format!("0x{value:02X}"))
                            .unwrap_or_else(|| "-".into());
                        let summary = parsed
                            .as_ref()
                            .map(summarize_payload)
                            .unwrap_or_else(|| payload.chars().take(96).collect());
                        let state = self.runtime_states.entry(device.local_id).or_default();
                        state.online = true;
                        state.last_seen = now_display();
                        state.rx_count += 1;
                        self.device_last_seen_at
                            .insert(device.local_id, Instant::now());
                        state.last_opcode = opcode.clone();
                        state.last_summary = summary.clone();
                        if let Some(parsed) = &parsed {
                            Self::update_device_state_from_payload(state, parsed);
                        }
                        (
                            device.name.clone(),
                            device.device_id.clone(),
                            opcode,
                            summary,
                        )
                    } else {
                        (
                            "(未匹配设备)".into(),
                            "-".into(),
                            "-".into(),
                            payload.chars().take(96).collect(),
                        )
                    };

                    let display_payload = parsed
                        .as_ref()
                        .map(redact_json)
                        .map(|value| {
                            serde_json::to_string_pretty(&value).unwrap_or_else(|_| payload.clone())
                        })
                        .unwrap_or_else(|| payload.clone());

                    self.append_log(LogEntry {
                        timestamp: now_display(),
                        direction: LogDirection::Rx,
                        device_name,
                        device_id,
                        topic,
                        opcode,
                        status: rx_status,
                        summary,
                        payload: display_payload,
                    });
                }
            }
        }
    }

    fn process_transfer_engine_events(&mut self) {
        for _ in 0..MAX_MQTT_EVENTS_PER_FRAME {
            let Ok(event) = self.transfer_engine.try_recv_event() else {
                break;
            };
            match event {
                TransferEngineEvent::Snapshot(transfers) => {
                    self.transfer_snapshots = transfers;
                }
                TransferEngineEvent::Log(entry) => {
                    self.append_log(entry);
                }
                TransferEngineEvent::Operation(record) => {
                    self.append_operation(record);
                }
                TransferEngineEvent::DeviceResult {
                    device_local_id,
                    label,
                    text,
                } => {
                    if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                        Self::set_device_result(state, &label, text);
                    }
                }
                TransferEngineEvent::SystemNotice(text) => {
                    self.system_notice = text;
                }
            }
        }
    }

    fn handle_mqtt_connection_state(&mut self, connected: bool, reconnecting: bool, message: &str) {
        #[cfg(not(test))]
        let _ = reconnecting;
        self.broker_connected = connected;
        self.connection_status = message.to_string();
        if connected {
            if self.needs_connection_recovery {
                self.mqtt.reapply_subscriptions();
                #[cfg(test)]
                self.resume_transfers_after_reconnect();
                self.needs_connection_recovery = false;
            }
            return;
        }

        for state in self.runtime_states.values_mut() {
            state.online = false;
            state.pending_count = 0;
        }
        self.pending_requests.clear();
        #[cfg(test)]
        self.pause_transfers_for_connection_loss(reconnecting);
        self.needs_connection_recovery = true;
    }

    #[cfg(test)]
    fn pause_transfers_for_connection_loss(&mut self, reconnecting: bool) {
        for transfer in &mut self.active_transfers {
            if transfer.terminal {
                continue;
            }
            let resume_index = transfer_resume_index_after_disconnect(transfer);
            transfer.next_index = resume_index;
            transfer.waiting_ack_opcode = None;
            transfer.waiting_since = None;
            transfer.paused = true;
            transfer.next_send_at = Instant::now();
            transfer.status = if reconnecting {
                format!(
                    "连接断开，自动重连后从第{}包继续",
                    transfer_display_packet_number(transfer.kind, resume_index)
                )
            } else {
                format!(
                    "连接已断开，待重连后从第{}包继续",
                    transfer_display_packet_number(transfer.kind, resume_index)
                )
            };
            if let Some(state) = self.runtime_states.get_mut(&transfer.device_local_id) {
                Self::set_device_result(state, "断线", transfer.status.clone());
            }
        }
    }

    #[cfg(test)]
    fn resume_transfers_after_reconnect(&mut self) {
        let resume_at = Instant::now() + Duration::from_millis(self.transfer_packet_delay_ms);
        for transfer in &mut self.active_transfers {
            if transfer.terminal {
                continue;
            }
            transfer.paused = false;
            transfer.waiting_ack_opcode = None;
            transfer.waiting_since = None;
            transfer.next_send_at = resume_at;
            transfer.status = format!(
                "连接已恢复，从第{}包继续",
                transfer_display_packet_number(transfer.kind, transfer.next_index)
            );
            if let Some(state) = self.runtime_states.get_mut(&transfer.device_local_id) {
                Self::set_device_result(state, "恢复", transfer.status.clone());
            }
        }
    }

    fn update_device_state_from_payload(state: &mut DeviceRuntimeState, payload: &Value) {
        let Some(opcode) = payload
            .get("opcode")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
        else {
            return;
        };

        match opcode {
            0x47 => {
                if let Some(version) = payload.get("version").and_then(Value::as_str) {
                    state.last_version = version.to_string();
                }
                if let Some(model) = payload.get("dev_model").and_then(Value::as_str) {
                    state.last_device_model = model.to_string();
                }
            }
            0x4F => {
                if let Some(mesh_addr) = payload.get("value").and_then(Value::as_str) {
                    state.last_mesh_addr = mesh_addr.to_string();
                }
            }
            0x1B => {
                if let Some(value) = payload.get("value").and_then(Value::as_str)
                    && let Ok(bytes) = crate::protocol::hex_to_bytes(value)
                    && bytes.len() >= 2
                {
                    state.last_switch_state = bytes[0].to_string();
                    state.last_run_mode = bytes[1].to_string();
                    if bytes.len() >= 3 {
                        state.last_version = format!("0x{:02X}", bytes[2]);
                    }
                }
            }
            0x1D => {
                if let Some(value) = payload.get("value").and_then(Value::as_str)
                    && let Ok(bytes) = crate::protocol::hex_to_bytes(value)
                    && bytes.len() >= 2
                {
                    match bytes[0] {
                        0 => state.last_version = format!("0x{:02X}", bytes[1]),
                        1 => state.last_run_mode = bytes[1].to_string(),
                        4 => state.last_remote_network_enable = bytes[1].to_string(),
                        5 => state.last_heartbeat_interval = bytes[1].to_string(),
                        6 => state.last_group_linkage = bytes[1].to_string(),
                        7 => state.last_switch_state = bytes[1].to_string(),
                        9 => state.last_linkage_mode = bytes[1].to_string(),
                        12 => state.last_microwave_setting = bytes[1].to_string(),
                        8 | 255 if bytes.len() >= 7 => {
                            state.last_mac_addr = bytes[1..7]
                                .iter()
                                .map(|byte| format!("{byte:02X}"))
                                .collect::<Vec<_>>()
                                .join(":");
                        }
                        _ => {}
                    }
                }
            }
            0x22 | 0x26 => {
                if let Some(value) = payload.get("value").and_then(Value::as_str)
                    && let Ok(bytes) = crate::protocol::hex_to_bytes(value)
                {
                    if !bytes.is_empty() {
                        state.last_scene_id = bytes[0].to_string();
                    }
                    if bytes.len() >= 5 {
                        state.last_run_mode = bytes[4].to_string();
                    }
                    if bytes.len() >= 6 {
                        state.last_switch_state = bytes[5].to_string();
                    }
                    state.last_scene_summary = format!(
                        "场景{} 模式{} 开关{}",
                        state.last_scene_id, state.last_run_mode, state.last_switch_state
                    );
                }
            }
            0x29 => {
                if let Some(power) = payload.get("power").and_then(Value::as_u64) {
                    state.last_power_w = power.to_string();
                }
                if let Some(energy) = payload.get("energy_consumption").and_then(Value::as_u64) {
                    state.last_energy_kwh = energy.to_string();
                }
            }
            0x51 => {
                if let Some(total) = payload.get("array_total_size").and_then(Value::as_u64) {
                    state.last_a_light_total = total.to_string();
                }
                if let Some(first) = payload
                    .get("value_array")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(Value::as_str)
                {
                    state.last_a_light_preview =
                        crate::protocol::decode_a_light_entry_public(first);
                }
            }
            0x32 => {
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    state.last_linkage_group_state = value.to_string();
                }
            }
            0x1F => {
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    let compact = value.chars().take(24).collect::<String>();
                    state.last_group_info = compact;
                    if let Ok(bytes) = crate::protocol::hex_to_bytes(value)
                        && !bytes.is_empty()
                        && bytes[0] == 0
                        && bytes.len() >= 7
                    {
                        let partition = u16::from_be_bytes([bytes[1], bytes[2]]);
                        let lane = u16::from_be_bytes([bytes[3], bytes[4]]);
                        let adjacent = u16::from_be_bytes([bytes[5], bytes[6]]);
                        state.last_partition_addr = if partition == 0 || partition == 0xFFFF {
                            String::new()
                        } else {
                            format!("0x{partition:04X}")
                        };
                        state.last_lane_group_addr = if lane == 0 || lane == 0xFFFF {
                            String::new()
                        } else {
                            format!("0x{lane:04X}")
                        };
                        state.last_adjacent_group_addr = if adjacent == 0 || adjacent == 0xFFFF {
                            String::new()
                        } else {
                            format!("0x{adjacent:04X}")
                        };
                    }
                }
            }
            0x10 => {
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    state.last_motion_event = match value {
                        "FF" => "检测到有人".into(),
                        "00" => "检测到离开".into(),
                        _ => format!("未知事件({value})"),
                    };
                }
            }
            _ => {}
        }
    }

    fn connect(&mut self) {
        self.config.broker = self.broker_editor.clone();
        self.config.transfer_packet_delay_ms = self.transfer_packet_delay_ms;
        self.config.transfer_ack_timeout_secs = self.transfer_ack_timeout_secs;
        self.config.bc_ota_start_ack_timeout_secs = self.bc_ota_start_ack_timeout_secs;
        self.config.transfer_max_retries = self.transfer_max_retries;
        self.config.voice_transfer_packet_delay_ms = self.voice_transfer_packet_delay_ms;
        self.config.voice_transfer_ack_timeout_secs = self.voice_transfer_ack_timeout_secs;
        self.config.voice_transfer_max_retries = self.voice_transfer_max_retries;
        self.connection_status = "连接中...".into();
        self.broker_connected = false;
        self.mqtt.connect(&self.config.broker);
        self.transfer_engine
            .send(TransferEngineCommand::SetPublisher(
                self.mqtt.client_handle(),
            ));
        self.sync_subscriptions();
        let _ = save_config(&self.config);
    }

    fn sync_subscriptions(&mut self) {
        let topics =
            subscription_topics_for_devices(&self.config.devices, self.auto_discovery_enabled);
        self.mqtt.sync_subscriptions(topics);
    }

    fn send_payload_to_device(
        &mut self,
        device: &DeviceProfile,
        payload: &Value,
        source: &str,
        expected_override: Option<u32>,
        append_log_entry: bool,
        append_operation_record: bool,
    ) -> Result<(), String> {
        self.mqtt
            .publish_json(&device.down_topic, &payload.to_string())?;

        let state = self.runtime_states.entry(device.local_id).or_default();
        state.tx_count += 1;
        let opcode = payload
            .get("opcode")
            .and_then(Value::as_u64)
            .map(|value| format!("0x{value:02X}"))
            .unwrap_or_else(|| "-".into());
        state.last_opcode = opcode.clone();
        Self::set_device_result(state, "发送", format!("已发送({source})"));
        if source != "transfer"
            && let Some(opcode_num) = payload.get("opcode").and_then(Value::as_u64)
        {
            let time_stamp = payload.get("time_stamp").and_then(Value::as_u64);
            let expected_opcode =
                expected_override.or_else(|| expected_response_opcode(opcode_num as u32));
            if let Some(expected_opcode) = expected_opcode {
                if let Some(time_stamp) = time_stamp {
                    self.pending_requests.push(PendingRequest {
                        device_local_id: device.local_id,
                        device_name: device.name.clone(),
                        device_id: device.device_id.clone(),
                        topic: device.down_topic.clone(),
                        opcode: opcode_num as u32,
                        expected_opcode,
                        sent_at: Instant::now(),
                        time_stamp: Some(time_stamp),
                    });
                    state.pending_count = state.pending_count.saturating_add(1);
                    Self::set_device_result(
                        state,
                        "待应答",
                        format!("等待应答(0x{:02X})", expected_opcode),
                    );
                } else {
                    Self::set_device_result(state, "发送", format!("已发送({source}, 未跟踪应答)"));
                }
            }
        }
        if append_log_entry || append_operation_record {
            let redacted = redact_json(payload);
            let summary = summarize_payload(&redacted);
            let status = source.to_uppercase();
            if append_log_entry {
                let logged_payload = if source == "transfer" {
                    compact_transfer_payload_log(&redacted)
                } else {
                    serde_json::to_string_pretty(&redacted).unwrap_or_else(|_| payload.to_string())
                };
                self.append_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::Tx,
                    device_name: device.name.clone(),
                    device_id: device.device_id.clone(),
                    topic: device.down_topic.clone(),
                    opcode: opcode.clone(),
                    status: status.clone(),
                    summary: summary.clone(),
                    payload: logged_payload,
                });
            }
            if append_operation_record {
                self.append_operation(OperationRecord {
                    timestamp: now_display(),
                    device_name: device.name.clone(),
                    opcode: opcode.clone(),
                    status,
                    detail: summary,
                    rtt_ms: String::new(),
                });
            }
        }
        Ok(())
    }

    fn selected_devices_owned(&self) -> Vec<DeviceProfile> {
        self.selected_device_refs().into_iter().cloned().collect()
    }

    fn ensure_selected_devices(&mut self) -> Option<Vec<DeviceProfile>> {
        let devices = self.selected_devices_owned();
        if devices.is_empty() {
            self.system_notice = "请先选择至少一个设备。".into();
            return None;
        }
        Some(devices)
    }

    fn should_confirm_preset_send(&self, command_key: &str, target_count: usize) -> bool {
        matches!(
            command_key,
            "set_network"
                | "set_mqtt_service"
                | "set_dns"
                | "network_management"
                | "reset_mesh_device"
        ) || target_count > 1
    }

    fn should_confirm_transfer(&self, target_count: usize, packet_count: usize) -> bool {
        target_count > 1 || packet_count > 10
    }

    fn should_confirm_raw_send(&self, opcode: u32, target_count: usize) -> bool {
        matches!(
            opcode,
            0x02 | 0x33 | 0x48 | 0x4A | 0x52 | 0x40 | 0x43 | 0x54 | 0x5C
        ) || target_count > 1
    }

    fn execute_preset_send(&mut self, items: Vec<(DeviceProfile, Value)>) {
        for (device, payload) in items {
            if let Err(err) =
                self.send_payload_to_device(&device, &payload, "preset", None, true, true)
            {
                self.system_notice = err;
                return;
            }
        }
    }

    fn execute_raw_send(&mut self, items: Vec<(DeviceProfile, Value, Option<u32>)>) {
        for (device, payload, expected_override) in items {
            if let Err(err) =
                self.send_payload_to_device(&device, &payload, "raw", expected_override, true, true)
            {
                self.system_notice = err;
                return;
            }
        }
    }

    fn execute_pending_confirmation(&mut self) {
        let Some(pending) = self.pending_confirmation.take() else {
            return;
        };
        match pending.action {
            PendingAction::PresetSend { items } => self.execute_preset_send(items),
            PendingAction::RawSend { items } => self.execute_raw_send(items),
            PendingAction::TransferQueue {
                devices,
                packets,
                preview,
                byte_size,
                kind,
            } => self.queue_transfer_action(devices, preview, packets, byte_size, kind),
        }
    }

    fn show_pending_confirmation_modal(&mut self, ctx: &egui::Context) {
        let Some((title, detail)) = self
            .pending_confirmation
            .as_ref()
            .map(|pending| (pending.title.clone(), pending.detail.clone()))
        else {
            return;
        };

        let response =
            egui::Modal::new(egui::Id::new("pending-confirmation-modal")).show(ctx, |ui| {
                ui.set_min_width(360.0);
                ui.vertical(|ui| {
                    ui.heading(title);
                    ui.add_space(8.0);
                    ui.label(detail);
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        if Self::primary_button(ui, "确认执行").clicked() {
                            self.execute_pending_confirmation();
                        }
                        if Self::secondary_button(ui, "取消").clicked() {
                            self.pending_confirmation = None;
                        }
                    });
                });
            });

        if response.should_close() {
            self.pending_confirmation = None;
        }
    }

    fn show_device_editor_modal(&mut self, ctx: &egui::Context) {
        if !self.device_editor_open {
            return;
        }

        let title = if self.device_editor.editing_id.is_some() {
            "编辑设备"
        } else {
            "新增设备"
        };
        let subtitle = match self.device_editor.name.trim() {
            "" => "未命名".to_string(),
            name => name.to_string(),
        };
        let response = egui::Modal::new(egui::Id::new("device-editor-modal")).show(ctx, |ui| {
            ui.set_min_width(420.0);
            ui.vertical(|ui| {
                ui.heading(title);
                ui.small(RichText::new(subtitle).color(ui.visuals().weak_text_color()));
                ui.add_space(8.0);
                self.render_device_editor_form(ui);
            });
        });

        if response.should_close() && self.device_editor_open {
            self.close_device_editor();
        }
    }

    fn normalize_raw_payload_for_device(
        &self,
        payload: Value,
        device: &DeviceProfile,
    ) -> Result<Value, String> {
        let mut object = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "原始负载必须是 JSON 对象".to_string())?;

        let opcode_value = object
            .get("opcode")
            .ok_or_else(|| "原始 JSON 必须包含 opcode 字段".to_string())?;
        let opcode_text = opcode_value.to_string().replace('"', "");
        let opcode = parse_opcode(&opcode_text)?;
        object.insert("opcode".into(), Value::from(opcode));

        if !object.contains_key("time_stamp") {
            object.insert("time_stamp".into(), Value::from(current_time_stamp()));
        }

        if opcode < 0x40 {
            object
                .entry("mesh_dev_type")
                .or_insert_with(|| Value::from(u64::from(device.mesh_dev_type)));
            object
                .entry("dest_addr")
                .or_insert_with(|| Value::from(u64::from(device.default_dest_addr)));
        }

        Ok(Value::Object(object))
    }

    fn resolve_pending_request(&mut self, device: &DeviceProfile, payload: &Value) {
        let Some(opcode) = payload
            .get("opcode")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
        else {
            return;
        };
        let response_time_stamp = payload.get("time_stamp").and_then(Value::as_u64);
        let position = match_pending_request_index(
            &self.pending_requests,
            device.local_id,
            opcode,
            response_time_stamp,
        );

        let Some(position) = position else {
            return;
        };
        let request = self.pending_requests.remove(position);
        let state = self.runtime_states.entry(device.local_id).or_default();
        state.pending_count = state.pending_count.saturating_sub(1);
        state.last_rtt_ms = format!("{}", request.sent_at.elapsed().as_millis());
        let rtt_ms = state.last_rtt_ms.clone();
        let (status, result_summary) = classify_execution_result(payload).unwrap_or_else(|| {
            let fallback = payload
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or("ACK")
                .to_string();
            ("ACK", fallback)
        });
        Self::set_device_result(
            state,
            status,
            format!(
                "{} 0x{:02X} -> 0x{:02X}",
                status, request.opcode, request.expected_opcode
            ),
        );
        self.append_log(LogEntry {
            timestamp: now_display(),
            direction: LogDirection::System,
            device_name: request.device_name.clone(),
            device_id: request.device_id.clone(),
            topic: request.topic.clone(),
            opcode: format!("0x{:02X}", request.expected_opcode),
            status: status.into(),
            summary: result_summary.clone(),
            payload: serde_json::to_string_pretty(&redact_json(payload))
                .unwrap_or_else(|_| payload.to_string()),
        });
        self.append_operation(OperationRecord {
            timestamp: now_display(),
            device_name: request.device_name,
            opcode: format!(
                "0x{:02X} -> 0x{:02X}",
                request.opcode, request.expected_opcode
            ),
            status: status.into(),
            detail: summarize_payload(&redact_json(payload)),
            rtt_ms,
        });
    }

    fn collect_pending_timeouts(&mut self) {
        let timeout = Duration::from_secs(10);
        let now = Instant::now();
        let mut expired = Vec::new();
        for (index, request) in self.pending_requests.iter().enumerate() {
            if now.duration_since(request.sent_at) >= timeout {
                expired.push(index);
            }
        }
        for index in expired.into_iter().rev() {
            let request = self.pending_requests.remove(index);
            if let Some(state) = self.runtime_states.get_mut(&request.device_local_id) {
                state.pending_count = state.pending_count.saturating_sub(1);
                Self::set_device_result(
                    state,
                    "超时",
                    format!(
                        "超时 0x{:02X} -> 0x{:02X}",
                        request.opcode, request.expected_opcode
                    ),
                );
            }
            self.append_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: request.device_name.clone(),
                device_id: request.device_id.clone(),
                topic: request.topic.clone(),
                opcode: format!("0x{:02X}", request.expected_opcode),
                status: "超时".into(),
                summary: "等待应答超时(10秒)".into(),
                payload: String::new(),
            });
            self.append_operation(OperationRecord {
                timestamp: now_display(),
                device_name: request.device_name,
                opcode: format!(
                    "0x{:02X} -> 0x{:02X}",
                    request.opcode, request.expected_opcode
                ),
                status: "超时".into(),
                detail: "等待应答超时(10秒)".into(),
                rtt_ms: String::new(),
            });
        }
    }

    fn configured_device_exists(&self, device_id: &str, up_topic: &str, down_topic: &str) -> bool {
        self.config.devices.iter().any(|device| {
            device.device_id == device_id
                || device.up_topic == up_topic
                || device.down_topic == down_topic
        })
    }

    fn observe_discovered_message(
        &mut self,
        topic: &str,
        payload: Option<&Value>,
        raw_payload: &str,
    ) {
        if !self.auto_discovery_enabled {
            return;
        }
        let Some((device_id, up_topic, down_topic)) =
            extract_device_topics_from_message_topic(topic)
        else {
            return;
        };
        let auto_import_device_id = device_id.clone();
        if self.configured_device_exists(&device_id, &up_topic, &down_topic) {
            self.discovered_devices
                .retain(|candidate| candidate.device_id != device_id);
            return;
        }

        let now = now_display();
        let summary = payload
            .map(summarize_payload)
            .unwrap_or_else(|| raw_payload.chars().take(96).collect());
        let opcode = payload
            .and_then(|value| value.get("opcode"))
            .and_then(Value::as_u64)
            .map(|value| format!("0x{value:02X}"))
            .unwrap_or_else(|| "-".into());
        let discovery_reason = payload
            .map(discovery_reason_from_payload)
            .unwrap_or_else(|| "上行消息".into());

        if let Some(candidate) = self
            .discovered_devices
            .iter_mut()
            .find(|candidate| candidate.device_id == device_id)
        {
            candidate.last_seen = now.clone();
            candidate.last_topic = topic.to_string();
            candidate.last_opcode = opcode;
            candidate.last_summary = summary;
            candidate.discovery_reason = discovery_reason;
            candidate.seen_count = candidate.seen_count.saturating_add(1);
            if let Some(payload) = payload {
                update_discovered_device_from_payload(candidate, payload);
            }
        } else {
            let mut candidate = DiscoveredDevice {
                suggested_name: suggested_discovered_name(&device_id, None),
                device_id,
                up_topic,
                down_topic,
                dev_model: String::new(),
                version: String::new(),
                mesh_dev_type: 1,
                default_dest_addr: None,
                last_opcode: opcode,
                last_summary: summary,
                discovery_reason,
                first_seen: now.clone(),
                last_seen: now,
                last_topic: topic.to_string(),
                seen_count: 1,
            };
            if let Some(payload) = payload {
                update_discovered_device_from_payload(&mut candidate, payload);
            }
            self.discovered_devices.push(candidate);
            self.discovered_devices
                .sort_by(|left, right| right.last_seen.cmp(&left.last_seen));
            if self.discovered_devices.len() > MAX_DISCOVERED_DEVICES {
                self.discovered_devices.truncate(MAX_DISCOVERED_DEVICES);
            }
        }

        if self.auto_import_discovered {
            self.import_discovered_device_by_id(&auto_import_device_id);
        }
    }

    fn import_discovered_device_by_id(&mut self, device_id: &str) {
        let Some(position) = self
            .discovered_devices
            .iter()
            .position(|candidate| candidate.device_id == device_id)
        else {
            return;
        };
        let candidate = self.discovered_devices.remove(position);
        if self.configured_device_exists(
            &candidate.device_id,
            &candidate.up_topic,
            &candidate.down_topic,
        ) {
            return;
        }

        let device = DeviceProfile {
            local_id: self.config.next_device_id,
            name: candidate.suggested_name.clone(),
            device_id: candidate.device_id.clone(),
            up_topic: candidate.up_topic.clone(),
            down_topic: candidate.down_topic.clone(),
            mesh_dev_type: candidate.mesh_dev_type,
            default_dest_addr: candidate.default_dest_addr.unwrap_or(1),
            subscribe_enabled: true,
        };
        self.config.next_device_id += 1;
        self.runtime_states.entry(device.local_id).or_default();
        self.config.devices.push(device);
        self.sync_subscriptions();
        let _ = save_config(&self.config);
        self.system_notice = format!("已从主动上报导入设备资源: {}", candidate.device_id);
    }

    fn load_discovered_device_into_editor(&mut self, device_id: &str) {
        let Some(candidate) = self
            .discovered_devices
            .iter()
            .find(|candidate| candidate.device_id == device_id)
        else {
            return;
        };
        self.device_editor = DeviceEditor {
            editing_id: None,
            name: candidate.suggested_name.clone(),
            device_id: candidate.device_id.clone(),
            up_topic: candidate.up_topic.clone(),
            down_topic: candidate.down_topic.clone(),
            mesh_dev_type: candidate.mesh_dev_type,
            default_dest_addr: candidate.default_dest_addr.unwrap_or(1),
            subscribe_enabled: true,
        };
        self.device_editor_open = true;
    }

    fn save_device_editor(&mut self) {
        if let Err(err) = self.validate_device_editor() {
            self.system_notice = err;
            return;
        }

        if let Some(editing_id) = self.device_editor.editing_id {
            if let Some(device) = self
                .config
                .devices
                .iter_mut()
                .find(|device| device.local_id == editing_id)
            {
                apply_editor(device, &self.device_editor);
            }
        } else {
            let device = DeviceProfile {
                local_id: self.config.next_device_id,
                name: self.device_editor.name.clone(),
                device_id: self.device_editor.device_id.clone(),
                up_topic: self.device_editor.up_topic.clone(),
                down_topic: self.device_editor.down_topic.clone(),
                mesh_dev_type: self.device_editor.mesh_dev_type,
                default_dest_addr: self.device_editor.default_dest_addr,
                subscribe_enabled: self.device_editor.subscribe_enabled,
            };
            self.runtime_states
                .insert(device.local_id, DeviceRuntimeState::default());
            self.config.next_device_id += 1;
            self.config.devices.push(device);
        }

        self.device_editor = Self::blank_device_editor();
        self.device_editor_open = false;
        self.sync_subscriptions();
        let _ = save_config(&self.config);
    }

    fn open_new_device_editor(&mut self) {
        self.device_editor = Self::blank_device_editor();
        self.device_editor_open = true;
    }

    fn open_device_editor(&mut self, device: &DeviceProfile) {
        self.device_editor = DeviceEditor::from_device(device);
        self.device_editor_open = true;
    }

    fn close_device_editor(&mut self) {
        self.device_editor = Self::blank_device_editor();
        self.device_editor_open = false;
    }

    fn delete_selected_devices(&mut self) {
        if self.selected_devices.is_empty() {
            return;
        }
        let selected_devices = self.selected_devices.clone();
        self.config
            .devices
            .retain(|device| !selected_devices.contains(&device.local_id));
        for local_id in &selected_devices {
            self.runtime_states.remove(local_id);
        }
        if self
            .device_editor
            .editing_id
            .is_some_and(|editing_id| selected_devices.contains(&editing_id))
        {
            self.close_device_editor();
        }
        self.selected_devices.clear();
        self.sync_subscriptions();
        let _ = save_config(&self.config);
    }

    fn delete_device(&mut self, local_id: u64) {
        let before_len = self.config.devices.len();
        self.config
            .devices
            .retain(|device| device.local_id != local_id);
        if self.config.devices.len() == before_len {
            return;
        }
        self.selected_devices.remove(&local_id);
        self.runtime_states.remove(&local_id);
        if self.device_editor.editing_id == Some(local_id) {
            self.close_device_editor();
        }
        self.sync_subscriptions();
        let _ = save_config(&self.config);
    }

    fn validate_device_editor(&self) -> Result<(), String> {
        if self.device_editor.device_id.trim().is_empty()
            || self.device_editor.up_topic.trim().is_empty()
            || self.device_editor.down_topic.trim().is_empty()
        {
            return Err("设备ID、上行主题、下行主题不能为空。".into());
        }
        if self.device_editor.up_topic.contains('#')
            || self.device_editor.up_topic.contains('+')
            || self.device_editor.down_topic.contains('#')
            || self.device_editor.down_topic.contains('+')
        {
            return Err("MQTT 主题不允许包含通配符 # 或 +。".into());
        }

        for device in &self.config.devices {
            if Some(device.local_id) == self.device_editor.editing_id {
                continue;
            }
            if device.device_id == self.device_editor.device_id {
                return Err("设备ID必须唯一。".into());
            }
            if device.up_topic == self.device_editor.up_topic {
                return Err("上行主题必须唯一。".into());
            }
            if device.down_topic == self.device_editor.down_topic {
                return Err("下行主题必须唯一。".into());
            }
        }
        Ok(())
    }

    fn selected_log_indices(&self) -> Vec<usize> {
        let filter_text = self.log_filter_text.trim().to_lowercase();
        self.logs
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                if !self.show_selected_logs_only || self.selected_devices.is_empty() {
                } else if !self.config.devices.iter().any(|device| {
                    self.selected_devices.contains(&device.local_id)
                        && device.device_id == entry.device_id
                }) {
                    return None;
                }
                if filter_text.is_empty() {
                    return Some(index);
                }
                let matches_filter = [
                    entry.device_name.as_str(),
                    entry.device_id.as_str(),
                    entry.topic.as_str(),
                    entry.opcode.as_str(),
                    entry.status.as_str(),
                    entry.summary.as_str(),
                ]
                .iter()
                .any(|field| field.to_lowercase().contains(&filter_text));
                matches_filter.then_some(index)
            })
            .collect()
    }

    fn select_log_row(&mut self, log_index: usize) {
        self.selected_log_index = Some(log_index);
        self.follow_latest_logs = false;
    }

    fn pick_ota_transfer_file(&mut self) {
        self.spawn_path_dialog(PendingFileDialogKind::OtaTransferFile, || {
            FileDialog::new().pick_file()
        });
    }

    fn pick_voice_transfer_file(&mut self) {
        self.spawn_path_dialog(PendingFileDialogKind::VoiceTransferFile, || {
            FileDialog::new().pick_file()
        });
    }

    fn start_transfer(
        &mut self,
        kind: TransferKind,
        file_path: String,
        version: u8,
        voice_name: String,
    ) {
        if file_path.trim().is_empty() {
            self.system_notice = "请先选择传输文件。".into();
            return;
        }
        let path = PathBuf::from(&file_path);
        let Ok(bytes) = fs::read(&path) else {
            self.system_notice = "读取传输文件失败。".into();
            return;
        };
        if bytes.is_empty() {
            self.system_notice = "传输文件不能为空。".into();
            return;
        }
        if bytes.len() > MAX_TRANSFER_BYTES {
            self.system_notice = format!(
                "传输文件过大：{} 字节，当前上限 {} 字节。",
                bytes.len(),
                MAX_TRANSFER_BYTES
            );
            return;
        }
        let Some(devices) = self.ensure_selected_devices() else {
            return;
        };
        let preview = transfer_preview(kind, &bytes, version, &voice_name);
        let packets = match build_transfer_packets(kind, &bytes, version, &voice_name) {
            Ok(packets) => packets,
            Err(err) => {
                self.system_notice = err;
                return;
            }
        };
        if self.should_confirm_transfer(devices.len(), packets.len()) {
            self.pending_confirmation = Some(PendingConfirmation {
                title: "确认发起传输".into(),
                detail: format!(
                    "类型：{}，目标设备：{} 台，文件大小：{} 字节，分包：{}",
                    kind.label(),
                    devices.len(),
                    bytes.len(),
                    transfer_display_total_packets(kind, packets.len())
                ),
                action: PendingAction::TransferQueue {
                    devices,
                    packets,
                    preview,
                    byte_size: bytes.len(),
                    kind,
                },
            });
            return;
        }
        self.queue_transfer_action(devices, preview, packets, bytes.len(), kind);
    }

    fn queue_transfer_action(
        &mut self,
        devices: Vec<DeviceProfile>,
        preview: Value,
        packets: Vec<Value>,
        byte_size: usize,
        kind: TransferKind,
    ) {
        self.append_log(LogEntry {
            timestamp: now_display(),
            direction: LogDirection::System,
            device_name: "系统".into(),
            device_id: "-".into(),
            topic: "transfer".into(),
            opcode: preview
                .get("opcode")
                .and_then(Value::as_u64)
                .map(|value| format!("0x{value:02X}"))
                .unwrap_or_else(|| "-".into()),
            status: "传输".into(),
            summary: format!(
                "{} | 文件={} | 分包={}",
                kind.label(),
                byte_size,
                transfer_display_total_packets(kind, packets.len())
            ),
            payload: serde_json::to_string_pretty(&redact_json(&preview))
                .unwrap_or_else(|_| preview.to_string()),
        });
        self.transfer_engine
            .send(TransferEngineCommand::QueueTransfer {
                devices,
                packets,
                kind,
                settings: self.transfer_engine_settings(kind),
            });
    }

    fn transfer_engine_settings(&self, kind: TransferKind) -> TransferEngineSettings {
        match kind {
            TransferKind::BcOta | TransferKind::AOta => TransferEngineSettings {
                packet_delay_ms: self.transfer_packet_delay_ms,
                ack_timeout_secs: self.transfer_ack_timeout_secs,
                bc_ota_start_ack_timeout_secs: self.bc_ota_start_ack_timeout_secs,
                max_retries: self.transfer_max_retries,
            },
            TransferKind::VoiceFile | TransferKind::RealtimeVoice => TransferEngineSettings {
                packet_delay_ms: self.voice_transfer_packet_delay_ms,
                ack_timeout_secs: self.voice_transfer_ack_timeout_secs,
                bc_ota_start_ack_timeout_secs: self.bc_ota_start_ack_timeout_secs,
                max_retries: self.voice_transfer_max_retries,
            },
        }
    }

    #[cfg(test)]
    fn tick_active_transfers(&mut self) {
        let now = Instant::now();
        let devices_by_id: HashMap<u64, DeviceProfile> = self
            .config
            .devices
            .iter()
            .cloned()
            .map(|device| (device.local_id, device))
            .collect();

        for index in 0..self.active_transfers.len() {
            if self.active_transfers[index].terminal {
                continue;
            }
            if self.active_transfers[index].paused {
                continue;
            }
            if self.active_transfers[index].waiting_ack_opcode.is_some() {
                continue;
            }
            if now < self.active_transfers[index].next_send_at {
                continue;
            }
            if self.active_transfers[index].next_index >= self.active_transfers[index].packets.len()
            {
                continue;
            }

            let device_local_id = self.active_transfers[index].device_local_id;
            let Some(device) = devices_by_id.get(&device_local_id) else {
                self.active_transfers[index].status = "设备已删除".into();
                self.active_transfers[index].next_index =
                    self.active_transfers[index].packets.len();
                self.active_transfers[index].waiting_ack_opcode = None;
                self.active_transfers[index].waiting_since = None;
                self.active_transfers[index].terminal = true;
                self.active_transfers[index].succeeded = false;
                self.active_transfers[index].paused = false;
                if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                    Self::set_device_result(state, "失败", "设备已删除".into());
                }
                continue;
            };
            let packet = self.active_transfers[index].packets
                [self.active_transfers[index].next_index]
                .clone();
            let packet_index = self.active_transfers[index].next_index;
            let packet_count = self.active_transfers[index].packets.len();
            let log_packet = should_log_transfer_packet(packet_index, packet_count);
            let opcode = packet.get("opcode").and_then(Value::as_u64).unwrap_or(0) as u32;
            match self.send_payload_to_device(device, &packet, "transfer", None, log_packet, false)
            {
                Ok(()) => {
                    self.active_transfers[index].last_sent_index = Some(packet_index);
                    self.active_transfers[index].last_sent_time_stamp =
                        packet.get("time_stamp").and_then(Value::as_u64);
                    self.active_transfers[index].next_index += 1;
                    let display_completed =
                        transfer_display_completed_packets(&self.active_transfers[index]);
                    let display_total = transfer_display_total_packets(
                        self.active_transfers[index].kind,
                        self.active_transfers[index].packets.len(),
                    );
                    self.active_transfers[index].status =
                        format!("发送中 {}/{}", display_completed, display_total);
                    if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                        Self::set_device_result(
                            state,
                            "传输",
                            format!(
                                "{} 发送中 {}/{}",
                                self.active_transfers[index].kind.label(),
                                display_completed,
                                display_total
                            ),
                        );
                    }
                    if let Some(expected_ack) = transfer_expected_ack_opcode(
                        self.active_transfers[index].kind,
                        opcode,
                        self.active_transfers[index].next_index,
                        self.active_transfers[index].packets.len(),
                    ) {
                        self.active_transfers[index].waiting_ack_opcode = Some(expected_ack);
                        self.active_transfers[index].waiting_since = Some(now);
                        self.active_transfers[index].status =
                            format!("等待ACK 0x{:02X}", expected_ack);
                        if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                            Self::set_device_result(
                                state,
                                "待应答",
                                format!("传输等待ACK 0x{:02X}", expected_ack),
                            );
                        }
                    } else {
                        self.active_transfers[index].next_send_at =
                            now + Duration::from_millis(self.transfer_packet_delay_ms);
                    }
                }
                Err(err) => {
                    self.retry_or_fail_transfer(index, packet_index, format!("发送失败: {err}"));
                    if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                        Self::set_device_result(state, "错误", format!("传输发送失败: {err}"));
                    }
                    self.system_notice = err;
                }
            }
        }

        let mut completed = Vec::new();
        for (index, transfer) in self.active_transfers.iter().enumerate() {
            if transfer.next_index >= transfer.packets.len()
                && transfer.waiting_ack_opcode.is_none()
                && !transfer.terminal
            {
                completed.push(index);
            }
        }
        for index in completed {
            let (
                device_local_id,
                device_name,
                device_id,
                down_topic,
                status,
                summary,
                succeeded,
                kind_label,
                transfer_status,
            ) = {
                let transfer = &mut self.active_transfers[index];
                transfer.terminal = true;
                transfer.succeeded = !(transfer.status.contains("拒绝")
                    || transfer.status.contains("超时")
                    || transfer.status.contains("失败"));
                let status = if transfer.succeeded {
                    "完成".to_string()
                } else {
                    "失败".to_string()
                };
                let summary = if transfer.succeeded {
                    format!("{} 传输完成", transfer.kind.label())
                } else {
                    format!("{} 传输失败: {}", transfer.kind.label(), transfer.status)
                };
                (
                    transfer.device_local_id,
                    transfer.device_name.clone(),
                    transfer.device_id.clone(),
                    transfer.down_topic.clone(),
                    status,
                    summary,
                    transfer.succeeded,
                    transfer.kind.label().to_string(),
                    transfer.status.clone(),
                )
            };
            self.append_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: device_name.clone(),
                device_id: device_id.clone(),
                topic: down_topic.clone(),
                opcode: "-".into(),
                status: status.clone(),
                summary: summary.clone(),
                payload: String::new(),
            });
            self.append_operation(OperationRecord {
                timestamp: now_display(),
                device_name: device_name.clone(),
                opcode: kind_label.clone(),
                status: status.clone(),
                detail: summary.clone(),
                rtt_ms: String::new(),
            });
            if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                Self::set_device_result(
                    state,
                    if succeeded { "成功" } else { "失败" },
                    if succeeded {
                        format!("{kind_label} 传输完成")
                    } else {
                        format!("{kind_label} 传输失败: {transfer_status}")
                    },
                );
            }
        }
    }

    #[cfg(test)]
    fn resolve_transfer_ack(&mut self, device: &DeviceProfile, payload: &Value) {
        let Some(opcode) = payload
            .get("opcode")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
        else {
            return;
        };
        let position = self.active_transfers.iter().position(|transfer| {
            let timestamp_matches = if transfer
                .waiting_ack_opcode
                .map(response_can_omit_timestamp)
                .unwrap_or(false)
            {
                true
            } else {
                transfer
                    .last_sent_time_stamp
                    .zip(payload.get("time_stamp").and_then(Value::as_u64))
                    .map(|(expected, actual)| expected == actual)
                    .unwrap_or(false)
            };
            transfer.device_local_id == device.local_id
                && transfer.waiting_ack_opcode == Some(opcode)
                && timestamp_matches
        });
        let Some(position) = position else {
            return;
        };
        if matches!(self.active_transfers[position].kind, TransferKind::BcOta) && opcode == 0x41 {
            let ack_value = payload.get("value").and_then(Value::as_u64).unwrap_or(0);
            let last_packet_sent = transfer_sent_last_packet(&self.active_transfers[position]);
            let retry_from = self.active_transfers[position]
                .last_sent_index
                .unwrap_or(self.active_transfers[position].next_index.saturating_sub(1));
            let now = Instant::now();

            match ack_value {
                2 => {
                    let reason = format!(
                        "BC OTA ACK失败，重发第{}包",
                        transfer_display_packet_number(TransferKind::BcOta, retry_from)
                    );
                    self.retry_or_fail_transfer(position, retry_from, reason);
                    let transfer = &self.active_transfers[position];
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(
                            state,
                            if transfer.next_index >= transfer.packets.len() {
                                "失败"
                            } else {
                                "重试"
                            },
                            transfer.status.clone(),
                        );
                    }
                    return;
                }
                1 | 3 if last_packet_sent => {
                    let transfer = &mut self.active_transfers[position];
                    transfer.paused = false;
                    transfer.waiting_since = Some(now);
                    transfer.status = "最后包已确认，等待升级结果".into();
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(state, "传输", "最后包已确认，等待升级结果".into());
                    }
                    return;
                }
                1 | 3 => {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA ACK成功，继续发送".into();
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(state, "传输", "BC OTA ACK成功，继续发送".into());
                    }
                    return;
                }
                5 if last_packet_sent => {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA升级成功".into();
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(state, "成功", "BC OTA升级成功".into());
                    }
                    return;
                }
                4 if last_packet_sent => {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA升级失败".into();
                    transfer.failure_packet_index = transfer.last_sent_index;
                    transfer.last_failure_reason = "BC OTA升级失败".into();
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(state, "失败", "BC OTA升级失败".into());
                    }
                    return;
                }
                _ => {
                    let reason = format!("BC OTA ACK失败，异常值 {}", ack_value);
                    self.retry_or_fail_transfer(position, retry_from, reason);
                    let transfer = &self.active_transfers[position];
                    if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                        Self::set_device_result(
                            state,
                            if transfer.next_index >= transfer.packets.len() {
                                "失败"
                            } else {
                                "重试"
                            },
                            transfer.status.clone(),
                        );
                    }
                    return;
                }
            }
        }
        let transfer = &mut self.active_transfers[position];
        if let Some((status, summary)) = classify_execution_result(payload)
            && status == "错误"
        {
            let device_name = transfer.device_name.clone();
            let device_id = transfer.device_id.clone();
            let down_topic = transfer.down_topic.clone();
            let op_device_name = device_name.clone();
            let op_summary = summary.clone();
            transfer.status = format!("ACK失败: {summary}");
            transfer.next_index = transfer.packets.len();
            transfer.waiting_ack_opcode = None;
            transfer.waiting_since = None;
            transfer.terminal = true;
            transfer.succeeded = false;
            transfer.paused = false;
            let payload_text = serde_json::to_string_pretty(&redact_json(payload))
                .unwrap_or_else(|_| payload.to_string());
            self.append_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name,
                device_id,
                topic: down_topic,
                opcode: format!("0x{:02X}", opcode),
                status: "错误".into(),
                summary,
                payload: payload_text,
            });
            self.append_operation(OperationRecord {
                timestamp: now_display(),
                device_name: op_device_name,
                opcode: format!("0x{:02X}", opcode),
                status: "错误".into(),
                detail: op_summary,
                rtt_ms: String::new(),
            });
            return;
        }
        if matches!(opcode, 0x41 | 0x44) {
            let consent = payload.get("value").and_then(Value::as_u64).unwrap_or(0);
            if consent != 1 {
                let device_name = transfer.device_name.clone();
                let device_id = transfer.device_id.clone();
                let down_topic = transfer.down_topic.clone();
                let op_device_name = device_name.clone();
                transfer.status = "设备拒绝继续传输".into();
                transfer.next_index = transfer.packets.len();
                transfer.waiting_ack_opcode = None;
                transfer.waiting_since = None;
                transfer.terminal = true;
                transfer.succeeded = false;
                let payload_text = serde_json::to_string_pretty(&redact_json(payload))
                    .unwrap_or_else(|_| payload.to_string());
                transfer.failure_packet_index = transfer.last_sent_index;
                transfer.last_failure_reason = "设备未同意继续传输".into();
                if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                    Self::set_device_result(state, "拒绝", "设备未同意继续传输".into());
                }
                self.append_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::System,
                    device_name,
                    device_id,
                    topic: down_topic,
                    opcode: format!("0x{:02X}", opcode),
                    status: "拒绝".into(),
                    summary: "设备未同意继续传输".into(),
                    payload: payload_text,
                });
                self.append_operation(OperationRecord {
                    timestamp: now_display(),
                    device_name: op_device_name,
                    opcode: format!("0x{:02X}", opcode),
                    status: "拒绝".into(),
                    detail: "设备未同意继续传输".into(),
                    rtt_ms: String::new(),
                });
                return;
            }
        }
        transfer.waiting_ack_opcode = None;
        transfer.waiting_since = None;
        transfer.paused = false;
        transfer.next_send_at =
            Instant::now() + Duration::from_millis(self.transfer_packet_delay_ms);
        if matches!(opcode, 0x41 | 0x44) {
            transfer.status = "已获同意，继续发送".into();
            if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                Self::set_device_result(state, "传输", "已获同意，继续发送".into());
            }
        } else {
            transfer.status = "收到ACK".into();
            if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                Self::set_device_result(state, "成功", "收到传输ACK".into());
            }
        }
    }

    #[cfg(test)]
    fn transfer_ack_timeout_for(&self, transfer: &ActiveTransfer) -> Duration {
        let timeout_secs = if transfer.kind == TransferKind::BcOta
            && transfer.waiting_ack_opcode == Some(0x41)
            && transfer.last_sent_index == Some(0)
        {
            self.bc_ota_start_ack_timeout_secs
        } else {
            self.transfer_ack_timeout_secs
        };
        Duration::from_secs(timeout_secs)
    }

    #[cfg(test)]
    fn collect_transfer_timeouts(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();
        for (index, transfer) in self.active_transfers.iter().enumerate() {
            if let (Some(expected_ack), Some(waiting_since)) =
                (transfer.waiting_ack_opcode, transfer.waiting_since)
            {
                let timeout = self.transfer_ack_timeout_for(transfer);
                if now.duration_since(waiting_since) >= timeout {
                    timed_out.push((index, expected_ack));
                }
            }
        }
        for (index, expected_ack) in timed_out {
            let device_local_id = self.active_transfers[index].device_local_id;
            let retry_from = self.active_transfers[index]
                .last_sent_index
                .unwrap_or(self.active_transfers[index].next_index.saturating_sub(1));
            self.retry_or_fail_transfer(
                index,
                retry_from,
                format!("ACK超时 0x{:02X}", expected_ack),
            );
            if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                Self::set_device_result(
                    state,
                    "超时",
                    format!("传输ACK超时 0x{:02X}", expected_ack),
                );
            }
        }
    }

    fn collect_device_offline_timeouts(&mut self) {
        let timeout = Duration::from_secs(DEVICE_OFFLINE_TIMEOUT_SECS);
        let now = Instant::now();
        for (device_id, last_seen_at) in &self.device_last_seen_at {
            if now.duration_since(*last_seen_at) >= timeout
                && let Some(state) = self.runtime_states.get_mut(device_id)
            {
                state.online = false;
            }
        }
    }

    fn cancel_transfers_for_devices(&mut self, device_ids: &[u64]) {
        self.transfer_engine
            .send(TransferEngineCommand::CancelDevices(device_ids.to_vec()));
        #[cfg(not(test))]
        {
            let _ = device_ids;
        }
        #[cfg(test)]
        {
            let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
            let mut cancelled = Vec::new();
            for transfer in &mut self.active_transfers {
                if device_ids.contains(&transfer.device_local_id) {
                    transfer.terminal = true;
                    transfer.succeeded = false;
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.status = "已取消".into();
                    transfer.paused = false;
                    cancelled.push((
                        transfer.device_name.clone(),
                        transfer.device_id.clone(),
                        transfer.down_topic.clone(),
                        transfer.kind.label().to_string(),
                    ));
                }
            }
            for (device_name, device_id, down_topic, kind_label) in cancelled {
                let device_id_for_lookup = device_id.clone();
                self.append_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::System,
                    device_name: device_name.clone(),
                    device_id: device_id.clone(),
                    topic: down_topic.clone(),
                    opcode: "-".into(),
                    status: "取消".into(),
                    summary: format!("{kind_label} 传输已取消"),
                    payload: String::new(),
                });
                self.append_operation(OperationRecord {
                    timestamp: now_display(),
                    device_name: device_name.clone(),
                    opcode: kind_label.clone(),
                    status: "取消".into(),
                    detail: "传输已取消".into(),
                    rtt_ms: String::new(),
                });
                if let Some(device) = self
                    .config
                    .devices
                    .iter()
                    .find(|device| device.device_id == device_id_for_lookup)
                    && let Some(state) = self.runtime_states.get_mut(&device.local_id)
                {
                    Self::set_device_result(state, "取消", format!("{kind_label} 传输已取消"));
                }
            }
        }
    }

    fn retry_failed_transfers_for_devices(&mut self, device_ids: &[u64]) {
        self.transfer_engine
            .send(TransferEngineCommand::RetryDevices(device_ids.to_vec()));
        #[cfg(test)]
        {
            let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
            for transfer in &mut self.active_transfers {
                if !device_ids.contains(&transfer.device_local_id) {
                    continue;
                }
                if transfer.terminal && !transfer.succeeded {
                    transfer.terminal = false;
                    transfer.status = "重新排队".into();
                    transfer.next_index = transfer.last_sent_index.unwrap_or(0);
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.next_send_at = Instant::now();
                    transfer.retry_count = 0;
                    transfer.paused = false;
                    if let Some(state) = self.runtime_states.get_mut(&transfer.device_local_id) {
                        Self::set_device_result(state, "重试", "失败传输重新排队".into());
                    }
                }
            }
        }
    }

    fn resume_transfers_for_devices(&mut self, device_ids: &[u64]) {
        self.transfer_engine
            .send(TransferEngineCommand::ResumeDevices(device_ids.to_vec()));
        #[cfg(test)]
        {
            let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
            for transfer in &mut self.active_transfers {
                if device_ids.contains(&transfer.device_local_id)
                    && transfer.paused
                    && !transfer.terminal
                {
                    transfer.paused = false;
                    transfer.status = "继续传输".into();
                    transfer.next_send_at = Instant::now();
                    if let Some(state) = self.runtime_states.get_mut(&transfer.device_local_id) {
                        Self::set_device_result(state, "继续", "继续传输".into());
                    }
                }
            }
        }
    }

    fn clear_terminal_transfers(&mut self) {
        self.transfer_engine
            .send(TransferEngineCommand::ClearTerminal);
        #[cfg(test)]
        self.active_transfers.retain(|transfer| !transfer.terminal);
    }

    fn clear_terminal_transfers_for_devices(&mut self, device_ids: &[u64]) {
        self.transfer_engine
            .send(TransferEngineCommand::ClearTerminalDevices(
                device_ids.to_vec(),
            ));
        #[cfg(test)]
        {
            let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
            self.active_transfers.retain(|transfer| {
                !(transfer.terminal && device_ids.contains(&transfer.device_local_id))
            });
        }
    }

    fn export_evidence(&mut self) {
        self.spawn_path_dialog(PendingFileDialogKind::EvidenceExport, || {
            FileDialog::new()
                .set_file_name("mesh-bc-test-evidence.json")
                .save_file()
        });
    }

    fn spawn_path_dialog<F>(&mut self, kind: PendingFileDialogKind, open_dialog: F)
    where
        F: FnOnce() -> Option<PathBuf> + Send + 'static,
    {
        if self.pending_file_dialog.is_some() {
            self.system_notice = "已有文件对话框正在等待结果。".into();
            return;
        }
        let (tx, rx) = mpsc::channel();
        self.pending_file_dialog = Some(PendingFileDialog { kind, rx });
        thread::spawn(move || {
            let _ = tx.send(open_dialog());
        });
    }

    fn poll_pending_file_dialog(&mut self) {
        let Some(pending) = self.pending_file_dialog.take() else {
            return;
        };
        match pending.rx.try_recv() {
            Ok(Some(path)) => match pending.kind {
                PendingFileDialogKind::OtaTransferFile => {
                    self.ota_transfer_file = path.display().to_string();
                }
                PendingFileDialogKind::VoiceTransferFile => {
                    self.voice_transfer_file = path.display().to_string();
                }
                PendingFileDialogKind::EvidenceExport => {
                    self.write_evidence_to_path(&path);
                }
            },
            Ok(None) => {}
            Err(TryRecvError::Empty) => {
                self.pending_file_dialog = Some(pending);
            }
            Err(TryRecvError::Disconnected) => {
                self.system_notice = "文件对话框已中断。".into();
            }
        }
    }

    fn write_evidence_to_path(&mut self, path: &Path) {
        let device_states = self
            .config
            .devices
            .iter()
            .map(|device| {
                let state = self
                    .runtime_states
                    .get(&device.local_id)
                    .cloned()
                    .unwrap_or_default();
                serde_json::json!({
                    "name": device.name,
                    "device_id": device.device_id,
                    "up_topic": device.up_topic,
                    "down_topic": device.down_topic,
                    "mesh_dev_type": device.mesh_dev_type,
                    "default_dest_addr": device.default_dest_addr,
                    "subscribe_enabled": device.subscribe_enabled,
                    "runtime": {
                        "online": state.online,
                        "last_seen": state.last_seen,
                        "tx_count": state.tx_count,
                        "rx_count": state.rx_count,
                        "pending_count": state.pending_count,
                        "last_opcode": state.last_opcode,
                        "last_result_label": state.last_result_label,
                        "last_result": state.last_result,
                        "last_rtt_ms": state.last_rtt_ms,
                        "last_summary": state.last_summary,
                        "last_version": state.last_version,
                        "last_device_model": state.last_device_model,
                        "last_mesh_addr": state.last_mesh_addr,
                        "last_switch_state": state.last_switch_state,
                        "last_run_mode": state.last_run_mode,
                        "last_remote_network_enable": state.last_remote_network_enable,
                        "last_heartbeat_interval": state.last_heartbeat_interval,
                        "last_group_linkage": state.last_group_linkage,
                        "last_linkage_mode": state.last_linkage_mode,
                        "last_microwave_setting": state.last_microwave_setting,
                        "last_linkage_group_state": state.last_linkage_group_state,
                        "last_scene_id": state.last_scene_id,
                        "last_energy_kwh": state.last_energy_kwh,
                        "last_power_w": state.last_power_w,
                        "last_a_light_total": state.last_a_light_total,
                        "last_a_light_preview": state.last_a_light_preview,
                        "last_group_info": state.last_group_info,
                        "last_scene_summary": state.last_scene_summary,
                        "last_motion_event": state.last_motion_event,
                        "last_mac_addr": state.last_mac_addr,
                        "last_partition_addr": state.last_partition_addr,
                        "last_lane_group_addr": state.last_lane_group_addr,
                        "last_adjacent_group_addr": state.last_adjacent_group_addr,
                    }
                })
            })
            .collect::<Vec<_>>();

        let pending = self
            .pending_requests
            .iter()
            .map(|request| {
                serde_json::json!({
                    "device_name": request.device_name,
                    "device_id": request.device_id,
                    "topic": request.topic,
                    "opcode": format!("0x{:02X}", request.opcode),
                    "expected_opcode": format!("0x{:02X}", request.expected_opcode),
                    "time_stamp": request.time_stamp,
                })
            })
            .collect::<Vec<_>>();

        let transfers = self
            .transfer_snapshots
            .iter()
            .map(|transfer| {
                serde_json::json!({
                    "device_name": transfer.device_name,
                    "device_id": transfer.device_id,
                    "kind": transfer.kind.label(),
                    "progress": transfer_snapshot_display_progress(transfer),
                    "waiting_ack_opcode": transfer.waiting_ack_opcode.map(|opcode| format!("0x{:02X}", opcode)),
                    "retry_count": transfer.retry_count,
                    "max_retries": transfer.max_retries,
                    "terminal": transfer.terminal,
                    "succeeded": transfer.succeeded,
                    "paused": transfer.paused,
                    "failure_packet_index": transfer.failure_packet_index,
                    "last_failure_reason": transfer.last_failure_reason,
                    "status": transfer.status,
                })
            })
            .collect::<Vec<_>>();

        let logs = self
            .logs
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "timestamp": entry.timestamp,
                    "direction": entry.direction.as_str(),
                    "device_name": entry.device_name,
                    "device_id": entry.device_id,
                    "topic": entry.topic,
                    "opcode": entry.opcode,
                    "status": entry.status,
                    "summary": entry.summary,
                    "payload": entry.payload,
                })
            })
            .collect::<Vec<_>>();

        let operations = self
            .recent_operations
            .iter()
            .map(|op| {
                serde_json::json!({
                    "timestamp": op.timestamp,
                    "device_name": op.device_name,
                    "opcode": op.opcode,
                    "status": op.status,
                    "detail": op.detail,
                    "rtt_ms": op.rtt_ms,
                })
            })
            .collect::<Vec<_>>();

        let discovered = self
            .discovered_devices
            .iter()
            .map(|device| {
                serde_json::json!({
                    "device_id": device.device_id,
                    "suggested_name": device.suggested_name,
                    "up_topic": device.up_topic,
                    "down_topic": device.down_topic,
                    "dev_model": device.dev_model,
                    "version": device.version,
                    "mesh_dev_type": device.mesh_dev_type,
                    "default_dest_addr": device.default_dest_addr,
                    "last_opcode": device.last_opcode,
                    "last_summary": device.last_summary,
                    "discovery_reason": device.discovery_reason,
                    "first_seen": device.first_seen,
                    "last_seen": device.last_seen,
                    "last_topic": device.last_topic,
                    "seen_count": device.seen_count,
                })
            })
            .collect::<Vec<_>>();

        let evidence = serde_json::json!({
            "generated_at": now_display(),
            "connection_status": self.connection_status,
            "auto_discovery": {
                "enabled": self.auto_discovery_enabled,
                "auto_import": self.auto_import_discovered,
            },
            "broker": {
                "name": self.broker_editor.name,
                "host": self.broker_editor.host,
                "port": self.broker_editor.port,
                "username": self.broker_editor.username,
                "client_id": self.broker_editor.client_id,
                "keepalive_secs": self.broker_editor.keepalive_secs,
                "use_tls": self.broker_editor.use_tls,
            },
                "transfer_settings": {
                    "ota": {
                    "packet_delay_ms": self.transfer_packet_delay_ms,
                    "ack_timeout_secs": self.transfer_ack_timeout_secs,
                    "bc_ota_start_ack_timeout_secs": self.bc_ota_start_ack_timeout_secs,
                    "max_retries": self.transfer_max_retries,
                    },
                    "voice": {
                        "packet_delay_ms": self.voice_transfer_packet_delay_ms,
                        "ack_timeout_secs": self.voice_transfer_ack_timeout_secs,
                        "max_retries": self.voice_transfer_max_retries,
                    },
                },
            "device_states": device_states,
            "pending_requests": pending,
            "active_transfers": transfers,
            "discovered_devices": discovered,
            "recent_operations": operations,
            "logs": logs,
        });

        match serde_json::to_string_pretty(&evidence) {
            Ok(text) => match fs::write(path, text) {
                Ok(()) => {
                    self.system_notice = format!("已导出测试证据: {}", path.display());
                }
                Err(err) => {
                    self.system_notice = format!("导出失败: {err}");
                }
            },
            Err(err) => {
                self.system_notice = format!("序列化导出内容失败: {err}");
            }
        }
    }

    #[cfg(test)]
    fn retry_or_fail_transfer(&mut self, index: usize, retry_packet_index: usize, reason: String) {
        let transfer = &mut self.active_transfers[index];
        apply_transfer_retry_state(
            transfer,
            retry_packet_index,
            reason,
            self.transfer_packet_delay_ms,
        );
    }

    #[cfg(test)]
    fn new_for_test() -> Self {
        Self {
            config: AppConfig::default(),
            broker_editor: BrokerProfile::default(),
            device_editor: Self::blank_device_editor(),
            device_editor_open: false,
            selected_devices: BTreeSet::new(),
            auto_discovery_enabled: true,
            auto_import_discovered: false,
            discovered_devices: Vec::new(),
            discovery_section_had_candidates: false,
            selected_log_index: None,
            command_key: "query_bc_info".into(),
            command_form: BTreeMap::new(),
            raw_json_text: "{}".into(),
            raw_expected_opcode: String::new(),
            connection_status: "未连接".into(),
            broker_connected: false,
            mqtt: MqttRuntime::default(),
            transfer_engine: TransferEngine::new(),
            runtime_states: HashMap::new(),
            logs: Vec::new(),
            recent_operations: Vec::new(),
            show_selected_logs_only: false,
            follow_latest_logs: true,
            log_filter_text: String::new(),
            ota_transfer_kind: TransferKind::BcOta,
            ota_transfer_file: String::new(),
            voice_transfer_kind: TransferKind::VoiceFile,
            voice_transfer_file: String::new(),
            transfer_version: 1,
            transfer_voice_name: "voice.adpcm".into(),
            transfer_packet_delay_ms: TRANSFER_PACKET_DELAY_MS,
            transfer_ack_timeout_secs: TRANSFER_ACK_TIMEOUT_SECS,
            bc_ota_start_ack_timeout_secs: BC_OTA_START_ACK_TIMEOUT_SECS,
            transfer_max_retries: TRANSFER_MAX_RETRIES,
            voice_transfer_packet_delay_ms: TRANSFER_PACKET_DELAY_MS,
            voice_transfer_ack_timeout_secs: TRANSFER_ACK_TIMEOUT_SECS,
            voice_transfer_max_retries: TRANSFER_MAX_RETRIES,
            system_notice: String::new(),
            needs_connection_recovery: false,
            pending_confirmation: None,
            pending_file_dialog: None,
            pending_requests: Vec::new(),
            transfer_snapshots: Vec::new(),
            #[cfg(test)]
            active_transfers: Vec::new(),
            device_last_seen_at: HashMap::new(),
        }
    }
}

#[cfg(test)]
fn apply_transfer_retry_state(
    transfer: &mut ActiveTransfer,
    retry_packet_index: usize,
    reason: String,
    packet_delay_ms: u64,
) {
    transfer.failure_packet_index = Some(retry_packet_index);
    transfer.last_failure_reason = reason.clone();
    if transfer.retry_count < transfer.max_retries {
        transfer.retry_count += 1;
        transfer.next_index = retry_packet_index;
        transfer.waiting_ack_opcode = None;
        transfer.waiting_since = None;
        transfer.paused = false;
        transfer.next_send_at = Instant::now()
            + Duration::from_millis(packet_delay_ms * u64::from(transfer.retry_count + 1));
        transfer.status = format!(
            "{}，从第{}包准备重试 {}/{}",
            reason,
            transfer_display_packet_number(transfer.kind, retry_packet_index),
            transfer.retry_count,
            transfer.max_retries
        );
    } else {
        transfer.status = reason;
        transfer.next_index = transfer.packets.len();
        transfer.waiting_ack_opcode = None;
        transfer.waiting_since = None;
        transfer.paused = false;
    }
}

fn match_pending_request_index(
    pending_requests: &[PendingRequest],
    device_local_id: u64,
    opcode: u32,
    response_time_stamp: Option<u64>,
) -> Option<usize> {
    let matching = pending_requests
        .iter()
        .enumerate()
        .filter(|(_, request)| {
            request.device_local_id == device_local_id && request.expected_opcode == opcode
        })
        .collect::<Vec<_>>();

    if let Some(response_time_stamp) = response_time_stamp {
        return matching.into_iter().find_map(|(index, request)| {
            (request.time_stamp == Some(response_time_stamp)).then_some(index)
        });
    }

    if response_can_omit_timestamp(opcode) && matching.len() == 1 {
        return Some(matching[0].0);
    }

    None
}

impl eframe::App for MeshBcTesterApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.process_mqtt_events();
        self.process_transfer_engine_events();
        self.poll_pending_file_dialog();
        let ctx = ui.ctx().clone();
        ctx.request_repaint_after(Duration::from_millis(100));
        ctx.set_theme(match self.config.ui_theme_mode {
            UiThemeMode::Dark => egui::Theme::Dark,
            UiThemeMode::Light => egui::Theme::Light,
        });
        Self::apply_visual_style(&ctx, self.config.ui_theme_mode);
        let panel_style = ctx.global_style();
        let panel_style = panel_style.as_ref();

        let top_panel = egui::Panel::top("top_bar").frame(Self::top_toolbar_frame(panel_style));
        let top_panel = if self.system_notice.is_empty() {
            top_panel.exact_size(TOP_TOOLBAR_HEIGHT)
        } else {
            top_panel.min_size(TOP_TOOLBAR_HEIGHT + TOP_TOOLBAR_NOTICE_HEIGHT)
        };
        top_panel.show_inside(ui, |ui| {
            self.render_top_toolbar(ui);
            if !self.system_notice.is_empty() {
                Self::status_chip(ui, "提示", &self.system_notice, ChipTone::Warning);
            }
        });

        egui::Panel::left("devices_panel")
            .resizable(true)
            .default_size(280.0)
            .min_size(220.0)
            .frame(Self::side_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                self.render_left_sidebar(ui);
            });

        egui::Panel::right("actions_panel")
            .resizable(true)
            .default_size(320.0)
            .min_size(220.0)
            .max_size(380.0)
            .frame(Self::side_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                self.render_right_actions_panel(ui);
            });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.render_central_workspace(ui);
        });

        self.show_device_editor_modal(&ctx);
        self.show_pending_confirmation_modal(&ctx);
    }

    fn on_exit(&mut self) {
        self.config.broker = self.broker_editor.clone();
        self.config.transfer_packet_delay_ms = self.transfer_packet_delay_ms;
        self.config.transfer_ack_timeout_secs = self.transfer_ack_timeout_secs;
        self.config.bc_ota_start_ack_timeout_secs = self.bc_ota_start_ack_timeout_secs;
        self.config.transfer_max_retries = self.transfer_max_retries;
        self.config.voice_transfer_packet_delay_ms = self.voice_transfer_packet_delay_ms;
        self.config.voice_transfer_ack_timeout_secs = self.voice_transfer_ack_timeout_secs;
        self.config.voice_transfer_max_retries = self.voice_transfer_max_retries;
        let _ = save_config(&self.config);
        self.mqtt.disconnect();
        self.transfer_engine
            .send(TransferEngineCommand::SetPublisher(None));
    }
}

fn compact_transfer_payload_log(payload: &Value) -> String {
    let mut redacted = redact_json(payload);
    if let Some(object) = redacted.as_object_mut()
        && let Some(value) = object.get_mut("value")
        && let Some(text) = value.as_str()
    {
        let abbreviated = if text.len() > 32 {
            format!("{}...(len={})", &text[..32], text.len())
        } else {
            text.to_string()
        };
        *value = Value::String(abbreviated);
    }
    serde_json::to_string_pretty(&redacted).unwrap_or_else(|_| payload.to_string())
}

#[cfg(test)]
fn transfer_expected_ack_opcode(
    kind: TransferKind,
    opcode: u32,
    next_index: usize,
    packet_count: usize,
) -> Option<u32> {
    match kind {
        TransferKind::BcOta => matches!(opcode, 0x40 | 0x42).then_some(0x41),
        TransferKind::AOta => (opcode == 0x43).then_some(0x44),
        TransferKind::VoiceFile => {
            if opcode == 0x54 {
                Some(0x55)
            } else if opcode == 0x56 {
                Some(0x57)
            } else if opcode == 0x58 && next_index == packet_count {
                Some(0x59)
            } else {
                None
            }
        }
        TransferKind::RealtimeVoice => {
            if opcode == 0x5C {
                Some(0x5D)
            } else if opcode == 0x60 && next_index == packet_count {
                Some(0x61)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
fn transfer_sent_last_packet(transfer: &ActiveTransfer) -> bool {
    transfer
        .last_sent_index
        .map(|index| index + 1 == transfer.packets.len())
        .unwrap_or(false)
}

#[cfg(test)]
fn transfer_resume_index_after_disconnect(transfer: &ActiveTransfer) -> usize {
    let packet_count = transfer.packets.len();
    let fallback = transfer.next_index.min(packet_count.saturating_sub(1));

    match transfer.kind {
        TransferKind::BcOta | TransferKind::AOta => transfer.last_sent_index.unwrap_or(fallback),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => {
            if transfer.waiting_ack_opcode.is_some() {
                transfer.last_sent_index.unwrap_or(fallback)
            } else {
                transfer.next_index.min(packet_count.saturating_sub(1))
            }
        }
    }
}

fn transfer_display_total_packets(kind: TransferKind, packet_count: usize) -> usize {
    match kind {
        TransferKind::BcOta | TransferKind::AOta => packet_count.saturating_sub(1),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => packet_count,
    }
}

fn transfer_display_completed_packets_for(
    kind: TransferKind,
    next_index: usize,
    packet_count: usize,
) -> usize {
    let total = transfer_display_total_packets(kind, packet_count);
    match kind {
        TransferKind::BcOta | TransferKind::AOta => next_index.saturating_sub(1).min(total),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => next_index.min(total),
    }
}

#[cfg(test)]
fn transfer_display_completed_packets(transfer: &ActiveTransfer) -> usize {
    transfer_display_completed_packets_for(
        transfer.kind,
        transfer.next_index,
        transfer.packets.len(),
    )
}

fn transfer_display_packet_number(kind: TransferKind, packet_index: usize) -> usize {
    match kind {
        TransferKind::BcOta | TransferKind::AOta => packet_index.max(1),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => packet_index + 1,
    }
}

#[cfg(test)]
fn transfer_display_progress(transfer: &ActiveTransfer) -> String {
    format!(
        "{}/{}",
        transfer_display_completed_packets(transfer),
        transfer_display_total_packets(transfer.kind, transfer.packets.len())
    )
}

fn transfer_snapshot_display_progress(transfer: &TransferSnapshot) -> String {
    format!(
        "{}/{}",
        transfer_display_completed_packets_for(
            transfer.kind,
            transfer.next_index,
            transfer.packet_count
        ),
        transfer_display_total_packets(transfer.kind, transfer.packet_count)
    )
}

#[cfg(test)]
fn should_log_transfer_packet(packet_index: usize, packet_count: usize) -> bool {
    packet_index == 0
        || packet_index + 1 >= packet_count
        || packet_index.is_multiple_of(TRANSFER_PACKET_LOG_INTERVAL)
}

#[inline]
fn rgb(r: u8, g: u8, b: u8) -> egui::Color32 {
    egui::Color32::from_rgb(r, g, b)
}

#[inline]
fn stroke(color: egui::Color32) -> egui::Stroke {
    egui::Stroke::new(1.0_f32, color)
}

#[inline]
fn chrome_stroke(dark_mode: bool) -> egui::Stroke {
    stroke(if dark_mode {
        rgb(30, 35, 37)
    } else {
        rgb(218, 224, 228)
    })
}

#[inline]
fn surface_stroke(dark_mode: bool) -> egui::Stroke {
    stroke(if dark_mode {
        rgb(34, 39, 41)
    } else {
        rgb(222, 228, 232)
    })
}

#[inline]
fn accent_color() -> egui::Color32 {
    rgb(42, 108, 255)
}

#[inline]
fn accent_text_color() -> egui::Color32 {
    accent_color()
}

#[derive(Clone, Copy)]
struct VisualPalette {
    panel_fill: egui::Color32,
    window_fill: egui::Color32,
    faint_bg_color: egui::Color32,
    extreme_bg_color: egui::Color32,
    code_bg_color: egui::Color32,
    window_stroke: egui::Color32,
    text_subdued: egui::Color32,
    text_default: egui::Color32,
    text_strong: egui::Color32,
    selection_bg_fill: egui::Color32,
    selection_stroke: egui::Color32,
    hyperlink_color: egui::Color32,
    inactive_bg_fill: egui::Color32,
    inactive_bg_stroke: egui::Color32,
    hovered_bg_fill: egui::Color32,
    hovered_bg_stroke: egui::Color32,
    active_bg_fill: egui::Color32,
    active_bg_stroke: egui::Color32,
    open_bg_fill: egui::Color32,
    open_bg_stroke: egui::Color32,
}

impl VisualPalette {
    fn dark() -> Self {
        Self {
            panel_fill: rgb(13, 16, 17),
            window_fill: rgb(5, 6, 7),
            faint_bg_color: rgb(20, 24, 25),
            extreme_bg_color: rgb(0, 0, 0),
            code_bg_color: rgb(28, 33, 35),
            window_stroke: rgb(38, 43, 46),
            text_subdued: rgb(125, 140, 146),
            text_default: rgb(202, 216, 222),
            text_strong: rgb(255, 255, 255),
            selection_bg_fill: rgb(0, 61, 161),
            selection_stroke: rgb(240, 242, 255),
            hyperlink_color: accent_text_color(),
            inactive_bg_fill: rgb(49, 56, 59),
            inactive_bg_stroke: rgb(38, 43, 46),
            hovered_bg_fill: rgb(55, 63, 66),
            hovered_bg_stroke: rgb(62, 70, 74),
            active_bg_fill: rgb(55, 63, 66),
            active_bg_stroke: accent_color(),
            open_bg_fill: rgb(43, 49, 52),
            open_bg_stroke: rgb(76, 86, 90),
        }
    }

    fn light() -> Self {
        Self {
            panel_fill: rgb(244, 246, 247),
            window_fill: rgb(255, 255, 255),
            faint_bg_color: rgb(238, 241, 243),
            extreme_bg_color: rgb(229, 233, 236),
            code_bg_color: rgb(248, 250, 251),
            window_stroke: rgb(205, 213, 219),
            text_subdued: rgb(91, 105, 113),
            text_default: rgb(38, 48, 54),
            text_strong: rgb(5, 10, 13),
            selection_bg_fill: rgb(0, 90, 230),
            selection_stroke: rgb(240, 242, 255),
            hyperlink_color: accent_color(),
            inactive_bg_fill: rgb(236, 240, 242),
            inactive_bg_stroke: rgb(205, 213, 219),
            hovered_bg_fill: rgb(226, 232, 236),
            hovered_bg_stroke: rgb(190, 200, 207),
            active_bg_fill: rgb(214, 228, 255),
            active_bg_stroke: accent_color(),
            open_bg_fill: rgb(226, 232, 236),
            open_bg_stroke: rgb(190, 200, 207),
        }
    }

    fn for_theme(theme: UiThemeMode) -> Self {
        match theme {
            UiThemeMode::Dark => Self::dark(),
            UiThemeMode::Light => Self::light(),
        }
    }
}

fn apply_visual_palette(visuals: &mut egui::Visuals, palette: VisualPalette) {
    visuals.panel_fill = palette.panel_fill;
    visuals.window_fill = palette.window_fill;
    visuals.faint_bg_color = palette.faint_bg_color;
    visuals.extreme_bg_color = palette.extreme_bg_color;
    visuals.code_bg_color = palette.code_bg_color;
    visuals.window_stroke = stroke(palette.window_stroke);
    visuals.selection.bg_fill = palette.selection_bg_fill;
    visuals.selection.stroke = stroke(palette.selection_stroke);
    visuals.hyperlink_color = palette.hyperlink_color;
    visuals.weak_text_color = Some(palette.text_subdued);
    visuals.widgets.inactive.weak_bg_fill = palette.inactive_bg_fill;
    visuals.widgets.inactive.bg_fill = palette.inactive_bg_fill;
    visuals.widgets.inactive.bg_stroke = stroke(palette.inactive_bg_stroke);
    visuals.widgets.hovered.weak_bg_fill = palette.hovered_bg_fill;
    visuals.widgets.hovered.bg_fill = palette.hovered_bg_fill;
    visuals.widgets.hovered.bg_stroke = stroke(palette.hovered_bg_stroke);
    visuals.widgets.active.weak_bg_fill = palette.active_bg_fill;
    visuals.widgets.active.bg_fill = palette.active_bg_fill;
    visuals.widgets.active.bg_stroke = stroke(palette.active_bg_stroke);
    visuals.widgets.open.weak_bg_fill = palette.open_bg_fill;
    visuals.widgets.open.bg_fill = palette.open_bg_fill;
    visuals.widgets.open.bg_stroke = stroke(palette.open_bg_stroke);
    visuals.widgets.noninteractive.weak_bg_fill = palette.faint_bg_color;
    visuals.widgets.noninteractive.bg_fill = palette.faint_bg_color;
    visuals.widgets.noninteractive.bg_stroke = stroke(palette.window_stroke);
    visuals.widgets.noninteractive.fg_stroke.color = palette.text_subdued;
    visuals.widgets.inactive.fg_stroke.color = palette.text_default;
    visuals.widgets.hovered.fg_stroke.color = palette.text_default;
    visuals.widgets.active.fg_stroke.color = palette.text_strong;
    visuals.widgets.open.fg_stroke.color = palette.text_default;
    visuals.text_edit_bg_color = Some(palette.code_bg_color);
}

impl MeshBcTesterApp {
    fn online_device_count(&self) -> usize {
        self.runtime_states
            .values()
            .filter(|state| state.online)
            .count()
    }

    fn render_top_toolbar(&mut self, ui: &mut egui::Ui) {
        ui.spacing_mut().item_spacing = egui::vec2(6.0, 2.0);
        ui.allocate_ui_with_layout(
            egui::vec2(ui.available_width(), TOP_TOOLBAR_HEIGHT),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                Self::status_dot(ui, self.broker_connected);
                ui.label(
                    RichText::new("Mesh BC")
                        .strong()
                        .color(ui.visuals().strong_text_color()),
                );
                ui.separator();
                ui.small(RichText::new("地址").color(ui.visuals().weak_text_color()));
                Self::compact_widget(
                    ui,
                    132.0,
                    TextEdit::singleline(&mut self.broker_editor.host).hint_text("192.168.1.10"),
                );
                ui.label(RichText::new(":").color(ui.visuals().weak_text_color()));
                Self::compact_widget(
                    ui,
                    58.0,
                    egui::DragValue::new(&mut self.broker_editor.port)
                        .range(1..=65535)
                        .speed(1),
                );
                ui.checkbox(&mut self.broker_editor.use_tls, "TLS");
                ui.menu_button("更多", |ui| {
                    ui.set_min_width(250.0);
                    Self::field_row(ui, "用户", |ui| {
                        Self::compact_text_edit(ui, 170.0, &mut self.broker_editor.username);
                    });
                    Self::field_row(ui, "密码", |ui| {
                        Self::compact_widget(
                            ui,
                            170.0,
                            TextEdit::singleline(&mut self.broker_editor.password)
                                .password(true)
                                .hint_text("密码"),
                        );
                    });
                    Self::field_row(ui, "客户端", |ui| {
                        Self::compact_text_edit(ui, 170.0, &mut self.broker_editor.client_id);
                    });
                    Self::field_row(ui, "Keepalive", |ui| {
                        Self::compact_widget(
                            ui,
                            76.0,
                            egui::DragValue::new(&mut self.broker_editor.keepalive_secs)
                                .range(5..=3600)
                                .speed(1),
                        );
                        ui.small(RichText::new("秒").color(ui.visuals().weak_text_color()));
                    });
                });
                ui.separator();
                if Self::primary_button(ui, "连接").clicked() {
                    self.connect();
                }
                if Self::secondary_button(ui, "断开").clicked() {
                    self.mqtt.disconnect();
                    self.transfer_engine
                        .send(TransferEngineCommand::SetPublisher(None));
                }
                if Self::secondary_button(ui, "导出").clicked() {
                    self.export_evidence();
                }
                ui.add_sized(
                    [TOP_STATUS_WIDTH, COMPACT_LABEL_HEIGHT],
                    egui::Label::new(
                        RichText::new(&self.connection_status)
                            .color(ui.visuals().weak_text_color()),
                    )
                    .truncate(),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let hover = Self::theme_toggle_button_hover(self.config.ui_theme_mode);
                    if Self::theme_icon_button(ui, self.config.ui_theme_mode)
                        .on_hover_text(hover)
                        .clicked()
                    {
                        self.config.ui_theme_mode = self.config.ui_theme_mode.toggle();
                        let _ = save_config(&self.config);
                    }
                });
            },
        );
    }

    fn render_right_actions_panel(&mut self, ui: &mut egui::Ui) {
        ui.spacing_mut().item_spacing = egui::vec2(4.0, 4.0);
        egui::ScrollArea::vertical()
            .id_salt("actions-panel-scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                ui.set_max_width(340.0);
                self.render_preset_command_section(ui);
                Self::section_gap(ui);
                self.render_raw_json_section(ui);
                Self::section_gap(ui);
                self.render_ota_transfer_section(ui);
                Self::section_gap(ui);
                self.render_voice_transfer_section(ui);
            });
    }

    fn render_preset_command_section(&mut self, ui: &mut egui::Ui) {
        let command_label = command_by_key(&self.command_key)
            .map(|spec| spec.label)
            .unwrap_or("未知命令");
        Self::tree_section(
            ui,
            "right_preset_commands",
            "预置命令",
            command_label,
            true,
            |ui| {
                Self::action_field_row(ui, "命令", |ui| {
                    egui::ComboBox::from_id_salt("right-command-combo")
                        .width(Self::action_value_width(ui, 190.0))
                        .selected_text(command_label)
                        .show_ui(ui, |ui| {
                            for spec in COMMANDS {
                                if ui
                                    .selectable_label(self.command_key == spec.key, spec.label)
                                    .clicked()
                                {
                                    self.command_key = spec.key.to_string();
                                    self.command_form = default_form_for_command(&self.command_key);
                                }
                            }
                        });
                });

                if let Some(spec) = command_by_key(&self.command_key) {
                    if spec.include_dest_addr {
                        Self::action_field_row(ui, "目标地址", |ui| {
                            let entry = self
                                .command_form
                                .entry("dest_addr".into())
                                .or_insert_with(|| "1".into());
                            Self::compact_text_edit(ui, Self::action_value_width(ui, 100.0), entry);
                        });
                    }
                    for field in spec.fields {
                        Self::action_field_row(ui, field.label, |ui| {
                            let value = self
                                .command_form
                                .entry(field.key.to_string())
                                .or_insert_with(|| field.default.to_string());
                            match field.kind {
                                FieldKind::Text | FieldKind::Integer => {
                                    Self::compact_text_edit(
                                        ui,
                                        Self::action_value_width(ui, 190.0),
                                        value,
                                    );
                                }
                                FieldKind::Choice(choices) => {
                                    egui::ComboBox::from_id_salt(field.key)
                                        .width(Self::action_value_width(ui, 190.0))
                                        .selected_text(value.clone())
                                        .show_ui(ui, |ui| {
                                            for choice in choices {
                                                ui.selectable_value(
                                                    value,
                                                    choice.value.to_string(),
                                                    choice.label,
                                                );
                                            }
                                        });
                                }
                            }
                        });
                    }
                    Self::action_button_row(ui, |ui| {
                        if Self::primary_button(ui, "发送预置命令").clicked() {
                            let Some(devices) = self.ensure_selected_devices() else {
                                return;
                            };
                            let mut items = Vec::new();
                            for device in devices {
                                match build_command_payload(spec, &device, &self.command_form) {
                                    Ok(payload) => items.push((device, payload)),
                                    Err(err) => self.system_notice = err,
                                }
                            }
                            if self.should_confirm_preset_send(spec.key, items.len()) {
                                self.pending_confirmation = Some(PendingConfirmation {
                                    title: "确认发送预置命令".into(),
                                    detail: format!(
                                        "命令：{}，目标设备：{} 台",
                                        spec.label,
                                        items.len()
                                    ),
                                    action: PendingAction::PresetSend { items },
                                });
                            } else {
                                self.execute_preset_send(items);
                            }
                        }
                    });
                }
            },
        );
    }

    fn render_raw_json_section(&mut self, ui: &mut egui::Ui) {
        let meta = if self.raw_expected_opcode.trim().is_empty() {
            "未指定 ACK".to_string()
        } else {
            format!("ACK {}", self.raw_expected_opcode.trim())
        };
        Self::tree_section(ui, "right_raw_json", "原始 JSON", &meta, false, |ui| {
            ui.add(
                TextEdit::multiline(&mut self.raw_json_text)
                    .font(egui::TextStyle::Monospace)
                    .desired_rows(6)
                    .desired_width(f32::INFINITY),
            );
            Self::action_field_row(ui, "期望应答", |ui| {
                Self::compact_widget(
                    ui,
                    Self::action_value_width(ui, 100.0),
                    TextEdit::singleline(&mut self.raw_expected_opcode).hint_text("如 0x47"),
                );
            });
            Self::action_button_row(ui, |ui| {
                if Self::primary_button(ui, "发送原始 JSON").clicked() {
                    self.submit_raw_json();
                }
            });
        });
    }

    fn submit_raw_json(&mut self) {
        match serde_json::from_str::<Value>(&self.raw_json_text) {
            Ok(payload @ Value::Object(_)) => {
                let Some(opcode) = payload.get("opcode") else {
                    self.system_notice = "原始 JSON 必须包含 opcode 字段".into();
                    return;
                };
                let opcode_text = opcode.to_string().replace('"', "");
                if parse_opcode(&opcode_text).is_err() {
                    self.system_notice = "opcode 必须是十进制整数或 0x 十六进制字符串".into();
                    return;
                }
                let expected_override = if self.raw_expected_opcode.trim().is_empty() {
                    None
                } else {
                    match parse_opcode(&self.raw_expected_opcode) {
                        Ok(opcode) => Some(opcode),
                        Err(err) => {
                            self.system_notice = format!("期望应答操作码无效: {err}");
                            return;
                        }
                    }
                };
                let Some(devices) = self.ensure_selected_devices() else {
                    return;
                };
                let opcode_num = parse_opcode(&opcode_text).unwrap_or(0);
                let mut items = Vec::new();
                for device in devices {
                    match self.normalize_raw_payload_for_device(payload.clone(), &device) {
                        Ok(normalized) => items.push((device, normalized, expected_override)),
                        Err(err) => {
                            self.system_notice = err;
                            return;
                        }
                    }
                }
                if self.should_confirm_raw_send(opcode_num, items.len()) {
                    self.pending_confirmation = Some(PendingConfirmation {
                        title: "确认发送原始 JSON".into(),
                        detail: format!(
                            "操作码：0x{:02X}，目标设备：{} 台",
                            opcode_num,
                            items.len()
                        ),
                        action: PendingAction::RawSend { items },
                    });
                } else {
                    self.execute_raw_send(items);
                }
            }
            Ok(_) => self.system_notice = "原始负载必须是 JSON 对象".into(),
            Err(err) => self.system_notice = err.to_string(),
        }
    }

    fn render_ota_transfer_section(&mut self, ui: &mut egui::Ui) {
        let meta = self.ota_transfer_kind.label();
        Self::tree_section(ui, "right_ota_transfer", "OTA升级", meta, true, |ui| {
            Self::action_field_row(ui, "升级类型", |ui| {
                egui::ComboBox::from_id_salt("right-ota-kind")
                    .width(Self::action_value_width(ui, 160.0))
                    .selected_text(self.ota_transfer_kind.label())
                    .show_ui(ui, |ui| {
                        for kind in [TransferKind::BcOta, TransferKind::AOta] {
                            ui.selectable_value(&mut self.ota_transfer_kind, kind, kind.label());
                        }
                    });
            });
            Self::action_field_row(ui, "固件文件", |ui| {
                let file_width = Self::action_file_width(ui);
                Self::compact_text_edit(ui, file_width, &mut self.ota_transfer_file);
                if Self::secondary_button(ui, "浏览").clicked() {
                    self.pick_ota_transfer_file();
                }
            });
            Self::action_field_row(ui, "参数", |ui| {
                Self::inline_dim_label(ui, "版本");
                Self::compact_widget(
                    ui,
                    52.0,
                    egui::DragValue::new(&mut self.transfer_version).range(0..=255),
                );
                Self::inline_dim_label(ui, "重试");
                Self::compact_widget(
                    ui,
                    48.0,
                    egui::DragValue::new(&mut self.transfer_max_retries).range(0..=10),
                );
            });
            Self::action_field_row(ui, "时序", |ui| {
                Self::inline_dim_label(ui, "间隔");
                Self::compact_widget(
                    ui,
                    56.0,
                    egui::DragValue::new(&mut self.transfer_packet_delay_ms).range(1..=5_000),
                );
                Self::inline_dim_label(ui, "ACK");
                Self::compact_widget(
                    ui,
                    56.0,
                    egui::DragValue::new(&mut self.transfer_ack_timeout_secs).range(1..=300),
                );
            });
            Self::action_field_row(ui, "BC起始", |ui| {
                Self::compact_widget(
                    ui,
                    56.0,
                    egui::DragValue::new(&mut self.bc_ota_start_ack_timeout_secs).range(1..=300),
                );
                Self::inline_dim_label(ui, "s");
            });
            Self::action_button_row(ui, |ui| {
                if Self::primary_button(ui, "发起 OTA 升级").clicked() {
                    self.start_transfer(
                        self.ota_transfer_kind,
                        self.ota_transfer_file.clone(),
                        self.transfer_version,
                        String::new(),
                    );
                }
            });
        });
    }

    fn render_voice_transfer_section(&mut self, ui: &mut egui::Ui) {
        let meta = self.voice_transfer_kind.label();
        Self::tree_section(
            ui,
            "right_voice_transfer",
            "声音传输",
            meta,
            false,
            |ui| {
                Self::action_field_row(ui, "传输类型", |ui| {
                    egui::ComboBox::from_id_salt("right-voice-kind")
                        .width(Self::action_value_width(ui, 160.0))
                        .selected_text(self.voice_transfer_kind.label())
                        .show_ui(ui, |ui| {
                            for kind in [TransferKind::VoiceFile, TransferKind::RealtimeVoice] {
                                ui.selectable_value(
                                    &mut self.voice_transfer_kind,
                                    kind,
                                    kind.label(),
                                );
                            }
                        });
                });
                Self::action_field_row(ui, "声音文件", |ui| {
                    let file_width = Self::action_file_width(ui);
                    Self::compact_text_edit(ui, file_width, &mut self.voice_transfer_file);
                    if Self::secondary_button(ui, "浏览").clicked() {
                        self.pick_voice_transfer_file();
                    }
                });
                Self::action_field_row(ui, "文件名", |ui| {
                    Self::compact_text_edit(
                        ui,
                        Self::action_value_width(ui, 170.0),
                        &mut self.transfer_voice_name,
                    );
                });
                Self::action_field_row(ui, "参数", |ui| {
                    Self::inline_dim_label(ui, "间隔");
                    Self::compact_widget(
                        ui,
                        44.0,
                        egui::DragValue::new(&mut self.voice_transfer_packet_delay_ms)
                            .range(1..=5_000),
                    );
                    Self::inline_dim_label(ui, "ACK");
                    Self::compact_widget(
                        ui,
                        44.0,
                        egui::DragValue::new(&mut self.voice_transfer_ack_timeout_secs)
                            .range(1..=300),
                    );
                    Self::inline_dim_label(ui, "重试");
                    Self::compact_widget(
                        ui,
                        38.0,
                        egui::DragValue::new(&mut self.voice_transfer_max_retries).range(0..=10),
                    );
                });
                Self::action_button_row(ui, |ui| {
                    if Self::primary_button(ui, "发起声音传输").clicked() {
                        self.start_transfer(
                            self.voice_transfer_kind,
                            self.voice_transfer_file.clone(),
                            self.transfer_version,
                            self.transfer_voice_name.clone(),
                        );
                    }
                });
            },
        );
    }

    fn render_left_sidebar(&mut self, ui: &mut egui::Ui) {
        ui.spacing_mut().item_spacing = egui::vec2(4.0, 4.0);
        self.render_device_tree_section(ui);
        self.render_discovery_tree_section(ui);
    }

    fn render_device_tree_section(&mut self, ui: &mut egui::Ui) {
        let summary = format!(
            "{} 台 · {} 在线 · {} 已选",
            self.config.devices.len(),
            self.online_device_count(),
            self.selected_devices.len()
        );
        let mut add_requested = false;
        let header = RichText::new(format!("设备  {summary}"))
            .size(12.0)
            .strong()
            .color(ui.visuals().strong_text_color());
        let section = egui::collapsing_header::CollapsingState::load_with_default_open(
            ui.ctx(),
            ui.make_persistent_id("devices_tree"),
            true,
        )
        .show_header(ui, |ui| {
            ui.allocate_ui_with_layout(
                egui::vec2(ui.available_width(), COMPACT_CONTROL_HEIGHT),
                egui::Layout::left_to_right(egui::Align::Center),
                |ui| {
                    ui.label(header);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if Self::add_icon_button(ui)
                            .on_hover_text("新增设备")
                            .clicked()
                        {
                            add_requested = true;
                        }
                    });
                },
            );
        });
        section.body(|ui| {
            ui.spacing_mut().item_spacing.y = 3.0;
            if !self.selected_devices.is_empty() && Self::secondary_button(ui, "删除已选").clicked()
            {
                self.delete_selected_devices();
            }
            if self.config.devices.is_empty() {
                ui.label(RichText::new("还没有配置设备。").color(ui.visuals().weak_text_color()));
                return;
            }

            let devices = self.config.devices.clone();
            egui::ScrollArea::vertical()
                .id_salt("configured-device-tree-scroll")
                .max_height(280.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    for device in devices {
                        let state = self
                            .runtime_states
                            .get(&device.local_id)
                            .cloned()
                            .unwrap_or_default();
                        self.render_device_tree_row(ui, &device, &state);
                    }
                });
        });
        if add_requested {
            self.open_new_device_editor();
        }
    }

    fn render_device_tree_row(
        &mut self,
        ui: &mut egui::Ui,
        device: &DeviceProfile,
        state: &DeviceRuntimeState,
    ) {
        let selected = self.selected_devices.contains(&device.local_id);
        let mut checked = selected;
        let mut edit_requested = false;
        let mut delete_requested = false;
        let response = Self::tree_row(
            ui,
            ("device", device.local_id),
            selected,
            34.0,
            0.0,
            DEVICE_ROW_CONTENT_CLICK_INSET,
            |ui, _row_hovered| {
                if ui.checkbox(&mut checked, "").changed() {
                    // Apply after drawing so row response stays presentation-only.
                }
                Self::status_dot(ui, state.online);
                ui.vertical(|ui| {
                    ui.set_width(ui.available_width().max(80.0));
                    ui.label(
                        RichText::new(&device.name)
                            .strong()
                            .color(Self::device_row_name_color(ui, selected)),
                    );
                    let rtt = if state.last_rtt_ms.is_empty() {
                        String::new()
                    } else {
                        format!(" · {}ms", state.last_rtt_ms)
                    };
                    let last = if state.last_result.is_empty() {
                        state.last_opcode.as_str()
                    } else {
                        state.last_result.as_str()
                    };
                    ui.small(
                        RichText::new(format!(
                            "{} · RX {} TX {} 待{}{} · {}",
                            Self::device_id_suffix(&device.device_id),
                            state.rx_count,
                            state.tx_count,
                            state.pending_count,
                            rtt,
                            Self::truncate_for_row(last, 18),
                        ))
                        .color(Self::device_row_meta_color(ui, selected)),
                    );
                });
            },
        );
        response.context_menu(|ui| {
            if ui.button("编辑").clicked() {
                edit_requested = true;
                ui.close();
            }
            if ui.button("删除").clicked() {
                delete_requested = true;
                ui.close();
            }
        });

        if checked != selected {
            if checked {
                self.selected_devices.insert(device.local_id);
            } else {
                self.selected_devices.remove(&device.local_id);
            }
        }
        if delete_requested {
            self.delete_device(device.local_id);
        } else if edit_requested || response.double_clicked() {
            self.open_device_editor(device);
        }
    }

    fn render_discovery_tree_section(&mut self, ui: &mut egui::Ui) {
        let summary = format!("{} 候选", self.discovered_devices.len());
        let has_discovered_devices = !self.discovered_devices.is_empty();
        let open_on_new_candidates =
            has_discovered_devices && !self.discovery_section_had_candidates;
        self.discovery_section_had_candidates = has_discovered_devices;
        Self::tree_section_with_open(
            ui,
            "discovery_tree",
            "发现",
            &summary,
            has_discovered_devices,
            open_on_new_candidates.then_some(true),
            |ui| {
                if ui
                    .checkbox(&mut self.auto_discovery_enabled, "从主动上报生成候选设备")
                    .changed()
                {
                    self.sync_subscriptions();
                }
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.auto_import_discovered, "自动导入");
                    if !self.discovered_devices.is_empty()
                        && Self::secondary_button(ui, "导入全部").clicked()
                    {
                        let ids = self
                            .discovered_devices
                            .iter()
                            .map(|candidate| candidate.device_id.clone())
                            .collect::<Vec<_>>();
                        for device_id in ids {
                            self.import_discovered_device_by_id(&device_id);
                        }
                    }
                    if !self.discovered_devices.is_empty()
                        && Self::secondary_button(ui, "清空").clicked()
                    {
                        self.discovered_devices.clear();
                    }
                });

                if self.discovered_devices.is_empty() {
                    ui.label(
                        RichText::new("当前没有发现设备。").color(ui.visuals().weak_text_color()),
                    );
                    return;
                }

                let candidates = self.discovered_devices.clone();
                let mut import_ids = Vec::new();
                let mut edit_ids = Vec::new();
                let mut remove_ids = Vec::new();
                egui::ScrollArea::vertical()
                    .max_height(220.0)
                    .id_salt("discovered-devices-tree-scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for candidate in candidates {
                            let (import, edit, remove) =
                                Self::render_discovered_tree_row(ui, &candidate);
                            if import {
                                import_ids.push(candidate.device_id.clone());
                            }
                            if edit {
                                edit_ids.push(candidate.device_id.clone());
                            }
                            if remove {
                                remove_ids.push(candidate.device_id.clone());
                            }
                        }
                    });

                for device_id in import_ids {
                    self.import_discovered_device_by_id(&device_id);
                }
                for device_id in edit_ids {
                    self.load_discovered_device_into_editor(&device_id);
                }
                if !remove_ids.is_empty() {
                    self.discovered_devices
                        .retain(|candidate| !remove_ids.contains(&candidate.device_id));
                }
            },
        );
    }

    fn render_discovered_tree_row(
        ui: &mut egui::Ui,
        candidate: &DiscoveredDevice,
    ) -> (bool, bool, bool) {
        let mut import_requested = false;
        let mut edit_requested = false;
        let mut remove_requested = false;
        Self::tree_row(
            ui,
            ("discovered", candidate.device_id.as_str()),
            false,
            38.0,
            DISCOVERED_ROW_ACTION_SLOT_WIDTH,
            DISCOVERED_ROW_CONTENT_CLICK_INSET,
            |ui, row_hovered| {
                Self::status_dot(ui, true);
                ui.vertical(|ui| {
                    ui.set_width(
                        (ui.available_width() - DISCOVERED_ROW_ACTION_SLOT_WIDTH).max(80.0),
                    );
                    ui.label(
                        RichText::new(&candidate.suggested_name)
                            .strong()
                            .color(ui.visuals().strong_text_color()),
                    );
                    let model = if candidate.dev_model.is_empty() {
                        candidate.discovery_reason.as_str()
                    } else {
                        candidate.dev_model.as_str()
                    };
                    ui.small(
                        RichText::new(format!(
                            "{} · {} · {} 次 · {}",
                            Self::device_id_suffix(&candidate.device_id),
                            model,
                            candidate.seen_count,
                            candidate.last_opcode
                        ))
                        .color(ui.visuals().weak_text_color()),
                    );
                });
                ui.allocate_ui_with_layout(
                    egui::vec2(DISCOVERED_ROW_ACTION_SLOT_WIDTH, COMPACT_CONTROL_HEIGHT),
                    egui::Layout::right_to_left(egui::Align::Center),
                    |ui| {
                        if row_hovered {
                            if Self::secondary_button(ui, "移除").clicked() {
                                remove_requested = true;
                            }
                            if Self::secondary_button(ui, "填入").clicked() {
                                edit_requested = true;
                            }
                            if Self::primary_button(ui, "导入").clicked() {
                                import_requested = true;
                            }
                        }
                    },
                );
            },
        );
        (import_requested, edit_requested, remove_requested)
    }

    fn render_device_editor_form(&mut self, ui: &mut egui::Ui) {
        Self::field_row(ui, "名称", |ui| {
            Self::compact_text_edit(ui, 300.0, &mut self.device_editor.name);
        });
        Self::field_row(ui, "设备ID", |ui| {
            let response = Self::compact_text_edit(ui, 300.0, &mut self.device_editor.device_id);
            if response.changed() && !self.device_editor.device_id.trim().is_empty() {
                let (up, down) = DeviceProfile::default_topics(self.device_editor.device_id.trim());
                if self.device_editor.up_topic.is_empty() {
                    self.device_editor.up_topic = up;
                }
                if self.device_editor.down_topic.is_empty() {
                    self.device_editor.down_topic = down;
                }
            }
        });
        Self::field_row(ui, "上行", |ui| {
            Self::compact_text_edit(ui, 300.0, &mut self.device_editor.up_topic);
        });
        Self::field_row(ui, "下行", |ui| {
            Self::compact_text_edit(ui, 300.0, &mut self.device_editor.down_topic);
        });
        Self::field_row(ui, "Mesh", |ui| {
            Self::compact_widget(
                ui,
                58.0,
                egui::DragValue::new(&mut self.device_editor.mesh_dev_type).range(0..=255),
            );
            ui.label("目标");
            Self::compact_widget(
                ui,
                70.0,
                egui::DragValue::new(&mut self.device_editor.default_dest_addr).range(1..=65535),
            );
        });
        Self::checkbox_chip(
            ui,
            &mut self.device_editor.subscribe_enabled,
            "订阅上行主题",
        );
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if Self::primary_button(ui, "保存").clicked() {
                self.save_device_editor();
            }
            if Self::secondary_button(ui, "取消").clicked() {
                self.close_device_editor();
            }
            ui.separator();
            if Self::secondary_button(ui, "清空").clicked() {
                self.open_new_device_editor();
            }
        });
    }

    fn tree_section(
        ui: &mut egui::Ui,
        id_salt: impl std::hash::Hash,
        title: &str,
        meta: &str,
        default_open: bool,
        add_body: impl FnOnce(&mut egui::Ui),
    ) {
        Self::tree_section_with_open(ui, id_salt, title, meta, default_open, None, add_body);
    }

    fn tree_section_with_open(
        ui: &mut egui::Ui,
        id_salt: impl std::hash::Hash,
        title: &str,
        meta: &str,
        default_open: bool,
        open: Option<bool>,
        add_body: impl FnOnce(&mut egui::Ui),
    ) {
        let header = RichText::new(format!("{title}  {meta}"))
            .size(12.0)
            .strong()
            .color(ui.visuals().strong_text_color());
        egui::CollapsingHeader::new(header)
            .id_salt(id_salt)
            .default_open(default_open)
            .open(open)
            .show(ui, |ui| {
                ui.spacing_mut().item_spacing.y = 3.0;
                add_body(ui);
            });
    }

    fn tree_row(
        ui: &mut egui::Ui,
        id_salt: impl std::hash::Hash,
        selected: bool,
        height: f32,
        action_slot_width: f32,
        content_click_inset: f32,
        add_content: impl FnOnce(&mut egui::Ui, bool),
    ) -> egui::Response {
        let desired = egui::vec2(ui.available_width(), height);
        let (rect, row_response) = ui.allocate_exact_size(desired, egui::Sense::hover());
        let inner_rect = rect.shrink2(egui::vec2(6.0, 0.0));
        let action_slot_width = action_slot_width.clamp(0.0, inner_rect.width());
        let non_action_rect = egui::Rect::from_min_max(
            inner_rect.min,
            egui::pos2(inner_rect.max.x - action_slot_width, inner_rect.max.y),
        );
        let content_click_inset = content_click_inset.clamp(0.0, non_action_rect.width());
        let content_click_rect = egui::Rect::from_min_max(
            egui::pos2(
                non_action_rect.min.x + content_click_inset,
                non_action_rect.min.y,
            ),
            non_action_rect.max,
        );
        let content_response = ui.interact(
            content_click_rect,
            ui.id().with(("tree-row-content", id_salt)),
            egui::Sense::click(),
        );
        let content_hovered = ui.rect_contains_pointer(non_action_rect);
        let row_hovered = row_response.hovered() || ui.rect_contains_pointer(rect);
        let fill = if selected {
            Self::selected_tree_row_fill(ui, content_hovered)
        } else if content_hovered {
            ui.visuals().widgets.hovered.weak_bg_fill
        } else {
            egui::Color32::TRANSPARENT
        };
        let row_rect = rect.shrink2(egui::vec2(1.0, 1.0));
        if fill != egui::Color32::TRANSPARENT {
            ui.painter().rect_filled(row_rect, 3.0, fill);
        }
        if selected {
            let rail_rect = egui::Rect::from_min_max(
                egui::pos2(row_rect.left() + 1.0, row_rect.top() + 5.0),
                egui::pos2(row_rect.left() + 3.0, row_rect.bottom() - 5.0),
            );
            ui.painter().rect_filled(rail_rect, 1.0, accent_color());
            ui.painter().rect_stroke(
                row_rect,
                3.0,
                Self::selected_tree_row_stroke(ui),
                egui::StrokeKind::Inside,
            );
        } else if content_hovered {
            ui.painter().rect_stroke(
                row_rect,
                3.0,
                surface_stroke(ui.visuals().dark_mode),
                egui::StrokeKind::Inside,
            );
        }
        ui.scope_builder(
            egui::UiBuilder::new()
                .max_rect(inner_rect)
                .layout(egui::Layout::left_to_right(egui::Align::Center)),
            |ui| {
                ui.spacing_mut().item_spacing = egui::vec2(5.0, 0.0);
                add_content(ui, row_hovered);
            },
        );
        content_response
    }

    fn selected_tree_row_fill(ui: &egui::Ui, hovered: bool) -> egui::Color32 {
        match (ui.visuals().dark_mode, hovered) {
            (true, true) => rgb(24, 34, 45),
            (true, false) => rgb(18, 27, 37),
            (false, true) => rgb(229, 240, 255),
            (false, false) => rgb(239, 246, 255),
        }
    }

    fn selected_tree_row_stroke(ui: &egui::Ui) -> egui::Stroke {
        stroke(if ui.visuals().dark_mode {
            rgb(49, 68, 91)
        } else {
            rgb(190, 211, 243)
        })
    }

    fn device_row_name_color(ui: &egui::Ui, selected: bool) -> egui::Color32 {
        if !selected {
            return ui.visuals().strong_text_color();
        }
        if ui.visuals().dark_mode {
            rgb(234, 246, 255)
        } else {
            rgb(12, 64, 150)
        }
    }

    fn device_row_meta_color(ui: &egui::Ui, selected: bool) -> egui::Color32 {
        if !selected {
            return ui.visuals().weak_text_color();
        }
        if ui.visuals().dark_mode {
            rgb(164, 188, 204)
        } else {
            rgb(57, 91, 145)
        }
    }

    fn field_row(ui: &mut egui::Ui, label: &str, add_field: impl FnOnce(&mut egui::Ui)) {
        ui.allocate_ui_with_layout(
            egui::vec2(ui.available_width(), COMPACT_CONTROL_HEIGHT),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                ui.add_sized(
                    [MENU_FIELD_LABEL_WIDTH, COMPACT_LABEL_HEIGHT],
                    egui::Label::new(RichText::new(label).color(ui.visuals().weak_text_color())),
                );
                add_field(ui);
            },
        );
    }

    fn action_field_row(ui: &mut egui::Ui, label: &str, add_field: impl FnOnce(&mut egui::Ui)) {
        ui.horizontal_top(|ui| {
            ui.add_sized(
                [ACTION_FIELD_LABEL_WIDTH, COMPACT_LABEL_HEIGHT],
                egui::Label::new(RichText::new(label).color(ui.visuals().weak_text_color())),
            );
            ui.vertical(|ui| {
                ui.spacing_mut().item_spacing = egui::vec2(4.0, 3.0);
                ui.horizontal_wrapped(|ui| {
                    ui.set_min_height(COMPACT_CONTROL_HEIGHT);
                    add_field(ui);
                });
            });
        });
    }

    fn action_value_width(ui: &egui::Ui, max_width: f32) -> f32 {
        ui.available_width().clamp(86.0, max_width)
    }

    fn action_file_width(ui: &egui::Ui) -> f32 {
        (ui.available_width() - 48.0).clamp(82.0, 170.0)
    }

    fn action_button_row(ui: &mut egui::Ui, add_button: impl FnOnce(&mut egui::Ui)) {
        ui.allocate_ui_with_layout(
            egui::vec2(ui.available_width(), COMPACT_CONTROL_HEIGHT),
            egui::Layout::left_to_right(egui::Align::Center),
            |ui| {
                ui.add_space(ACTION_BUTTON_INDENT_WIDTH);
                add_button(ui);
            },
        );
    }

    fn section_gap(ui: &mut egui::Ui) {
        ui.add_space(4.0);
    }

    fn inline_dim_label(ui: &mut egui::Ui, text: &str) {
        ui.small(RichText::new(text).color(ui.visuals().weak_text_color()));
    }

    fn status_dot(ui: &mut egui::Ui, active: bool) {
        let color = if active {
            if ui.visuals().dark_mode {
                rgb(68, 214, 132)
            } else {
                rgb(0, 137, 77)
            }
        } else {
            ui.visuals().weak_text_color()
        };
        let (rect, _) = ui.allocate_exact_size(
            egui::vec2(9.0, COMPACT_CONTROL_HEIGHT),
            egui::Sense::hover(),
        );
        ui.painter().circle_filled(rect.center(), 3.5, color);
    }

    fn device_id_suffix(device_id: &str) -> String {
        let suffix_len = device_id.chars().count().min(8);
        let suffix = device_id
            .chars()
            .rev()
            .take(suffix_len)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>();
        if suffix.is_empty() {
            "-".into()
        } else {
            suffix
        }
    }

    fn render_central_workspace(&mut self, ui: &mut egui::Ui) {
        Self::paint_tech_background(ui);
        self.render_workspace_header(ui);
        self.render_activity_overview(ui);

        let filtered_log_indices = self.selected_log_indices();
        self.render_log_workspace(ui, &filtered_log_indices);
    }

    fn render_workspace_header(&mut self, ui: &mut egui::Ui) {
        let total_rx: u64 = self
            .runtime_states
            .values()
            .map(|state| state.rx_count)
            .sum();
        let total_tx: u64 = self
            .runtime_states
            .values()
            .map(|state| state.tx_count)
            .sum();

        ui.horizontal_wrapped(|ui| {
            Self::section_heading(ui, "实时工作区");
            ui.separator();
            Self::stat_chip(ui, "设备", self.config.devices.len().to_string());
            Self::stat_chip(ui, "已选", self.selected_devices.len().to_string());
            Self::stat_chip(ui, "RX", total_rx.to_string());
            Self::stat_chip(ui, "TX", total_tx.to_string());
            Self::stat_chip(ui, "待应答", self.pending_requests.len().to_string());
            Self::stat_chip(ui, "传输", self.transfer_snapshots.len().to_string());
            Self::stat_chip(ui, "日志", self.logs.len().to_string());
            Self::checkbox_chip(ui, &mut self.show_selected_logs_only, "仅已选设备日志");
        });
        ui.add_space(2.0);
    }

    fn render_activity_overview(&mut self, ui: &mut egui::Ui) {
        let has_pending_requests = !self.pending_requests.is_empty();
        let has_transfers = !self.transfer_snapshots.is_empty();
        let has_recent_operations = !self.recent_operations.is_empty();
        if !has_pending_requests && !has_transfers && !has_recent_operations {
            return;
        }

        Self::panel_card_frame(ui).show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                Self::section_heading(ui, "活动状态");
                if has_pending_requests {
                    Self::stat_chip(ui, "请求", self.pending_requests.len().to_string());
                }
                if has_transfers {
                    Self::stat_chip(ui, "传输", self.transfer_snapshots.len().to_string());
                }
                if has_recent_operations {
                    Self::stat_chip(ui, "最近", self.recent_operations.len().to_string());
                }
            });

            if has_pending_requests {
                ui.add_space(2.0);
                self.render_request_status_table(ui);
            }
            if has_transfers {
                ui.separator();
                self.render_transfer_status_section(ui);
            }
            if has_recent_operations {
                ui.separator();
                self.render_recent_operations_table(ui);
            }
        });
    }

    fn render_request_status_table(&self, ui: &mut egui::Ui) {
        Self::section_heading(ui, "请求状态");
        let now = Instant::now();
        let table_height =
            Self::table_scroll_height(self.pending_requests.len(), REQUEST_STATUS_TABLE_HEIGHT);
        TableBuilder::new(ui)
            .id_salt("request-status-table")
            .striped(true)
            .column(Column::initial(120.0))
            .column(Column::initial(90.0))
            .column(Column::initial(90.0))
            .column(Column::initial(90.0))
            .column(Column::remainder().clip(true))
            .min_scrolled_height(table_height)
            .max_scroll_height(table_height)
            .header(TABLE_ROW_HEIGHT, |mut header| {
                header.col(|ui| {
                    Self::table_header_label(ui, "设备");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "请求");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "等待");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "时间戳");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "主题");
                });
            })
            .body(|body| {
                body.rows(TABLE_ROW_HEIGHT, self.pending_requests.len(), |mut row| {
                    let request = &self.pending_requests[row.index()];
                    row.col(|ui| {
                        ui.label(&request.device_name);
                    });
                    row.col(|ui| {
                        ui.label(format!(
                            "0x{:02X} -> 0x{:02X}",
                            request.opcode, request.expected_opcode
                        ));
                    });
                    row.col(|ui| {
                        ui.label(format!(
                            "{}ms",
                            now.duration_since(request.sent_at).as_millis()
                        ));
                    });
                    row.col(|ui| {
                        ui.label(
                            request
                                .time_stamp
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "-".into()),
                        );
                    });
                    row.col(|ui| {
                        ui.label(&request.topic);
                    });
                });
            });
    }

    fn render_transfer_status_section(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            Self::section_heading(ui, "传输状态");
            if self
                .transfer_snapshots
                .iter()
                .any(|transfer| transfer.paused && !transfer.terminal)
                && Self::secondary_button(ui, "继续待传输").clicked()
            {
                let ids = self
                    .transfer_snapshots
                    .iter()
                    .filter(|transfer| transfer.paused && !transfer.terminal)
                    .map(|transfer| transfer.device_local_id)
                    .collect::<Vec<_>>();
                self.resume_transfers_for_devices(&ids);
            }
            if self
                .transfer_snapshots
                .iter()
                .any(|transfer| transfer.terminal && !transfer.succeeded)
                && Self::secondary_button(ui, "重试失败").clicked()
            {
                let ids = self
                    .transfer_snapshots
                    .iter()
                    .filter(|transfer| transfer.terminal && !transfer.succeeded)
                    .map(|transfer| transfer.device_local_id)
                    .collect::<Vec<_>>();
                self.retry_failed_transfers_for_devices(&ids);
            }
            if self
                .transfer_snapshots
                .iter()
                .any(|transfer| transfer.terminal)
                && Self::secondary_button(ui, "清除终态").clicked()
            {
                self.clear_terminal_transfers();
            }
            if !self.transfer_snapshots.is_empty()
                && Self::secondary_button(ui, "取消全部").clicked()
            {
                let ids = self
                    .transfer_snapshots
                    .iter()
                    .map(|transfer| transfer.device_local_id)
                    .collect::<Vec<_>>();
                self.cancel_transfers_for_devices(&ids);
            }
        });

        let mut cancel_ids = Vec::new();
        let mut resume_ids = Vec::new();
        let mut retry_ids = Vec::new();
        let mut clear_ids = Vec::new();
        let table_height =
            Self::table_scroll_height(self.transfer_snapshots.len(), TRANSFER_STATUS_TABLE_HEIGHT);

        TableBuilder::new(ui)
            .id_salt("transfer-status-table")
            .striped(true)
            .column(Column::initial(120.0))
            .column(Column::initial(76.0))
            .column(Column::initial(86.0))
            .column(Column::initial(72.0))
            .column(Column::initial(58.0))
            .column(Column::initial(70.0))
            .column(Column::remainder().clip(true))
            .min_scrolled_height(table_height)
            .max_scroll_height(table_height)
            .header(TABLE_ROW_HEIGHT, |mut header| {
                header.col(|ui| {
                    Self::table_header_label(ui, "设备");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "类型");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "进度");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "等待ACK");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "重试");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "操作");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "状态 / 失败");
                });
            })
            .body(|body| {
                body.rows(
                    TABLE_ROW_HEIGHT,
                    self.transfer_snapshots.len(),
                    |mut row| {
                        let transfer = &self.transfer_snapshots[row.index()];
                        row.col(|ui| {
                            ui.label(&transfer.device_name);
                        });
                        row.col(|ui| {
                            ui.label(transfer.kind.label());
                        });
                        row.col(|ui| {
                            ui.label(transfer_snapshot_display_progress(transfer));
                        });
                        row.col(|ui| {
                            ui.label(
                                transfer
                                    .waiting_ack_opcode
                                    .map(|opcode| format!("0x{:02X}", opcode))
                                    .unwrap_or_else(|| "-".into()),
                            );
                        });
                        row.col(|ui| {
                            ui.label(format!("{}/{}", transfer.retry_count, transfer.max_retries));
                        });
                        row.col(|ui| {
                            if transfer.paused && !transfer.terminal {
                                if Self::secondary_button(ui, "继续").clicked() {
                                    resume_ids.push(transfer.device_local_id);
                                }
                            } else if transfer.terminal && !transfer.succeeded {
                                if Self::secondary_button(ui, "重试").clicked() {
                                    retry_ids.push(transfer.device_local_id);
                                }
                            } else if transfer.terminal {
                                if Self::secondary_button(ui, "清理").clicked() {
                                    clear_ids.push(transfer.device_local_id);
                                }
                            } else if Self::secondary_button(ui, "取消").clicked() {
                                cancel_ids.push(transfer.device_local_id);
                            }
                        });
                        row.col(|ui| {
                            let status_detail = Self::transfer_status_detail(transfer);
                            ui.label(status_detail);
                        });
                    },
                );
            });

        if !cancel_ids.is_empty() {
            self.cancel_transfers_for_devices(&cancel_ids);
        }
        if !resume_ids.is_empty() {
            self.resume_transfers_for_devices(&resume_ids);
        }
        if !retry_ids.is_empty() {
            self.retry_failed_transfers_for_devices(&retry_ids);
        }
        if !clear_ids.is_empty() {
            self.clear_terminal_transfers_for_devices(&clear_ids);
        }
    }

    fn transfer_status_detail(transfer: &TransferSnapshot) -> String {
        match (
            transfer.failure_packet_index,
            transfer.last_failure_reason.is_empty(),
        ) {
            (Some(index), false) => format!(
                "{} · #{} · {}",
                transfer.status,
                transfer_display_packet_number(transfer.kind, index),
                transfer.last_failure_reason
            ),
            (Some(index), true) => format!(
                "{} · #{}",
                transfer.status,
                transfer_display_packet_number(transfer.kind, index)
            ),
            (None, false) => format!("{} · {}", transfer.status, transfer.last_failure_reason),
            (None, true) => transfer.status.clone(),
        }
    }

    fn render_recent_operations_table(&mut self, ui: &mut egui::Ui) {
        let mut clear_recent = false;
        ui.horizontal(|ui| {
            Self::section_heading(ui, "最近操作");
            if Self::secondary_button(ui, "清空操作").clicked() {
                clear_recent = true;
            }
        });
        if clear_recent {
            self.recent_operations.clear();
            return;
        }

        let visible_count = self.recent_operations.len().min(20);
        let table_height = Self::table_scroll_height(visible_count, RECENT_OPERATIONS_TABLE_HEIGHT);
        TableBuilder::new(ui)
            .id_salt("recent-operations-table")
            .striped(true)
            .column(Column::initial(170.0))
            .column(Column::initial(120.0))
            .column(Column::initial(110.0))
            .column(Column::initial(80.0))
            .column(Column::initial(80.0))
            .column(Column::remainder().clip(true))
            .min_scrolled_height(table_height)
            .max_scroll_height(table_height)
            .header(TABLE_ROW_HEIGHT, |mut header| {
                header.col(|ui| {
                    Self::table_header_label(ui, "时间");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "设备");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "操作");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "状态");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "RTT");
                });
                header.col(|ui| {
                    Self::table_header_label(ui, "详情");
                });
            })
            .body(|body| {
                body.rows(TABLE_ROW_HEIGHT, visible_count, |mut row| {
                    let op_index = self.recent_operations.len() - 1 - row.index();
                    let op = &self.recent_operations[op_index];
                    row.col(|ui| {
                        ui.label(&op.timestamp);
                    });
                    row.col(|ui| {
                        ui.label(&op.device_name);
                    });
                    row.col(|ui| {
                        ui.label(&op.opcode);
                    });
                    row.col(|ui| {
                        ui.label(&op.status);
                    });
                    row.col(|ui| {
                        let value = if op.rtt_ms.is_empty() {
                            "-".to_string()
                        } else {
                            op.rtt_ms.clone()
                        };
                        ui.label(value);
                    });
                    row.col(|ui| {
                        ui.label(&op.detail);
                    });
                });
            });
    }

    fn render_log_workspace(&mut self, ui: &mut egui::Ui, filtered_log_indices: &[usize]) {
        let workspace_height = ui.available_height().max(320.0);
        let total_width = ui.available_width();
        let gutter = ui.spacing().item_spacing.x;

        if total_width < 760.0 {
            let log_height = (workspace_height * 0.58).max(220.0);
            self.render_message_log_panel(ui, filtered_log_indices, log_height);
            self.render_payload_inspector(ui, (workspace_height - log_height - gutter).max(260.0));
            return;
        }

        ui.horizontal_top(|ui| {
            let max_left_width = (total_width - gutter - 300.0).max(360.0);
            let left_width = (total_width * 0.68).clamp(360.0, max_left_width);
            let right_width = (total_width - left_width - gutter).max(300.0);

            ui.allocate_ui_with_layout(
                egui::vec2(left_width, workspace_height),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    self.render_message_log_panel(ui, filtered_log_indices, workspace_height);
                },
            );

            ui.allocate_ui_with_layout(
                egui::vec2(right_width, workspace_height),
                egui::Layout::top_down(egui::Align::Min),
                |ui| {
                    self.render_payload_inspector(ui, workspace_height);
                },
            );
        });
    }

    fn render_message_log_panel(
        &mut self,
        ui: &mut egui::Ui,
        filtered_log_indices: &[usize],
        panel_height: f32,
    ) {
        Self::panel_card_frame(ui).show(ui, |ui| {
            let mut cleared_logs = false;
            ui.horizontal_wrapped(|ui| {
                Self::section_heading(ui, "消息日志");
                ui.label("筛选");
                Self::compact_widget(
                    ui,
                    210.0,
                    TextEdit::singleline(&mut self.log_filter_text)
                        .hint_text("设备 / opcode / 状态 / 主题"),
                );
                if Self::secondary_button(ui, "清空日志").clicked() {
                    self.logs.clear();
                    self.selected_log_index = None;
                    cleared_logs = true;
                }
                Self::checkbox_chip(ui, &mut self.follow_latest_logs, "跟随最新");
                ui.small(format!(
                    "{}/{}",
                    filtered_log_indices.len(),
                    self.logs.len()
                ));
            });
            if cleared_logs {
                return;
            }

            let log_table_height = (panel_height - 74.0).max(180.0);
            TableBuilder::new(ui)
                .id_salt("message-log-table")
                .striped(true)
                .sense(egui::Sense::click())
                .column(Column::initial(170.0))
                .column(Column::initial(70.0))
                .column(Column::initial(120.0))
                .column(Column::initial(90.0))
                .column(Column::initial(90.0))
                .column(Column::initial(160.0).clip(true))
                .column(Column::remainder().clip(true))
                .min_scrolled_height(log_table_height)
                .max_scroll_height(log_table_height)
                .stick_to_bottom(self.follow_latest_logs)
                .header(TABLE_ROW_HEIGHT, |mut header| {
                    header.col(|ui| {
                        Self::table_header_label(ui, "时间");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "方向");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "设备");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "操作码");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "状态");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "主题");
                    });
                    header.col(|ui| {
                        Self::table_header_label(ui, "摘要");
                    });
                })
                .body(|body| {
                    body.rows(TABLE_ROW_HEIGHT, filtered_log_indices.len(), |mut row| {
                        let log_index = filtered_log_indices[row.index()];
                        let Some(entry) = self.logs.get(log_index) else {
                            return;
                        };
                        row.set_selected(self.selected_log_index == Some(log_index));
                        row.col(|ui| {
                            ui.label(&entry.timestamp);
                        });
                        row.col(|ui| {
                            ui.label(entry.direction.as_str());
                        });
                        row.col(|ui| {
                            ui.label(&entry.device_name);
                        });
                        row.col(|ui| {
                            ui.label(&entry.opcode);
                        });
                        row.col(|ui| {
                            ui.label(&entry.status);
                        });
                        row.col(|ui| {
                            ui.label(&entry.topic);
                        });
                        row.col(|ui| {
                            ui.label(&entry.summary);
                        });
                        if row.response().clicked() {
                            self.select_log_row(log_index);
                        }
                    });
                });
        });
    }

    fn render_payload_inspector(&self, ui: &mut egui::Ui, panel_height: f32) {
        let selected_entry = self
            .selected_log_index
            .and_then(|index| self.logs.get(index));

        Self::panel_card_frame(ui).show(ui, |ui| {
            Self::section_heading(ui, "日志检查器");
            let Some(entry) = selected_entry else {
                ui.label("选择一条日志后查看解析字段和原始负载。");
                return;
            };

            ui.horizontal_wrapped(|ui| {
                ui.small(RichText::new(&entry.timestamp).monospace());
                ui.separator();
                ui.label(RichText::new(entry.direction.as_str()).strong());
                ui.separator();
                ui.label(&entry.device_name);
                ui.separator();
                ui.label(RichText::new(&entry.opcode).monospace());
            });
            ui.small(RichText::new(&entry.topic).color(ui.visuals().weak_text_color()));
            ui.add_space(4.0);

            Self::section_heading(ui, "解析详情");
            let details_height = (panel_height * 0.32).clamp(120.0, 220.0);
            if let Ok(payload) = serde_json::from_str::<Value>(&entry.payload) {
                let details = decode_payload_details(&payload);
                if details.is_empty() {
                    ui.label("当前负载暂无结构化解析。");
                } else {
                    TableBuilder::new(ui)
                        .id_salt("payload-details-table")
                        .striped(true)
                        .column(Column::initial(150.0))
                        .column(Column::remainder().clip(true))
                        .min_scrolled_height(details_height)
                        .max_scroll_height(details_height)
                        .body(|body| {
                            body.rows(TABLE_ROW_HEIGHT, details.len(), |mut row| {
                                let (key, value) = &details[row.index()];
                                row.col(|ui| {
                                    ui.label(
                                        RichText::new(key)
                                            .monospace()
                                            .strong()
                                            .color(ui.visuals().weak_text_color()),
                                    );
                                });
                                row.col(|ui| {
                                    ui.label(value);
                                });
                            });
                        });
                }
            } else {
                ui.label("当前负载不是可解析的 JSON。");
            }

            ui.separator();
            Self::section_heading(ui, "当前负载");
            let mut payload = entry.payload.clone();
            let payload_height = (panel_height - details_height - 124.0).max(180.0);
            egui::ScrollArea::vertical()
                .id_salt("current-payload-scroll")
                .max_height(payload_height)
                .show(ui, |ui| {
                    ui.add(
                        TextEdit::multiline(&mut payload)
                            .font(egui::TextStyle::Monospace)
                            .desired_width(f32::INFINITY)
                            .interactive(false),
                    );
                });
        });
    }

    fn table_scroll_height(row_count: usize, max_height: f32) -> f32 {
        let natural_height = TABLE_ROW_HEIGHT * (row_count as f32 + 1.0) + 6.0;
        natural_height.clamp(TABLE_ROW_HEIGHT * 2.0, max_height)
    }

    fn apply_visual_style(ctx: &egui::Context, theme: UiThemeMode) {
        ctx.global_style_mut(|style| {
            style.spacing.item_spacing = egui::vec2(5.0, 3.0);
            style.spacing.button_padding = egui::vec2(5.0, 0.0);
            style.spacing.interact_size = egui::vec2(32.0, COMPACT_CONTROL_HEIGHT);
            style.spacing.slider_width = 150.0;
            style.spacing.combo_width = 140.0;
            style.spacing.indent = 10.0;
            style.spacing.scroll.bar_width = 6.0;
            style.spacing.scroll.bar_inner_margin = 2.0;
            style.spacing.scroll.bar_outer_margin = 2.0;

            style.text_styles.insert(
                egui::TextStyle::Heading,
                egui::FontId::new(15.5, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Body,
                egui::FontId::new(12.0, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Button,
                egui::FontId::new(12.0, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Monospace,
                egui::FontId::new(12.0, egui::FontFamily::Monospace),
            );
            style.text_styles.insert(
                egui::TextStyle::Small,
                egui::FontId::new(10.5, egui::FontFamily::Proportional),
            );

            let visuals = &mut style.visuals;
            *visuals = match theme {
                UiThemeMode::Dark => egui::Visuals::dark(),
                UiThemeMode::Light => egui::Visuals::light(),
            };
            let palette = VisualPalette::for_theme(theme);
            apply_visual_palette(visuals, palette);
            let radius = egui::CornerRadius::same(4);
            visuals.button_frame = true;
            visuals.clip_rect_margin = 0.0;
            visuals.striped = false;
            visuals.window_shadow = egui::epaint::Shadow {
                offset: [0, 12],
                blur: 36,
                spread: 0,
                color: egui::Color32::from_rgba_unmultiplied(0, 0, 0, 120),
            };
            visuals.popup_shadow = visuals.window_shadow;
            visuals.window_corner_radius = egui::CornerRadius::same(6);
            visuals.menu_corner_radius = egui::CornerRadius::same(6);
            visuals.widgets.noninteractive.corner_radius = radius;
            visuals.widgets.inactive.corner_radius = radius;
            visuals.widgets.hovered.corner_radius = radius;
            visuals.widgets.active.corner_radius = radius;
            visuals.widgets.open.corner_radius = radius;
        });
    }

    fn side_panel_frame(style: &egui::Style) -> egui::Frame {
        egui::Frame::side_top_panel(style)
            .inner_margin(egui::Margin::symmetric(6, 4))
            .fill(style.visuals.panel_fill)
            .stroke(chrome_stroke(style.visuals.dark_mode))
    }

    fn top_toolbar_frame(style: &egui::Style) -> egui::Frame {
        egui::Frame::side_top_panel(style)
            .inner_margin(egui::Margin::symmetric(8, 0))
            .fill(style.visuals.panel_fill)
            .stroke(chrome_stroke(style.visuals.dark_mode))
    }

    fn panel_card_frame(ui: &egui::Ui) -> egui::Frame {
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(6, 4))
            .outer_margin(egui::Margin::symmetric(0, 2))
            .corner_radius(egui::CornerRadius::same(4))
            .fill(ui.visuals().faint_bg_color)
            .stroke(surface_stroke(ui.visuals().dark_mode))
    }

    fn primary_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
        ui.add(
            egui::Button::new(RichText::new(text).color(rgb(255, 255, 255)))
                .min_size(egui::vec2(0.0, COMPACT_CONTROL_HEIGHT))
                .fill(rgb(0, 54, 146))
                .stroke(stroke(rgb(0, 75, 194))),
        )
    }

    fn secondary_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
        let (fill, stroke_color) = if ui.visuals().dark_mode {
            (rgb(28, 33, 35), rgb(38, 43, 46))
        } else {
            (rgb(244, 246, 247), rgb(205, 213, 219))
        };
        ui.add(
            egui::Button::new(text)
                .min_size(egui::vec2(0.0, COMPACT_CONTROL_HEIGHT))
                .fill(fill)
                .stroke(stroke(stroke_color)),
        )
    }

    fn add_icon_button(ui: &mut egui::Ui) -> egui::Response {
        let (rect, response) =
            ui.allocate_exact_size(COMPACT_ICON_BUTTON_SIZE, egui::Sense::click());
        let pressed = response.hovered() && ui.input(|input| input.pointer.primary_down());
        let (base_fill, hover_fill, active_fill, stroke_color, icon_color) =
            if ui.visuals().dark_mode {
                (
                    rgb(20, 24, 25),
                    rgb(43, 49, 52),
                    rgb(55, 63, 66),
                    rgb(62, 70, 74),
                    rgb(147, 196, 255),
                )
            } else {
                (
                    rgb(244, 246, 247),
                    rgb(226, 232, 236),
                    rgb(214, 228, 255),
                    rgb(190, 200, 207),
                    rgb(0, 75, 194),
                )
            };
        let fill = if pressed {
            active_fill
        } else if response.hovered() {
            hover_fill
        } else {
            base_fill
        };
        let button_rect = rect.shrink2(egui::vec2(1.0, 1.0));
        ui.painter().rect(
            button_rect,
            4.0,
            fill,
            stroke(stroke_color),
            egui::StrokeKind::Inside,
        );
        let center = button_rect.center();
        let half = 4.8;
        ui.painter().line_segment(
            [
                center + egui::vec2(-half, 0.0),
                center + egui::vec2(half, 0.0),
            ],
            egui::Stroke::new(1.4_f32, icon_color),
        );
        ui.painter().line_segment(
            [
                center + egui::vec2(0.0, -half),
                center + egui::vec2(0.0, half),
            ],
            egui::Stroke::new(1.4_f32, icon_color),
        );
        response
    }

    fn theme_icon_button(ui: &mut egui::Ui, theme: UiThemeMode) -> egui::Response {
        let (rect, response) =
            ui.allocate_exact_size(COMPACT_ICON_BUTTON_SIZE, egui::Sense::click());
        let pressed = response.hovered() && ui.input(|input| input.pointer.primary_down());
        let (hover_fill, active_fill, stroke_color, icon_color) = if ui.visuals().dark_mode {
            (
                rgb(43, 49, 52),
                rgb(55, 63, 66),
                rgb(62, 70, 74),
                rgb(218, 231, 238),
            )
        } else {
            (
                rgb(226, 232, 236),
                rgb(214, 228, 255),
                rgb(190, 200, 207),
                rgb(38, 48, 54),
            )
        };
        let fill = if pressed {
            active_fill
        } else if response.hovered() {
            hover_fill
        } else {
            egui::Color32::TRANSPARENT
        };
        let icon_background = if fill == egui::Color32::TRANSPARENT {
            ui.visuals().panel_fill
        } else {
            fill
        };
        let painter = ui.painter_at(rect);
        let button_rect = rect.shrink2(egui::vec2(1.0, 1.0));
        if fill != egui::Color32::TRANSPARENT {
            painter.rect_filled(button_rect, 4.0, fill);
            painter.rect_stroke(
                button_rect,
                4.0,
                stroke(stroke_color),
                egui::StrokeKind::Inside,
            );
        }
        Self::paint_theme_icon(ui, rect, theme, icon_color, icon_background);
        response.on_hover_cursor(egui::CursorIcon::PointingHand)
    }

    fn paint_theme_icon(
        ui: &egui::Ui,
        rect: egui::Rect,
        theme: UiThemeMode,
        icon_color: egui::Color32,
        cutout_color: egui::Color32,
    ) {
        let painter = ui.painter_at(rect);
        let center = rect.center();
        match theme {
            UiThemeMode::Dark => {
                painter.circle_stroke(center, 3.2, egui::Stroke::new(1.3_f32, icon_color));
                for index in 0..8 {
                    let angle = index as f32 * std::f32::consts::TAU / 8.0;
                    let direction = egui::vec2(angle.cos(), angle.sin());
                    painter.line_segment(
                        [center + direction * 5.0, center + direction * 7.0],
                        egui::Stroke::new(1.1_f32, icon_color),
                    );
                }
            }
            UiThemeMode::Light => {
                painter.circle_filled(center, 5.3, icon_color);
                painter.circle_filled(center + egui::vec2(2.4, -1.4), 5.1, cutout_color);
            }
        }
    }

    fn theme_toggle_button_hover(theme: UiThemeMode) -> String {
        let target = match theme {
            UiThemeMode::Dark => "切换到浅色主题",
            UiThemeMode::Light => "切换到深色主题",
        };
        format!("当前{}，{target}", theme.label())
    }

    fn stat_chip(ui: &mut egui::Ui, label: &str, value: String) {
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(5, 1))
            .fill(ui.visuals().extreme_bg_color)
            .stroke(surface_stroke(ui.visuals().dark_mode))
            .show(ui, |ui| {
                ui.set_min_height(COMPACT_CONTROL_HEIGHT);
                ui.horizontal(|ui| {
                    ui.small(RichText::new(label).color(ui.visuals().weak_text_color()));
                    ui.separator();
                    ui.label(
                        RichText::new(value)
                            .strong()
                            .color(ui.visuals().strong_text_color()),
                    );
                });
            });
    }

    fn checkbox_chip(ui: &mut egui::Ui, checked: &mut bool, text: &str) -> egui::Response {
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(5, 1))
            .fill(ui.visuals().extreme_bg_color)
            .stroke(surface_stroke(ui.visuals().dark_mode))
            .show(ui, |ui| {
                ui.set_min_height(COMPACT_CONTROL_HEIGHT);
                ui.checkbox(checked, text)
            })
            .inner
    }

    fn section_heading(ui: &mut egui::Ui, text: &str) {
        ui.label(
            RichText::new(text)
                .size(12.0)
                .strong()
                .color(accent_text_color()),
        );
    }

    fn table_header_label(ui: &mut egui::Ui, text: &str) {
        ui.label(
            RichText::new(text)
                .size(11.0)
                .strong()
                .color(ui.visuals().weak_text_color()),
        );
    }

    fn status_chip(ui: &mut egui::Ui, label: &str, value: &str, tone: ChipTone) {
        let (fill, border, text) = match (tone, ui.visuals().dark_mode) {
            (ChipTone::Warning, true) => (rgb(49, 34, 7), rgb(171, 97, 16), rgb(255, 210, 138)),
            (ChipTone::Warning, false) => (rgb(255, 246, 225), rgb(217, 132, 29), rgb(126, 72, 9)),
        };

        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(5, 1))
            .fill(fill)
            .stroke(stroke(border))
            .show(ui, |ui| {
                ui.set_min_height(COMPACT_CONTROL_HEIGHT);
                ui.horizontal(|ui| {
                    ui.small(RichText::new(label).color(ui.visuals().weak_text_color()));
                    ui.separator();
                    ui.label(RichText::new(value).strong().color(text));
                });
            });
    }

    fn paint_tech_background(ui: &egui::Ui) {
        let rect = ui.max_rect();
        let painter = ui.painter_at(rect);
        painter.rect_filled(rect, 0.0, ui.visuals().window_fill);
    }

    fn compact_widget<W: egui::Widget>(ui: &mut egui::Ui, width: f32, widget: W) -> egui::Response {
        ui.add_sized([width, COMPACT_CONTROL_HEIGHT], widget)
    }

    fn compact_text_edit(ui: &mut egui::Ui, width: f32, value: &mut String) -> egui::Response {
        Self::compact_widget(ui, width, TextEdit::singleline(value))
    }

    fn truncate_for_row(text: &str, max_chars: usize) -> String {
        let mut iter = text.chars();
        let shortened: String = iter.by_ref().take(max_chars).collect();
        if iter.next().is_some() {
            format!("{shortened}…")
        } else {
            shortened
        }
    }
}

fn default_form_for_command(command_key: &str) -> BTreeMap<String, String> {
    let mut form = BTreeMap::new();
    if let Some(spec) = command_by_key(command_key) {
        if spec.include_dest_addr {
            form.insert("dest_addr".into(), "1".into());
        }
        for field in spec.fields {
            if field.key == "time_stamp" && field.default.is_empty() {
                form.insert(field.key.into(), current_time_stamp().to_string());
            } else {
                form.insert(field.key.into(), field.default.into());
            }
        }
    }
    form
}

fn apply_editor(device: &mut DeviceProfile, editor: &DeviceEditor) {
    device.name = editor.name.clone();
    device.device_id = editor.device_id.clone();
    device.up_topic = editor.up_topic.clone();
    device.down_topic = editor.down_topic.clone();
    device.mesh_dev_type = editor.mesh_dev_type;
    device.default_dest_addr = editor.default_dest_addr;
    device.subscribe_enabled = editor.subscribe_enabled;
}

fn extract_device_id_from_discovered_topic(topic: &str) -> Option<String> {
    let mut segments = topic.split('/').filter(|segment| !segment.is_empty());
    while let Some(segment) = segments.next() {
        if segment != "device" {
            continue;
        }
        let device_id = segments.next()?;
        let direction = segments.next()?;
        if direction == "up" && !device_id.is_empty() {
            return Some(device_id.to_string());
        }
    }
    None
}

fn extract_device_topics_from_message_topic(topic: &str) -> Option<(String, String, String)> {
    let device_id = extract_device_id_from_discovered_topic(topic)?;
    let (up_topic, down_topic) = DeviceProfile::default_topics(&device_id);
    Some((device_id, up_topic, down_topic))
}

fn discovery_reason_from_payload(payload: &Value) -> String {
    match payload
        .get("opcode")
        .and_then(Value::as_u64)
        .map(|value| value as u32)
    {
        Some(0x47) => "设备信息回复".into(),
        Some(0x1B) => "心跳上报".into(),
        Some(0x10) => "人体微波事件".into(),
        Some(0x1D) => "运行状态回复".into(),
        Some(opcode) => format!("上行消息 0x{opcode:02X}"),
        None => "上行消息".into(),
    }
}

fn suggested_discovered_name(device_id: &str, dev_model: Option<&str>) -> String {
    if let Some(model) = dev_model.filter(|model| !model.trim().is_empty()) {
        return model.to_string();
    }
    let suffix_len = device_id.chars().count().min(6);
    let suffix = device_id
        .chars()
        .rev()
        .take(suffix_len)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("发现设备 {suffix}")
}

fn update_discovered_device_from_payload(candidate: &mut DiscoveredDevice, payload: &Value) {
    let Some(opcode) = payload
        .get("opcode")
        .and_then(Value::as_u64)
        .map(|value| value as u32)
    else {
        return;
    };

    if let Some(src_addr) = payload.get("src_addr").and_then(Value::as_u64)
        && let Ok(src_addr) = u16::try_from(src_addr)
        && src_addr != 0
    {
        candidate.default_dest_addr = Some(src_addr);
    }

    match opcode {
        0x47 => {
            if let Some(model) = payload.get("dev_model").and_then(Value::as_str) {
                candidate.dev_model = model.to_string();
                candidate.suggested_name =
                    suggested_discovered_name(&candidate.device_id, Some(model));
            }
            if let Some(version) = payload.get("version").and_then(Value::as_str) {
                candidate.version = version.to_string();
            }
            if let Some(device_id) = payload.get("dev_id").and_then(Value::as_str)
                && !device_id.is_empty()
            {
                candidate.device_id = device_id.to_string();
                let (up_topic, down_topic) = DeviceProfile::default_topics(device_id);
                candidate.up_topic = up_topic;
                candidate.down_topic = down_topic;
            }
        }
        0x1B => {
            if let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = crate::protocol::hex_to_bytes(value)
            {
                if bytes.len() >= 3 {
                    candidate.version = format!("0x{:02X}", bytes[2]);
                }
                if bytes.len() >= 4 {
                    candidate.mesh_dev_type = bytes[3];
                    if candidate.dev_model.is_empty() {
                        candidate.suggested_name = suggested_discovered_name(
                            &candidate.device_id,
                            Some(match bytes[3] {
                                0 => "A灯",
                                1 => "B/C灯",
                                2 => "节能灯",
                                _ => "发现设备",
                            }),
                        );
                    }
                }
            }
        }
        0x10 => {}
        0x1D => {
            if candidate.default_dest_addr.is_none()
                && let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = crate::protocol::hex_to_bytes(value)
                && bytes.len() >= 7
                && matches!(bytes[0], 8 | 255)
            {
                candidate.last_summary = format!(
                    "{} | MAC {}",
                    candidate.last_summary,
                    bytes[1..7]
                        .iter()
                        .map(|byte| format!("{byte:02X}"))
                        .collect::<Vec<_>>()
                        .join(":")
                );
            }
        }
        _ => {}
    }
}

fn normalized_topic_prefix(topic: &str) -> &str {
    topic.trim_end_matches('/')
}

fn topic_matches_device_up_topic(device_up_topic: &str, incoming_topic: &str) -> bool {
    let prefix = normalized_topic_prefix(device_up_topic);
    if prefix.is_empty() {
        return false;
    }
    incoming_topic == prefix
        || incoming_topic
            .strip_prefix(prefix)
            .map(|suffix| suffix.starts_with('/'))
            .unwrap_or(false)
}

fn compatible_up_topic_filters(topic: &str) -> Vec<String> {
    let prefix = normalized_topic_prefix(topic);
    if prefix.is_empty() {
        return Vec::new();
    }
    vec![prefix.to_string(), format!("{prefix}/#")]
}

fn subscription_topics_for_devices(
    devices: &[DeviceProfile],
    auto_discovery_enabled: bool,
) -> Vec<String> {
    let mut topics = devices
        .iter()
        .filter(|device| device.subscribe_enabled)
        .flat_map(|device| compatible_up_topic_filters(&device.up_topic))
        .collect::<BTreeSet<_>>();

    if auto_discovery_enabled {
        topics.extend(
            DISCOVERY_UP_TOPIC_FILTERS
                .iter()
                .map(|topic| (*topic).to_string()),
        );
    }

    topics.into_iter().collect()
}

fn configure_chinese_fonts(ctx: &egui::Context) {
    let mut fonts = FontDefinitions::default();

    let proportional_font = load_font_from_candidates("zh_cn_ui", PROPORTIONAL_CJK_FONT_CANDIDATES);
    let proportional_name = proportional_font.as_ref().map(|(name, _)| name.clone());
    if let Some((name, font_data)) = proportional_font {
        fonts
            .font_data
            .insert(name.clone(), std::sync::Arc::new(font_data));
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .insert(0, name);
    }

    if let Some((name, font_data)) =
        load_font_from_candidates("zh_cn_mono", MONOSPACE_CJK_FONT_CANDIDATES)
    {
        fonts
            .font_data
            .insert(name.clone(), std::sync::Arc::new(font_data));
        fonts
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .insert(0, name);
    } else if let Some(name) = proportional_name {
        fonts
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .push(name);
    }

    ctx.set_fonts(fonts);
}

const PROPORTIONAL_CJK_FONT_CANDIDATES: &[(&str, u32)] = &[
    ("assets/fonts/SourceHanSansSC-Regular.otf", 0),
    ("assets/fonts/SourceHanSansCN-Regular.otf", 0),
    ("assets/fonts/NotoSansCJKsc-Regular.otf", 0),
    ("assets/fonts/NotoSansCJKSC-Regular.otf", 0),
    ("assets/fonts/NotoSansSC-Regular.otf", 0),
    ("/System/Library/Fonts/PingFang.ttc", 0),
    ("/System/Library/Fonts/Hiragino Sans GB.ttc", 0),
    ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 0),
    (
        "/usr/share/fonts/opentype/noto/NotoSansCJKSC-Regular.otf",
        0,
    ),
    (
        "/usr/share/fonts/opentype/noto/NotoSansCJKsc-Regular.otf",
        0,
    ),
    (
        "/usr/share/fonts/opentype/adobe-source-han-sans/SourceHanSansSC-Regular.otf",
        0,
    ),
    ("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc", 0),
    ("/System/Library/Fonts/STHeiti Medium.ttc", 0),
    ("/System/Library/Fonts/STHeiti Light.ttc", 0),
    ("C:/Windows/Fonts/msyh.ttc", 0),
    ("C:/Windows/Fonts/Deng.ttf", 0),
];

const MONOSPACE_CJK_FONT_CANDIDATES: &[(&str, u32)] = &[
    ("assets/fonts/NotoSansMonoCJKsc-Regular.otf", 0),
    ("assets/fonts/NotoSansMonoCJKSC-Regular.otf", 0),
    ("assets/fonts/SarasaMonoSC-Regular.ttf", 0),
    ("assets/fonts/SarasaMonoSC-Regular.otf", 0),
    ("assets/fonts/SarasaGothicSC-Regular.ttf", 0),
    (
        "/usr/share/fonts/opentype/noto/NotoSansMonoCJK-Regular.ttc",
        0,
    ),
    (
        "/usr/share/fonts/opentype/noto/NotoSansMonoCJKsc-Regular.otf",
        0,
    ),
];

fn load_font_from_candidates(name: &str, candidates: &[(&str, u32)]) -> Option<(String, FontData)> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(Path::to_path_buf));

    for (candidate, index) in candidates {
        let paths = if Path::new(candidate).is_absolute() {
            vec![PathBuf::from(candidate)]
        } else {
            let mut paths = vec![PathBuf::from(candidate)];
            if let Some(dir) = &exe_dir {
                paths.push(dir.join(candidate));
            }
            paths
        };

        for path in paths {
            if !path.exists() {
                continue;
            }
            if let Ok(bytes) = fs::read(&path) {
                let mut data = FontData::from_owned(bytes).tweak(FontTweak {
                    scale: 0.92,
                    y_offset_factor: 0.12,
                    ..Default::default()
                });
                data.index = *index;
                return Some((name.into(), data));
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(
        device_local_id: u64,
        expected_opcode: u32,
        time_stamp: Option<u64>,
    ) -> PendingRequest {
        PendingRequest {
            device_local_id,
            device_name: "设备A".into(),
            device_id: "dev-a".into(),
            topic: "topic/down".into(),
            opcode: 0x46,
            expected_opcode,
            sent_at: Instant::now(),
            time_stamp,
        }
    }

    #[test]
    fn pending_match_prefers_exact_timestamp() {
        let requests = vec![pending(1, 0x47, Some(100)), pending(1, 0x47, Some(200))];
        let index = match_pending_request_index(&requests, 1, 0x47, Some(200));
        assert_eq!(index, Some(1));
    }

    #[test]
    fn pending_match_allows_single_unique_candidate_when_reply_lacks_timestamp() {
        let requests = vec![pending(1, 0x41, None)];
        let index = match_pending_request_index(&requests, 1, 0x41, None);
        assert_eq!(index, Some(0));
    }

    #[test]
    fn pending_match_rejects_ambiguous_candidates_without_timestamp() {
        let requests = vec![pending(1, 0x47, Some(100)), pending(1, 0x47, Some(200))];
        let index = match_pending_request_index(&requests, 1, 0x47, None);
        assert_eq!(index, None);
    }

    #[test]
    fn pending_match_rejects_missing_timestamp_for_timestamped_response() {
        let requests = vec![pending(1, 0x47, Some(100))];
        let index = match_pending_request_index(&requests, 1, 0x47, None);
        assert_eq!(index, None);
    }

    #[test]
    fn transfer_engine_settings_are_independent_by_transfer_family() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.transfer_packet_delay_ms = 25;
        app.transfer_ack_timeout_secs = 30;
        app.bc_ota_start_ack_timeout_secs = 45;
        app.transfer_max_retries = 4;
        app.voice_transfer_packet_delay_ms = 7;
        app.voice_transfer_ack_timeout_secs = 9;
        app.voice_transfer_max_retries = 1;

        let ota = app.transfer_engine_settings(TransferKind::BcOta);
        assert_eq!(ota.packet_delay_ms, 25);
        assert_eq!(ota.ack_timeout_secs, 30);
        assert_eq!(ota.bc_ota_start_ack_timeout_secs, 45);
        assert_eq!(ota.max_retries, 4);

        let voice = app.transfer_engine_settings(TransferKind::VoiceFile);
        assert_eq!(voice.packet_delay_ms, 7);
        assert_eq!(voice.ack_timeout_secs, 9);
        assert_eq!(voice.bc_ota_start_ack_timeout_secs, 45);
        assert_eq!(voice.max_retries, 1);
    }

    fn log_entry(device_id: &str, opcode: &str, summary: &str) -> LogEntry {
        LogEntry {
            timestamp: now_display(),
            direction: LogDirection::Rx,
            device_name: format!("设备{device_id}"),
            device_id: device_id.into(),
            topic: format!("/application/AP-C-BM/device/{device_id}/up"),
            opcode: opcode.into(),
            status: "接收".into(),
            summary: summary.into(),
            payload: "{}".into(),
        }
    }

    #[test]
    fn selected_log_indices_preserve_source_indices_for_virtual_rows() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.config.devices = vec![DeviceProfile {
            local_id: 10,
            name: "设备A".into(),
            device_id: "dev-a".into(),
            up_topic: "/application/AP-C-BM/device/dev-a/up".into(),
            down_topic: "/application/AP-C-BM/device/dev-a/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        }];
        app.selected_devices.insert(10);
        app.show_selected_logs_only = true;
        app.log_filter_text = "OTA".into();
        app.logs = vec![
            log_entry("dev-a", "0x41", "OTA ack first"),
            log_entry("dev-b", "0x41", "OTA ack other device"),
            log_entry("dev-a", "0x1B", "heartbeat"),
            log_entry("dev-a", "0x42", "OTA data"),
        ];

        assert_eq!(app.selected_log_indices(), vec![0, 3]);
    }

    #[test]
    fn select_log_row_updates_selected_entry_and_stops_following() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.follow_latest_logs = true;

        app.select_log_row(3);

        assert_eq!(app.selected_log_index, Some(3));
        assert!(!app.follow_latest_logs);
    }

    fn sample_transfer() -> ActiveTransfer {
        ActiveTransfer {
            device_local_id: 1,
            device_name: "设备A".into(),
            device_id: "dev-a".into(),
            up_topic: "topic/up".into(),
            down_topic: "topic/down".into(),
            kind: TransferKind::BcOta,
            packets: vec![
                serde_json::json!({"opcode": 0x40}),
                serde_json::json!({"opcode": 0x42}),
            ],
            next_index: 1,
            next_send_at: Instant::now(),
            waiting_ack_opcode: Some(0x41),
            waiting_since: Some(Instant::now()),
            last_sent_index: Some(0),
            last_sent_time_stamp: Some(123),
            retry_count: 0,
            max_retries: 2,
            status: "等待ACK 0x41".into(),
            terminal: false,
            succeeded: false,
            paused: false,
            failure_packet_index: None,
            last_failure_reason: String::new(),
        }
    }

    fn sample_final_packet_transfer() -> ActiveTransfer {
        let mut transfer = sample_transfer();
        transfer.next_index = transfer.packets.len();
        transfer.waiting_ack_opcode = Some(0x41);
        transfer.waiting_since = Some(Instant::now());
        transfer.last_sent_index = Some(transfer.packets.len() - 1);
        transfer.last_sent_time_stamp = Some(456);
        transfer
    }

    fn sample_a_ota_mid_transfer() -> ActiveTransfer {
        ActiveTransfer {
            device_local_id: 1,
            device_name: "设备A".into(),
            device_id: "dev-a".into(),
            up_topic: "topic/up".into(),
            down_topic: "topic/down".into(),
            kind: TransferKind::AOta,
            packets: vec![
                serde_json::json!({"opcode": 0x43}),
                serde_json::json!({"opcode": 0x45}),
                serde_json::json!({"opcode": 0x45}),
            ],
            next_index: 2,
            next_send_at: Instant::now(),
            waiting_ack_opcode: None,
            waiting_since: None,
            last_sent_index: Some(1),
            last_sent_time_stamp: Some(234),
            retry_count: 0,
            max_retries: 2,
            status: "发送中 2/3".into(),
            terminal: false,
            succeeded: false,
            paused: false,
            failure_packet_index: None,
            last_failure_reason: String::new(),
        }
    }

    #[test]
    fn bc_ota_display_progress_hides_start_packet() {
        let transfer = sample_transfer();
        assert_eq!(transfer_display_progress(&transfer), "0/1");
        assert_eq!(
            transfer_display_total_packets(TransferKind::BcOta, transfer.packets.len()),
            1
        );
    }

    #[test]
    fn transfer_packet_log_sampling_keeps_key_packets() {
        assert!(should_log_transfer_packet(0, 8673));
        assert!(should_log_transfer_packet(100, 8673));
        assert!(should_log_transfer_packet(8672, 8673));
        assert!(!should_log_transfer_packet(99, 8673));
        assert!(!should_log_transfer_packet(101, 8673));
    }

    #[test]
    fn bc_ota_display_packet_number_hides_start_packet_slot() {
        assert_eq!(transfer_display_packet_number(TransferKind::BcOta, 0), 1);
        assert_eq!(transfer_display_packet_number(TransferKind::BcOta, 1), 1);
        assert_eq!(transfer_display_packet_number(TransferKind::BcOta, 2), 2);
    }

    #[test]
    fn bc_ota_start_ack_uses_extended_timeout() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![{
                let mut transfer = sample_transfer();
                transfer.waiting_since = Some(Instant::now() - Duration::from_secs(12));
                transfer
            }],
            ..MeshBcTesterApp::new_for_test()
        };
        app.transfer_ack_timeout_secs = 10;
        app.bc_ota_start_ack_timeout_secs = 20;

        app.collect_transfer_timeouts();

        let transfer = &app.active_transfers[0];
        assert_eq!(transfer.status, "等待ACK 0x41");
        assert_eq!(transfer.retry_count, 0);
        assert_eq!(transfer.waiting_ack_opcode, Some(0x41));
    }

    #[test]
    fn bc_ota_data_ack_keeps_using_normal_timeout() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![{
                let mut transfer = sample_transfer();
                transfer.last_sent_index = Some(1);
                transfer.next_index = 2;
                transfer.waiting_since = Some(Instant::now() - Duration::from_secs(12));
                transfer
            }],
            ..MeshBcTesterApp::new_for_test()
        };
        app.transfer_ack_timeout_secs = 10;
        app.bc_ota_start_ack_timeout_secs = 20;

        app.collect_transfer_timeouts();

        let transfer = &app.active_transfers[0];
        assert_eq!(transfer.retry_count, 1);
        assert_eq!(transfer.next_index, 1);
        assert!(transfer.status.contains("ACK超时 0x41"));
        assert_eq!(transfer.failure_packet_index, Some(1));
    }

    #[test]
    fn transfer_retry_state_requeues_when_retries_remain() {
        let mut transfer = sample_transfer();
        apply_transfer_retry_state(&mut transfer, 0, "ACK超时 0x41".into(), 15);
        assert_eq!(transfer.retry_count, 1);
        assert_eq!(transfer.next_index, 0);
        assert!(transfer.status.contains("准备重试"));
        assert!(transfer.waiting_ack_opcode.is_none());
        assert_eq!(transfer.failure_packet_index, Some(0));
        assert_eq!(transfer.last_failure_reason, "ACK超时 0x41");
    }

    #[test]
    fn transfer_retry_state_finishes_when_retry_budget_exhausted() {
        let mut transfer = sample_transfer();
        transfer.retry_count = transfer.max_retries;
        let total_packets = transfer.packets.len();
        apply_transfer_retry_state(&mut transfer, 0, "ACK超时 0x41".into(), 15);
        assert_eq!(transfer.next_index, total_packets);
        assert_eq!(transfer.status, "ACK超时 0x41");
    }

    #[test]
    fn retry_failed_transfer_resets_terminal_state() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![{
                let mut transfer = sample_transfer();
                transfer.terminal = true;
                transfer.succeeded = false;
                transfer.status = "ACK超时 0x41".into();
                transfer.next_index = transfer.packets.len();
                transfer
            }],
            ..MeshBcTesterApp::new_for_test()
        };
        app.retry_failed_transfers_for_devices(&[1]);
        let transfer = &app.active_transfers[0];
        assert!(!transfer.terminal);
        assert_eq!(transfer.status, "重新排队");
        assert_eq!(transfer.retry_count, 0);
    }

    #[test]
    fn clear_terminal_transfers_removes_finished_rows() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![{
                let mut transfer = sample_transfer();
                transfer.terminal = true;
                transfer.succeeded = true;
                transfer
            }],
            ..MeshBcTesterApp::new_for_test()
        };
        app.clear_terminal_transfers();
        assert!(app.active_transfers.is_empty());
    }

    #[test]
    fn resume_paused_transfer_clears_pause_and_sets_status() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![{
                let mut transfer = sample_transfer();
                transfer.paused = true;
                transfer.status = "已获同意，等待继续".into();
                transfer.waiting_ack_opcode = None;
                transfer
            }],
            ..MeshBcTesterApp::new_for_test()
        };
        app.resume_transfers_for_devices(&[1]);
        let transfer = &app.active_transfers[0];
        assert!(!transfer.paused);
        assert_eq!(transfer.status, "继续传输");
    }

    #[test]
    fn cancel_transfer_marks_terminal_and_logs_entry() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.cancel_transfers_for_devices(&[1]);
        assert_eq!(app.active_transfers.len(), 1);
        let transfer = &app.active_transfers[0];
        assert!(transfer.terminal);
        assert!(!transfer.succeeded);
        assert_eq!(transfer.status, "已取消");
        assert_eq!(
            app.logs.last().map(|entry| entry.status.as_str()),
            Some("取消")
        );
    }

    #[test]
    fn connection_loss_pauses_active_ota_transfer_for_recovery() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_transfer()],
            pending_requests: vec![pending(1, 0x41, None)],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());

        app.handle_mqtt_connection_state(false, true, "连接断开，自动重连中");

        let transfer = &app.active_transfers[0];
        assert!(transfer.paused);
        assert_eq!(transfer.next_index, 0);
        assert!(transfer.waiting_ack_opcode.is_none());
        assert!(app.pending_requests.is_empty());
        assert!(app.needs_connection_recovery);
        assert_eq!(
            app.runtime_states
                .get(&1)
                .map(|state| state.last_result.as_str()),
            Some("连接断开，自动重连后从第1包继续")
        );
    }

    #[test]
    fn connection_recovery_resumes_paused_ota_transfer() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());

        app.handle_mqtt_connection_state(false, true, "连接断开，自动重连中");
        app.handle_mqtt_connection_state(true, false, "已重连");

        let transfer = &app.active_transfers[0];
        assert!(!transfer.paused);
        assert_eq!(transfer.next_index, 0);
        assert_eq!(transfer.status, "连接已恢复，从第1包继续");
        assert!(app.broker_connected);
        assert!(!app.needs_connection_recovery);
        assert_eq!(
            app.runtime_states
                .get(&1)
                .map(|state| state.last_result.as_str()),
            Some("连接已恢复，从第1包继续")
        );
    }

    #[test]
    fn a_ota_connection_loss_rewinds_to_last_sent_packet() {
        let transfer = sample_a_ota_mid_transfer();
        assert_eq!(transfer_resume_index_after_disconnect(&transfer), 1);
    }

    #[test]
    fn raw_send_confirmation_required_for_dangerous_opcode() {
        let app = MeshBcTesterApp::new_for_test();
        assert!(app.should_confirm_raw_send(0x4A, 1));
    }

    #[test]
    fn raw_send_confirmation_required_for_multi_device_send() {
        let app = MeshBcTesterApp::new_for_test();
        assert!(app.should_confirm_raw_send(0x46, 2));
        assert!(!app.should_confirm_raw_send(0x46, 1));
    }

    #[test]
    fn voice_file_chunk_requires_ack_0x57() {
        let ack = transfer_expected_ack_opcode(TransferKind::VoiceFile, 0x56, 2, 5);
        assert_eq!(ack, Some(0x57));
    }

    #[test]
    fn bc_ota_data_packet_requires_ack_0x41() {
        let ack = transfer_expected_ack_opcode(TransferKind::BcOta, 0x42, 2, 3);
        assert_eq!(ack, Some(0x41));
    }

    #[test]
    fn device_state_updates_from_energy_reply() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x29,
            "power": 40,
            "energy_consumption": 12
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_power_w, "40");
        assert_eq!(state.last_energy_kwh, "12");
    }

    #[test]
    fn device_state_updates_from_a_light_list_reply() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x51,
            "array_total_size": 88,
            "value_array": ["E001112233445566"]
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_a_light_total, "88");
        assert_eq!(state.last_a_light_preview, "0xE001/11:22:33:44:55:66");
    }

    #[test]
    fn device_by_topic_matches_nested_gen_subtopic() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.config.devices = vec![DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        }];

        let device = app.device_by_topic("/application/AP-C-BM/device/34B7DA848802/up/gen/0");
        assert_eq!(device.map(|item| item.local_id), Some(1));
    }

    #[test]
    fn bc_ota_ack_on_nested_gen_topic_without_timestamp_is_accepted() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.config.devices = vec![DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        }];
        app.runtime_states.insert(1, DeviceRuntimeState::default());

        let device = app
            .device_by_topic("/application/AP-C-BM/device/34B7DA848802/up/gen/0")
            .cloned()
            .expect("nested gen topic should resolve to device");
        let payload = serde_json::json!({
            "opcode": 0x41,
            "value": 3
        });

        app.resolve_transfer_ack(&device, &payload);

        let transfer = &app.active_transfers[0];
        assert!(!transfer.paused);
        assert_eq!(transfer.status, "BC OTA ACK成功，继续发送");
        assert!(transfer.waiting_ack_opcode.is_none());
        assert_eq!(
            app.runtime_states
                .get(&1)
                .map(|state| state.last_result.as_str()),
            Some("BC OTA ACK成功，继续发送")
        );
    }

    #[test]
    fn bc_ota_ack_value_2_requeues_current_packet() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());
        let device = DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        };

        app.resolve_transfer_ack(
            &device,
            &serde_json::json!({
                "opcode": 0x41,
                "value": 2
            }),
        );

        let transfer = &app.active_transfers[0];
        assert_eq!(transfer.next_index, 0);
        assert_eq!(transfer.retry_count, 1);
        assert!(transfer.status.contains("准备重试"));
        assert!(transfer.waiting_ack_opcode.is_none());
        assert_eq!(transfer.failure_packet_index, Some(0));
    }

    #[test]
    fn bc_ota_last_packet_ack_value_3_keeps_waiting_for_upgrade_result() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_final_packet_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());
        let device = DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        };

        app.resolve_transfer_ack(
            &device,
            &serde_json::json!({
                "opcode": 0x41,
                "value": 3
            }),
        );

        let transfer = &app.active_transfers[0];
        assert_eq!(transfer.status, "最后包已确认，等待升级结果");
        assert_eq!(transfer.waiting_ack_opcode, Some(0x41));
        assert!(transfer.waiting_since.is_some());
    }

    #[test]
    fn bc_ota_final_ack_value_5_marks_transfer_success() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_final_packet_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());
        let device = DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        };

        app.resolve_transfer_ack(
            &device,
            &serde_json::json!({
                "opcode": 0x41,
                "value": 5
            }),
        );
        app.tick_active_transfers();

        let transfer = &app.active_transfers[0];
        assert!(transfer.terminal);
        assert!(transfer.succeeded);
        assert_eq!(transfer.status, "BC OTA升级成功");
    }

    #[test]
    fn bc_ota_final_ack_value_4_marks_transfer_failed() {
        let mut app = MeshBcTesterApp {
            active_transfers: vec![sample_final_packet_transfer()],
            ..MeshBcTesterApp::new_for_test()
        };
        app.runtime_states.insert(1, DeviceRuntimeState::default());
        let device = DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        };

        app.resolve_transfer_ack(
            &device,
            &serde_json::json!({
                "opcode": 0x41,
                "value": 4
            }),
        );
        app.tick_active_transfers();

        let transfer = &app.active_transfers[0];
        assert!(transfer.terminal);
        assert!(!transfer.succeeded);
        assert_eq!(transfer.status, "BC OTA升级失败");
        assert_eq!(transfer.failure_packet_index, Some(1));
    }

    #[test]
    fn device_by_topic_rejects_similar_but_unrelated_topic() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.config.devices = vec![DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        }];

        let device = app.device_by_topic("/application/AP-C-BM/device/34B7DA848802/upstream/gen/0");
        assert!(device.is_none());
    }

    #[test]
    fn compatible_up_topic_filters_cover_exact_and_nested_topics() {
        let topics = compatible_up_topic_filters("/application/AP-C-BM/device/34B7DA848802/up");
        assert_eq!(
            topics,
            vec![
                "/application/AP-C-BM/device/34B7DA848802/up".to_string(),
                "/application/AP-C-BM/device/34B7DA848802/up/#".to_string(),
            ]
        );
    }

    #[test]
    fn subscription_topics_include_discovery_wildcards_when_enabled() {
        let device = DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        };

        let topics = subscription_topics_for_devices(&[device], true);

        assert!(topics.contains(&"/application/AP-C-BM/device/+/up".to_string()));
        assert!(topics.contains(&"/application/AP-C-BM/device/+/up/#".to_string()));
        assert!(topics.contains(&"/application/AP-C-BM/device/34B7DA848802/up/#".to_string()));
    }

    #[test]
    fn subscription_topics_skip_discovery_wildcards_when_disabled() {
        let topics = subscription_topics_for_devices(&[], false);

        assert!(!topics.contains(&"/application/AP-C-BM/device/+/up".to_string()));
        assert!(!topics.contains(&"/application/AP-C-BM/device/+/up/#".to_string()));
    }

    #[test]
    fn extract_device_id_from_discovered_topic_supports_nested_up_topics() {
        let topic = "/application/AP-C-BM/device/34B7DA848802/up/gen/0";
        assert_eq!(
            extract_device_id_from_discovered_topic(topic).as_deref(),
            Some("34B7DA848802")
        );
    }

    #[test]
    fn observe_discovered_message_creates_candidate_from_heartbeat() {
        let mut app = MeshBcTesterApp::new_for_test();
        let payload = serde_json::json!({
            "opcode": 0x1B,
            "value": "01030D01",
            "src_addr": 0x1234
        });

        app.observe_discovered_message(
            "/application/AP-C-BM/device/34B7DA848802/up/gen/0",
            Some(&payload),
            &payload.to_string(),
        );

        assert_eq!(app.discovered_devices.len(), 1);
        let candidate = &app.discovered_devices[0];
        assert_eq!(candidate.device_id, "34B7DA848802");
        assert_eq!(
            candidate.up_topic,
            "/application/AP-C-BM/device/34B7DA848802/up"
        );
        assert_eq!(
            candidate.down_topic,
            "/application/AP-C-BM/device/34B7DA848802/down"
        );
        assert_eq!(candidate.discovery_reason, "心跳上报");
        assert_eq!(candidate.mesh_dev_type, 1);
        assert_eq!(candidate.default_dest_addr, Some(0x1234));
        assert_eq!(candidate.version, "0x0D");
    }

    #[test]
    fn import_discovered_device_creates_device_profile() {
        let mut app = MeshBcTesterApp::new_for_test();
        let payload = serde_json::json!({
            "opcode": 0x47,
            "version": "13",
            "dev_model": "Turbo AP-C-BM-1",
            "dev_id": "34B7DA848802"
        });

        app.observe_discovered_message(
            "/application/AP-C-BM/device/34B7DA848802/up",
            Some(&payload),
            &payload.to_string(),
        );
        app.import_discovered_device_by_id("34B7DA848802");

        assert_eq!(app.config.devices.len(), 1);
        let device = &app.config.devices[0];
        assert_eq!(device.device_id, "34B7DA848802");
        assert_eq!(
            device.up_topic,
            "/application/AP-C-BM/device/34B7DA848802/up"
        );
        assert_eq!(
            device.down_topic,
            "/application/AP-C-BM/device/34B7DA848802/down"
        );
        assert_eq!(device.name, "Turbo AP-C-BM-1");
        assert!(app.discovered_devices.is_empty());
    }

    #[test]
    fn device_editor_modal_opens_for_existing_device_and_closes_on_save() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.config.devices = vec![DeviceProfile {
            local_id: 1,
            name: "BC灯".into(),
            device_id: "34B7DA848802".into(),
            up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
            down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
        }];

        let device = app.config.devices[0].clone();
        app.open_device_editor(&device);
        assert!(app.device_editor_open);
        assert_eq!(app.device_editor.editing_id, Some(1));

        app.device_editor.name = "改名设备".into();
        app.save_device_editor();

        assert!(!app.device_editor_open);
        assert_eq!(app.config.devices[0].name, "改名设备");
        assert!(app.device_editor.editing_id.is_none());
    }

    #[test]
    fn delete_device_removes_single_row_state_and_closes_matching_editor() {
        let mut app = MeshBcTesterApp::new_for_test();
        app.config.devices = vec![
            DeviceProfile {
                local_id: 1,
                name: "BC灯".into(),
                device_id: "34B7DA848802".into(),
                up_topic: "/application/AP-C-BM/device/34B7DA848802/up".into(),
                down_topic: "/application/AP-C-BM/device/34B7DA848802/down".into(),
                mesh_dev_type: 1,
                default_dest_addr: 1,
                subscribe_enabled: true,
            },
            DeviceProfile {
                local_id: 2,
                name: "BC灯 2".into(),
                device_id: "34B7DA848803".into(),
                up_topic: "/application/AP-C-BM/device/34B7DA848803/up".into(),
                down_topic: "/application/AP-C-BM/device/34B7DA848803/down".into(),
                mesh_dev_type: 1,
                default_dest_addr: 1,
                subscribe_enabled: true,
            },
        ];
        app.selected_devices.insert(1);
        app.runtime_states.insert(1, DeviceRuntimeState::default());
        app.runtime_states.insert(2, DeviceRuntimeState::default());
        let device = app.config.devices[0].clone();
        app.open_device_editor(&device);

        app.delete_device(1);

        assert_eq!(app.config.devices.len(), 1);
        assert_eq!(app.config.devices[0].local_id, 2);
        assert!(!app.selected_devices.contains(&1));
        assert!(!app.runtime_states.contains_key(&1));
        assert!(app.runtime_states.contains_key(&2));
        assert!(!app.device_editor_open);
    }

    #[test]
    fn discovered_fill_opens_prefilled_device_editor_modal() {
        let mut app = MeshBcTesterApp::new_for_test();
        let payload = serde_json::json!({
            "opcode": 0x47,
            "version": "13",
            "dev_model": "Turbo AP-C-BM-1",
            "dev_id": "34B7DA848802"
        });

        app.observe_discovered_message(
            "/application/AP-C-BM/device/34B7DA848802/up",
            Some(&payload),
            &payload.to_string(),
        );
        app.load_discovered_device_into_editor("34B7DA848802");

        assert!(app.device_editor_open);
        assert!(app.device_editor.editing_id.is_none());
        assert_eq!(app.device_editor.name, "Turbo AP-C-BM-1");
        assert_eq!(app.device_editor.device_id, "34B7DA848802");
    }

    #[test]
    fn pending_transfer_file_dialog_applies_selected_path() {
        let (tx, rx) = mpsc::channel();
        let mut app = MeshBcTesterApp::new_for_test();
        app.pending_file_dialog = Some(PendingFileDialog {
            kind: PendingFileDialogKind::OtaTransferFile,
            rx,
        });
        tx.send(Some(PathBuf::from("/tmp/firmware.bin"))).unwrap();

        app.poll_pending_file_dialog();

        assert_eq!(app.ota_transfer_file, "/tmp/firmware.bin");
        assert!(app.pending_file_dialog.is_none());
    }

    #[test]
    fn device_state_updates_from_mesh_management_reply() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x1F,
            "value": "00E000F000E100000000"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_group_info, "00E000F000E100000000");
        assert_eq!(state.last_partition_addr, "0xE000");
        assert_eq!(state.last_lane_group_addr, "0xF000");
        assert_eq!(state.last_adjacent_group_addr, "0xE100");
    }

    #[test]
    fn device_state_updates_from_bc_info_reply() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x47,
            "version": "10",
            "dev_model": "BC-01"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_version, "10");
        assert_eq!(state.last_device_model, "BC-01");
    }

    #[test]
    fn device_state_updates_from_status_query_mac() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x1D,
            "value": "08112233445566"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_mac_addr, "11:22:33:44:55:66");
    }

    #[test]
    fn device_state_updates_from_status_query_operational_fields() {
        let mut state = DeviceRuntimeState::default();
        let payload_remote = serde_json::json!({
            "opcode": 0x1D,
            "value": "0401"
        });
        let payload_heartbeat = serde_json::json!({
            "opcode": 0x1D,
            "value": "051E"
        });
        let payload_linkage = serde_json::json!({
            "opcode": 0x1D,
            "value": "0601"
        });
        let payload_mode = serde_json::json!({
            "opcode": 0x1D,
            "value": "0902"
        });
        let payload_microwave = serde_json::json!({
            "opcode": 0x1D,
            "value": "0C05"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload_remote);
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload_heartbeat);
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload_linkage);
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload_mode);
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload_microwave);
        assert_eq!(state.last_remote_network_enable, "1");
        assert_eq!(state.last_heartbeat_interval, "30");
        assert_eq!(state.last_group_linkage, "1");
        assert_eq!(state.last_linkage_mode, "2");
        assert_eq!(state.last_microwave_setting, "5");
    }

    #[test]
    fn device_state_updates_from_linkage_group_reply() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x32,
            "value": "01"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_linkage_group_state, "01");
    }

    #[test]
    fn device_state_updates_from_motion_event() {
        let mut state = DeviceRuntimeState::default();
        let payload = serde_json::json!({
            "opcode": 0x10,
            "value": "FF"
        });
        MeshBcTesterApp::update_device_state_from_payload(&mut state, &payload);
        assert_eq!(state.last_motion_event, "检测到有人");
    }
}
