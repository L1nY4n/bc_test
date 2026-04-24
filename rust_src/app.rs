use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use eframe::egui::{self, FontData, FontDefinitions, FontFamily, FontTweak, RichText, TextEdit};
use egui_extras::{Column, TableBuilder};
use rfd::FileDialog;
use serde_json::Value;

use crate::models::{
    AppConfig, BrokerProfile, DeviceEditor, DeviceProfile, DeviceRuntimeState, LogDirection,
    LogEntry, TransferKind, now_display,
};
use crate::mqtt::{MqttEvent, MqttRuntime};
use crate::protocol::{
    COMMANDS, FieldKind, build_command_payload, build_transfer_packets, classify_execution_result,
    command_by_key, current_time_stamp, decode_payload_details, expected_response_opcode,
    parse_opcode, redact_json, response_can_omit_timestamp, summarize_payload, transfer_preview,
};
use crate::store::{load_config, save_config};

pub struct MeshBcTesterApp {
    config: AppConfig,
    broker_editor: BrokerProfile,
    device_editor: DeviceEditor,
    selected_devices: BTreeSet<u64>,
    selected_log_index: Option<usize>,
    command_key: String,
    command_form: BTreeMap<String, String>,
    raw_json_text: String,
    raw_expected_opcode: String,
    connection_status: String,
    mqtt: MqttRuntime,
    runtime_states: HashMap<u64, DeviceRuntimeState>,
    logs: Vec<LogEntry>,
    recent_operations: Vec<OperationRecord>,
    show_selected_logs_only: bool,
    log_filter_text: String,
    transfer_kind: TransferKind,
    transfer_file: String,
    transfer_version: u8,
    transfer_voice_name: String,
    transfer_packet_delay_ms: u64,
    transfer_ack_timeout_secs: u64,
    transfer_max_retries: u8,
    system_notice: String,
    pending_confirmation: Option<PendingConfirmation>,
    pending_requests: Vec<PendingRequest>,
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

struct OperationRecord {
    timestamp: String,
    device_name: String,
    opcode: String,
    status: String,
    detail: String,
    rtt_ms: String,
}

struct ActiveTransfer {
    device_local_id: u64,
    device_name: String,
    device_id: String,
    down_topic: String,
    kind: TransferKind,
    packets: Vec<Value>,
    next_index: usize,
    next_send_at: Instant,
    waiting_ack_opcode: Option<u32>,
    waiting_since: Option<Instant>,
    last_sent_index: Option<usize>,
    last_sent_time_stamp: Option<u64>,
    retry_count: u8,
    max_retries: u8,
    status: String,
    terminal: bool,
    succeeded: bool,
    paused: bool,
    failure_packet_index: Option<usize>,
    last_failure_reason: String,
}

struct PendingConfirmation {
    title: String,
    detail: String,
    action: PendingAction,
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

const TRANSFER_PACKET_DELAY_MS: u64 = 15;
const TRANSFER_ACK_TIMEOUT_SECS: u64 = 10;
const TRANSFER_MAX_RETRIES: u8 = 2;
const MAX_TRANSFER_BYTES: usize = 2 * 1024 * 1024;
const MAX_TRANSFER_PACKETS: usize = 6000;
const MAX_TRANSFER_TOTAL_PUBLISHES: usize = 12000;
const DEVICE_OFFLINE_TIMEOUT_SECS: u64 = 120;
const MAX_OPERATION_HISTORY: usize = 300;

impl MeshBcTesterApp {
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
        let transfer_max_retries = if config.transfer_max_retries == 0 {
            TRANSFER_MAX_RETRIES
        } else {
            config.transfer_max_retries
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

        Self {
            config,
            broker_editor,
            device_editor: DeviceEditor {
                mesh_dev_type: 1,
                default_dest_addr: 1,
                subscribe_enabled: true,
                ..Default::default()
            },
            selected_devices: BTreeSet::new(),
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
            mqtt: MqttRuntime::default(),
            runtime_states,
            logs: Vec::new(),
            recent_operations: Vec::new(),
            show_selected_logs_only: false,
            log_filter_text: String::new(),
            transfer_kind: TransferKind::BcOta,
            transfer_file: String::new(),
            transfer_version: 1,
            transfer_voice_name: "voice.adpcm".into(),
            transfer_packet_delay_ms,
            transfer_ack_timeout_secs,
            transfer_max_retries,
            system_notice: String::new(),
            pending_confirmation: None,
            pending_requests: Vec::new(),
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
            .find(|device| topic == device.up_topic)
    }

    fn process_mqtt_events(&mut self) {
        self.collect_pending_timeouts();
        self.collect_transfer_timeouts();
        self.collect_device_offline_timeouts();
        while let Ok(event) = self.mqtt.events_rx.try_recv() {
            match event {
                MqttEvent::Connection { message } => {
                    self.connection_status = message.clone();
                    if message != "已连接" {
                        for state in self.runtime_states.values_mut() {
                            state.online = false;
                            state.pending_count = 0;
                        }
                        self.pending_requests.clear();
                        self.active_transfers.clear();
                    }
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
                MqttEvent::Message { topic, payload } => {
                    let device = self.device_by_topic(&topic).cloned();
                    let parsed: Option<Value> = serde_json::from_str(&payload).ok();
                    let mut rx_status = "接收".to_string();
                    if let (Some(device), Some(parsed)) = (&device, &parsed) {
                        self.resolve_transfer_ack(device, parsed);
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
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    if let Ok(bytes) = crate::protocol::hex_to_bytes(value) {
                        if bytes.len() >= 2 {
                            state.last_switch_state = bytes[0].to_string();
                            state.last_run_mode = bytes[1].to_string();
                            if bytes.len() >= 3 {
                                state.last_version = format!("0x{:02X}", bytes[2]);
                            }
                        }
                    }
                }
            }
            0x1D => {
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    if let Ok(bytes) = crate::protocol::hex_to_bytes(value) {
                        if bytes.len() >= 2 {
                            match bytes[0] {
                                0 => state.last_version = format!("0x{:02X}", bytes[1]),
                                1 => state.last_run_mode = bytes[1].to_string(),
                                4 => state.last_remote_network_enable = bytes[1].to_string(),
                                5 => state.last_heartbeat_interval = bytes[1].to_string(),
                                6 => state.last_group_linkage = bytes[1].to_string(),
                                7 => state.last_switch_state = bytes[1].to_string(),
                                9 => state.last_linkage_mode = bytes[1].to_string(),
                                12 => state.last_microwave_setting = bytes[1].to_string(),
                                8 | 255 => {
                                    if bytes.len() >= 7 {
                                        state.last_mac_addr = bytes[1..7]
                                            .iter()
                                            .map(|byte| format!("{byte:02X}"))
                                            .collect::<Vec<_>>()
                                            .join(":");
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            0x22 | 0x26 => {
                if let Some(value) = payload.get("value").and_then(Value::as_str) {
                    if let Ok(bytes) = crate::protocol::hex_to_bytes(value) {
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
                    if let Ok(bytes) = crate::protocol::hex_to_bytes(value) {
                        if !bytes.is_empty() && bytes[0] == 0 && bytes.len() >= 7 {
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
                            state.last_adjacent_group_addr = if adjacent == 0 || adjacent == 0xFFFF
                            {
                                String::new()
                            } else {
                                format!("0x{adjacent:04X}")
                            };
                        }
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
        self.config.transfer_max_retries = self.transfer_max_retries;
        self.connection_status = "连接中...".into();
        self.mqtt.connect(&self.config.broker);
        self.sync_subscriptions();
        let _ = save_config(&self.config);
    }

    fn sync_subscriptions(&mut self) {
        let topics = self
            .config
            .devices
            .iter()
            .filter(|device| device.subscribe_enabled)
            .map(|device| device.up_topic.clone())
            .collect::<Vec<_>>();
        self.mqtt.sync_subscriptions(topics);
    }

    fn send_payload_to_device(
        &mut self,
        device: &DeviceProfile,
        payload: &Value,
        source: &str,
        expected_override: Option<u32>,
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
        if source != "transfer" {
            if let Some(opcode_num) = payload.get("opcode").and_then(Value::as_u64) {
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
                        Self::set_device_result(
                            state,
                            "发送",
                            format!("已发送({source}, 未跟踪应答)"),
                        );
                    }
                }
            }
        }
        let redacted = redact_json(payload);
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
            status: source.to_uppercase(),
            summary: summarize_payload(&redacted),
            payload: logged_payload,
        });
        self.append_operation(OperationRecord {
            timestamp: now_display(),
            device_name: device.name.clone(),
            opcode: opcode.clone(),
            status: source.to_uppercase(),
            detail: summarize_payload(&redacted),
            rtt_ms: String::new(),
        });
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
            if let Err(err) = self.send_payload_to_device(&device, &payload, "preset", None) {
                self.system_notice = err;
                return;
            }
        }
    }

    fn execute_raw_send(&mut self, items: Vec<(DeviceProfile, Value, Option<u32>)>) {
        for (device, payload, expected_override) in items {
            if let Err(err) =
                self.send_payload_to_device(&device, &payload, "raw", expected_override)
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
        let (status, result_summary) = classify_execution_result(payload)
            .map(|(status, summary)| (status, summary))
            .unwrap_or_else(|| {
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

        self.device_editor = DeviceEditor {
            mesh_dev_type: 1,
            default_dest_addr: 1,
            subscribe_enabled: true,
            ..Default::default()
        };
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

    fn selected_logs(&self) -> Vec<(usize, &LogEntry)> {
        self.logs
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                if !self.show_selected_logs_only || self.selected_devices.is_empty() {
                } else if !self.config.devices.iter().any(|device| {
                    self.selected_devices.contains(&device.local_id)
                        && device.device_id == entry.device_id
                }) {
                    return false;
                }
                if self.log_filter_text.trim().is_empty() {
                    return true;
                }
                let needle = self.log_filter_text.to_lowercase();
                [
                    entry.device_name.as_str(),
                    entry.device_id.as_str(),
                    entry.topic.as_str(),
                    entry.opcode.as_str(),
                    entry.status.as_str(),
                    entry.summary.as_str(),
                ]
                .iter()
                .any(|field| field.to_lowercase().contains(&needle))
            })
            .collect()
    }

    fn pick_transfer_file(&mut self) {
        if let Some(path) = FileDialog::new().pick_file() {
            self.transfer_file = path.display().to_string();
        }
    }

    fn send_transfer_preview(&mut self) {
        if self.transfer_file.trim().is_empty() {
            self.system_notice = "请先选择传输文件。".into();
            return;
        }
        let path = PathBuf::from(&self.transfer_file);
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
        let preview = transfer_preview(
            self.transfer_kind,
            &bytes,
            self.transfer_version,
            &self.transfer_voice_name,
        );
        let packets = match build_transfer_packets(
            self.transfer_kind,
            &bytes,
            self.transfer_version,
            &self.transfer_voice_name,
        ) {
            Ok(packets) => packets,
            Err(err) => {
                self.system_notice = err;
                return;
            }
        };
        if packets.len() > MAX_TRANSFER_PACKETS {
            self.system_notice = format!(
                "传输分包过多：{}，当前上限 {}。",
                packets.len(),
                MAX_TRANSFER_PACKETS
            );
            return;
        }
        if self.should_confirm_transfer(devices.len(), packets.len()) {
            self.pending_confirmation = Some(PendingConfirmation {
                title: "确认发起传输".into(),
                detail: format!(
                    "类型：{}，目标设备：{} 台，文件大小：{} 字节，分包：{}",
                    self.transfer_kind.label(),
                    devices.len(),
                    bytes.len(),
                    packets.len()
                ),
                action: PendingAction::TransferQueue {
                    devices,
                    packets,
                    preview,
                    byte_size: bytes.len(),
                    kind: self.transfer_kind,
                },
            });
            return;
        }
        let total_publishes = packets.len().saturating_mul(devices.len());
        if total_publishes > MAX_TRANSFER_TOTAL_PUBLISHES {
            self.system_notice = format!(
                "本次传输总发送量过大：{}，当前上限 {}。请减少设备数量或文件大小。",
                total_publishes, MAX_TRANSFER_TOTAL_PUBLISHES
            );
            return;
        }
        self.queue_transfer_action(devices, preview, packets, bytes.len(), self.transfer_kind);
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
                packets.len()
            ),
            payload: serde_json::to_string_pretty(&redact_json(&preview))
                .unwrap_or_else(|_| preview.to_string()),
        });
        for device in devices {
            if self
                .active_transfers
                .iter()
                .any(|transfer| transfer.device_local_id == device.local_id)
            {
                self.append_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::System,
                    device_name: device.name.clone(),
                    device_id: device.device_id.clone(),
                    topic: device.down_topic.clone(),
                    opcode: "-".into(),
                    status: "跳过".into(),
                    summary: "该设备已有活跃传输任务".into(),
                    payload: String::new(),
                });
                continue;
            }
            if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                Self::set_device_result(
                    state,
                    "传输",
                    format!(
                        "{} 已排队，共{}包",
                        self.transfer_kind.label(),
                        packets.len()
                    ),
                );
            }
            self.active_transfers.push(ActiveTransfer {
                device_local_id: device.local_id,
                device_name: device.name.clone(),
                device_id: device.device_id.clone(),
                down_topic: device.down_topic.clone(),
                kind: self.transfer_kind,
                packets: packets.clone(),
                next_index: 0,
                next_send_at: Instant::now(),
                waiting_ack_opcode: None,
                waiting_since: None,
                last_sent_index: None,
                last_sent_time_stamp: None,
                retry_count: 0,
                max_retries: self.transfer_max_retries,
                status: "已排队".into(),
                terminal: false,
                succeeded: false,
                paused: false,
                failure_packet_index: None,
                last_failure_reason: String::new(),
            });
        }
    }

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
            let opcode = packet.get("opcode").and_then(Value::as_u64).unwrap_or(0) as u32;
            match self.send_payload_to_device(device, &packet, "transfer", None) {
                Ok(()) => {
                    self.active_transfers[index].last_sent_index = Some(packet_index);
                    self.active_transfers[index].last_sent_time_stamp =
                        packet.get("time_stamp").and_then(Value::as_u64);
                    self.active_transfers[index].next_index += 1;
                    self.active_transfers[index].status = format!(
                        "发送中 {}/{}",
                        self.active_transfers[index].next_index,
                        self.active_transfers[index].packets.len()
                    );
                    if let Some(state) = self.runtime_states.get_mut(&device_local_id) {
                        Self::set_device_result(
                            state,
                            "传输",
                            format!(
                                "{} 发送中 {}/{}",
                                self.active_transfers[index].kind.label(),
                                self.active_transfers[index].next_index,
                                self.active_transfers[index].packets.len()
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
        let transfer = &mut self.active_transfers[position];
        if let Some((status, summary)) = classify_execution_result(payload) {
            if status == "错误" {
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
        if matches!(opcode, 0x41 | 0x44) {
            transfer.paused = true;
            transfer.status = "已获同意，等待继续".into();
            if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                Self::set_device_result(state, "待继续", "已获同意，等待继续".into());
            }
        } else {
            transfer.paused = false;
            transfer.next_send_at =
                Instant::now() + Duration::from_millis(TRANSFER_PACKET_DELAY_MS);
            transfer.status = "收到ACK".into();
            if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                Self::set_device_result(state, "成功", "收到传输ACK".into());
            }
        }
    }

    fn collect_transfer_timeouts(&mut self) {
        let timeout = Duration::from_secs(self.transfer_ack_timeout_secs);
        let now = Instant::now();
        let mut timed_out = Vec::new();
        for (index, transfer) in self.active_transfers.iter().enumerate() {
            if let (Some(expected_ack), Some(waiting_since)) =
                (transfer.waiting_ack_opcode, transfer.waiting_since)
            {
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
            if now.duration_since(*last_seen_at) >= timeout {
                if let Some(state) = self.runtime_states.get_mut(device_id) {
                    state.online = false;
                }
            }
        }
    }

    fn cancel_transfers_for_devices(&mut self, device_ids: &[u64]) {
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
            {
                if let Some(state) = self.runtime_states.get_mut(&device.local_id) {
                    Self::set_device_result(state, "取消", format!("{kind_label} 传输已取消"));
                }
            }
        }
    }

    fn retry_failed_transfers_for_devices(&mut self, device_ids: &[u64]) {
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

    fn resume_transfers_for_devices(&mut self, device_ids: &[u64]) {
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

    fn clear_terminal_transfers(&mut self) {
        self.active_transfers.retain(|transfer| !transfer.terminal);
    }

    fn export_evidence(&mut self) {
        let Some(path) = FileDialog::new()
            .set_file_name("mesh-bc-test-evidence.json")
            .save_file()
        else {
            return;
        };

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
            .active_transfers
            .iter()
            .map(|transfer| {
                serde_json::json!({
                    "device_name": transfer.device_name,
                    "device_id": transfer.device_id,
                    "kind": transfer.kind.label(),
                    "progress": format!("{}/{}", transfer.next_index, transfer.packets.len()),
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

        let evidence = serde_json::json!({
            "generated_at": now_display(),
            "connection_status": self.connection_status,
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
                "packet_delay_ms": self.transfer_packet_delay_ms,
                "ack_timeout_secs": self.transfer_ack_timeout_secs,
                "max_retries": self.transfer_max_retries,
            },
            "device_states": device_states,
            "pending_requests": pending,
            "active_transfers": transfers,
            "recent_operations": operations,
            "logs": logs,
        });

        match serde_json::to_string_pretty(&evidence) {
            Ok(text) => match fs::write(&path, text) {
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
            device_editor: DeviceEditor {
                mesh_dev_type: 1,
                default_dest_addr: 1,
                subscribe_enabled: true,
                ..Default::default()
            },
            selected_devices: BTreeSet::new(),
            selected_log_index: None,
            command_key: "query_bc_info".into(),
            command_form: BTreeMap::new(),
            raw_json_text: "{}".into(),
            raw_expected_opcode: String::new(),
            connection_status: "未连接".into(),
            mqtt: MqttRuntime::default(),
            runtime_states: HashMap::new(),
            logs: Vec::new(),
            recent_operations: Vec::new(),
            show_selected_logs_only: false,
            log_filter_text: String::new(),
            transfer_kind: TransferKind::BcOta,
            transfer_file: String::new(),
            transfer_version: 1,
            transfer_voice_name: "voice.adpcm".into(),
            transfer_packet_delay_ms: TRANSFER_PACKET_DELAY_MS,
            transfer_ack_timeout_secs: TRANSFER_ACK_TIMEOUT_SECS,
            transfer_max_retries: TRANSFER_MAX_RETRIES,
            system_notice: String::new(),
            pending_confirmation: None,
            pending_requests: Vec::new(),
            active_transfers: Vec::new(),
            device_last_seen_at: HashMap::new(),
        }
    }
}

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
            retry_packet_index + 1,
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
        self.tick_active_transfers();
        let ctx = ui.ctx().clone();
        ctx.request_repaint_after(Duration::from_millis(100));
        Self::apply_visual_style(&ctx);
        let panel_style = ctx.global_style();
        let panel_style = panel_style.as_ref();

        egui::Panel::top("top_bar")
            .frame(Self::top_bottom_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(RichText::new("蓝牙Mesh BC灯测试工具").heading().strong());
                    ui.separator();
                    ui.label("代理");
                    ui.text_edit_singleline(&mut self.broker_editor.name);
                    ui.label("地址");
                    ui.text_edit_singleline(&mut self.broker_editor.host);
                    ui.add(egui::DragValue::new(&mut self.broker_editor.port).range(1..=65535));
                    ui.label("用户");
                    ui.text_edit_singleline(&mut self.broker_editor.username);
                    ui.add(
                        TextEdit::singleline(&mut self.broker_editor.password)
                            .password(true)
                            .hint_text("密码"),
                    );
                    ui.label("客户端ID");
                    ui.text_edit_singleline(&mut self.broker_editor.client_id);
                    ui.add(
                        egui::DragValue::new(&mut self.broker_editor.keepalive_secs)
                            .range(5..=3600),
                    );
                    ui.checkbox(&mut self.broker_editor.use_tls, "启用 TLS");
                    if Self::primary_button(ui, "连接").clicked() {
                        self.connect();
                    }
                    if Self::secondary_button(ui, "断开").clicked() {
                        self.mqtt.disconnect();
                    }
                    if Self::secondary_button(ui, "导出结果").clicked() {
                        self.export_evidence();
                    }
                    ui.label(format!("状态: {}", self.connection_status));
                });
                if !self.system_notice.is_empty() {
                    ui.colored_label(egui::Color32::YELLOW, &self.system_notice);
                }
            });

        if let Some((title, detail)) = self
            .pending_confirmation
            .as_ref()
            .map(|pending| (pending.title.clone(), pending.detail.clone()))
        {
            egui::Panel::top("confirm_bar")
                .frame(Self::top_bottom_panel_frame(panel_style))
                .show_inside(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.colored_label(egui::Color32::YELLOW, format!("待确认: {}", title));
                        ui.separator();
                        ui.label(detail);
                        if Self::primary_button(ui, "确认执行").clicked() {
                            self.execute_pending_confirmation();
                        }
                        if Self::secondary_button(ui, "取消").clicked() {
                            self.pending_confirmation = None;
                        }
                    });
                });
        }

        egui::Panel::bottom("status_bar")
            .frame(Self::top_bottom_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                let online_count = self
                    .runtime_states
                    .values()
                    .filter(|state| state.online)
                    .count();
                ui.horizontal_wrapped(|ui| {
                    ui.label(format!("连接状态: {}", self.connection_status));
                    ui.separator();
                    ui.label(format!("已配置设备: {}", self.config.devices.len()));
                    ui.separator();
                    ui.label(format!("在线设备: {}", online_count));
                    ui.separator();
                    ui.label(format!("已选设备: {}", self.selected_devices.len()));
                    ui.separator();
                    ui.label(format!("等待应答: {}", self.pending_requests.len()));
                    ui.separator();
                    ui.label(format!("日志条数: {}", self.logs.len()));
                });
            });

        egui::Panel::left("devices_panel")
            .resizable(true)
            .default_size(340.0)
            .min_size(280.0)
            .frame(Self::side_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                Self::panel_card_collapsible(ui, "devices_card", "设备工作台", |ui| {
                    egui::ScrollArea::vertical()
                        .max_height(320.0)
                        .show(ui, |ui| {
                            for device in &self.config.devices {
                                let selected = self.selected_devices.contains(&device.local_id);
                                ui.horizontal(|ui| {
                                    let mut checked = selected;
                                    if ui.checkbox(&mut checked, "").changed() {
                                        if checked {
                                            self.selected_devices.insert(device.local_id);
                                        } else {
                                            self.selected_devices.remove(&device.local_id);
                                        }
                                    }
                                    let state = self
                                        .runtime_states
                                        .get(&device.local_id)
                                        .cloned()
                                        .unwrap_or_default();
                                    ui.vertical(|ui| {
                                        ui.label(RichText::new(&device.name).strong());
                                        ui.small(format!(
                                            "{} | RX {} TX {} 待{}",
                                            device.device_id,
                                            state.rx_count,
                                            state.tx_count,
                                            state.pending_count
                                        ));
                                        ui.small(format!("上行: {}", device.up_topic));
                                        if !state.last_rtt_ms.is_empty() {
                                            ui.small(format!("RTT: {} ms", state.last_rtt_ms));
                                        }
                                        if !state.last_version.is_empty()
                                            || !state.last_mesh_addr.is_empty()
                                            || !state.last_device_model.is_empty()
                                        {
                                            ui.small(format!(
                                                "版本:{} 型号:{} Mesh:{}",
                                                if state.last_version.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_version
                                                },
                                                if state.last_device_model.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_device_model
                                                },
                                                if state.last_mesh_addr.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_mesh_addr
                                                }
                                            ));
                                        }
                                        if !state.last_switch_state.is_empty()
                                            || !state.last_run_mode.is_empty()
                                            || !state.last_heartbeat_interval.is_empty()
                                            || !state.last_remote_network_enable.is_empty()
                                            || !state.last_group_linkage.is_empty()
                                            || !state.last_linkage_mode.is_empty()
                                            || !state.last_microwave_setting.is_empty()
                                            || !state.last_linkage_group_state.is_empty()
                                        {
                                            ui.small(format!(
                                                "开关:{} 模式:{} 心跳:{} 遥控组网:{} 组内联动:{} 联动模式:{} 微波:{} 联动组:{}",
                                                if state.last_switch_state.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_switch_state
                                                },
                                                if state.last_run_mode.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_run_mode
                                                }
                                                ,
                                                if state.last_heartbeat_interval.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_heartbeat_interval
                                                },
                                                if state.last_remote_network_enable.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_remote_network_enable
                                                },
                                                if state.last_group_linkage.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_group_linkage
                                                },
                                                if state.last_linkage_mode.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_linkage_mode
                                                },
                                                if state.last_microwave_setting.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_microwave_setting
                                                },
                                                if state.last_linkage_group_state.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_linkage_group_state
                                                }
                                            ));
                                        }
                                        if !state.last_scene_id.is_empty()
                                            || !state.last_energy_kwh.is_empty()
                                            || !state.last_a_light_total.is_empty()
                                            || !state.last_power_w.is_empty()
                                        {
                                            ui.small(format!(
                                                "场景:{} 功率:{}W 能耗:{}kWh A灯:{}",
                                                if state.last_scene_id.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_scene_id
                                                },
                                                if state.last_power_w.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_power_w
                                                },
                                                if state.last_energy_kwh.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_energy_kwh
                                                },
                                                if state.last_a_light_total.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_a_light_total
                                                }
                                            ));
                                        }
                                        if !state.last_a_light_preview.is_empty() {
                                            ui.small(format!(
                                                "A灯样本: {}",
                                                state.last_a_light_preview
                                            ));
                                        }
                                        if !state.last_group_info.is_empty()
                                            || !state.last_motion_event.is_empty()
                                        {
                                            ui.small(format!(
                                                "组网:{} 事件:{}",
                                                if state.last_group_info.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_group_info
                                                },
                                                if state.last_motion_event.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_motion_event
                                                }
                                            ));
                                        }
                                        if !state.last_mac_addr.is_empty()
                                            || !state.last_partition_addr.is_empty()
                                            || !state.last_lane_group_addr.is_empty()
                                            || !state.last_adjacent_group_addr.is_empty()
                                        {
                                            ui.small(format!(
                                                "MAC:{} 分区:{} 车道组:{} 相邻组:{}",
                                                if state.last_mac_addr.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_mac_addr
                                                },
                                                if state.last_partition_addr.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_partition_addr
                                                },
                                                if state.last_lane_group_addr.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_lane_group_addr
                                                },
                                                if state.last_adjacent_group_addr.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_adjacent_group_addr
                                                },
                                            ));
                                        }
                                        if !state.last_scene_summary.is_empty() {
                                            ui.small(format!(
                                                "场景摘要: {}",
                                                state.last_scene_summary
                                            ));
                                        }
                                        if !state.last_result.is_empty() {
                                            ui.small(format!(
                                                "结果[{}]: {}",
                                                if state.last_result_label.is_empty() {
                                                    "--"
                                                } else {
                                                    &state.last_result_label
                                                },
                                                state.last_result
                                            ));
                                        }
                                    });
                                    if ui.button("编辑").clicked() {
                                        self.device_editor = DeviceEditor::from_device(device);
                                    }
                                });
                                ui.separator();
                            }
                        });
                });

                Self::panel_card_collapsible(ui, "device_editor_card", "设备编辑", |ui| {
                    ui.horizontal(|ui| {
                        ui.label("名称");
                        ui.text_edit_singleline(&mut self.device_editor.name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("设备ID");
                        let response = ui.text_edit_singleline(&mut self.device_editor.device_id);
                        if response.changed() && !self.device_editor.device_id.trim().is_empty() {
                            let (up, down) =
                                DeviceProfile::default_topics(self.device_editor.device_id.trim());
                            if self.device_editor.up_topic.is_empty() {
                                self.device_editor.up_topic = up;
                            }
                            if self.device_editor.down_topic.is_empty() {
                                self.device_editor.down_topic = down;
                            }
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("上行主题");
                        ui.text_edit_singleline(&mut self.device_editor.up_topic);
                    });
                    ui.horizontal(|ui| {
                        ui.label("下行主题");
                        ui.text_edit_singleline(&mut self.device_editor.down_topic);
                    });
                    ui.horizontal(|ui| {
                        ui.label("Mesh类型");
                        ui.add(
                            egui::DragValue::new(&mut self.device_editor.mesh_dev_type)
                                .range(0..=255),
                        );
                        ui.label("目标地址");
                        ui.add(
                            egui::DragValue::new(&mut self.device_editor.default_dest_addr)
                                .range(1..=65535),
                        );
                    });
                    ui.checkbox(&mut self.device_editor.subscribe_enabled, "订阅上行主题");
                    ui.horizontal(|ui| {
                        if Self::primary_button(ui, "保存设备").clicked() {
                            self.save_device_editor();
                        }
                        if Self::secondary_button(ui, "清空").clicked() {
                            self.device_editor = DeviceEditor {
                                mesh_dev_type: 1,
                                default_dest_addr: 1,
                                subscribe_enabled: true,
                                ..Default::default()
                            };
                        }
                        if Self::secondary_button(ui, "删除已选").clicked() {
                            self.config
                                .devices
                                .retain(|device| !self.selected_devices.contains(&device.local_id));
                            self.selected_devices.clear();
                            self.sync_subscriptions();
                            let _ = save_config(&self.config);
                        }
                    });
                });
            });

        egui::Panel::right("actions_panel")
            .resizable(true)
            .default_size(320.0)
            .min_size(280.0)
            .frame(Self::side_panel_frame(panel_style))
            .show_inside(ui, |ui| {
                Self::panel_card_collapsible(ui, "preset_commands", "预置命令", |ui| {
                    egui::ComboBox::from_label("命令")
                        .selected_text(
                            command_by_key(&self.command_key)
                                .map(|spec| spec.label)
                                .unwrap_or("未知命令"),
                        )
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

                    if let Some(spec) = command_by_key(&self.command_key) {
                        if spec.include_dest_addr {
                            ui.horizontal(|ui| {
                                ui.label("目标地址");
                                let entry = self
                                    .command_form
                                    .entry("dest_addr".into())
                                    .or_insert_with(|| "1".into());
                                ui.text_edit_singleline(entry);
                            });
                        }
                        for field in spec.fields {
                            ui.horizontal(|ui| {
                                ui.label(field.label);
                                let value = self
                                    .command_form
                                    .entry(field.key.to_string())
                                    .or_insert_with(|| field.default.to_string());
                                match field.kind {
                                    FieldKind::Text | FieldKind::Integer => {
                                        ui.text_edit_singleline(value);
                                    }
                                    FieldKind::Choice(choices) => {
                                        egui::ComboBox::from_id_salt(field.key)
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
                    }
                });

                Self::panel_card_collapsible(ui, "raw_json_card", "原始 JSON", |ui| {
                    ui.add(
                        TextEdit::multiline(&mut self.raw_json_text)
                            .font(egui::TextStyle::Monospace)
                            .desired_rows(6)
                            .desired_width(f32::INFINITY),
                    );
                    ui.horizontal(|ui| {
                        ui.label("期望应答");
                        ui.add(
                            TextEdit::singleline(&mut self.raw_expected_opcode)
                                .desired_width(90.0)
                                .hint_text("如 0x47"),
                        );
                    });
                    if Self::primary_button(ui, "发送原始 JSON").clicked() {
                        match serde_json::from_str::<Value>(&self.raw_json_text) {
                            Ok(payload @ Value::Object(_)) => {
                                if let Some(opcode) = payload.get("opcode") {
                                    let opcode_text = opcode.to_string().replace('"', "");
                                    if parse_opcode(&opcode_text).is_err() {
                                        self.system_notice =
                                            "opcode 必须是十进制整数或 0x 十六进制字符串".into();
                                    } else {
                                        let expected_override =
                                            if self.raw_expected_opcode.trim().is_empty() {
                                                None
                                            } else {
                                                match parse_opcode(&self.raw_expected_opcode) {
                                                    Ok(opcode) => Some(opcode),
                                                    Err(err) => {
                                                        self.system_notice =
                                                            format!("期望应答操作码无效: {err}");
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
                                            match self.normalize_raw_payload_for_device(
                                                payload.clone(),
                                                &device,
                                            ) {
                                                Ok(normalized) => items.push((
                                                    device,
                                                    normalized,
                                                    expected_override,
                                                )),
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
                                } else {
                                    self.system_notice = "原始 JSON 必须包含 opcode 字段".into();
                                }
                            }
                            Ok(_) => self.system_notice = "原始负载必须是 JSON 对象".into(),
                            Err(err) => self.system_notice = err.to_string(),
                        }
                    }
                });

                Self::panel_card_collapsible(ui, "transfer_preview_card", "传输预览", |ui| {
                    egui::ComboBox::from_label("传输类型")
                        .selected_text(self.transfer_kind.label())
                        .show_ui(ui, |ui| {
                            for kind in TransferKind::ALL {
                                ui.selectable_value(&mut self.transfer_kind, kind, kind.label());
                            }
                        });
                    ui.horizontal(|ui| {
                        ui.label("文件");
                        ui.text_edit_singleline(&mut self.transfer_file);
                        if Self::secondary_button(ui, "浏览").clicked() {
                            self.pick_transfer_file();
                        }
                    });
                    ui.horizontal(|ui| {
                        ui.label("版本");
                        ui.add(egui::DragValue::new(&mut self.transfer_version).range(0..=255));
                        ui.label("语音文件名");
                        ui.text_edit_singleline(&mut self.transfer_voice_name);
                    });
                    ui.horizontal(|ui| {
                        ui.label("包间隔(ms)");
                        ui.add(
                            egui::DragValue::new(&mut self.transfer_packet_delay_ms)
                                .range(1..=5_000),
                        );
                        ui.label("ACK超时(s)");
                        ui.add(
                            egui::DragValue::new(&mut self.transfer_ack_timeout_secs)
                                .range(1..=300),
                        );
                        ui.label("最大重试");
                        ui.add(egui::DragValue::new(&mut self.transfer_max_retries).range(0..=10));
                    });
                    if Self::primary_button(ui, "发送传输预览").clicked() {
                        self.send_transfer_preview();
                    }
                });
            });

        egui::CentralPanel::default().show_inside(ui, |ui| {
            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.heading("实时工作区");
                    ui.separator();
                    Self::stat_chip(ui, "已配置设备", self.config.devices.len().to_string());
                    Self::stat_chip(ui, "已选设备", self.selected_devices.len().to_string());
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
                    Self::stat_chip(ui, "总接收", total_rx.to_string());
                    Self::stat_chip(ui, "总发送", total_tx.to_string());
                    Self::stat_chip(ui, "待应答", self.pending_requests.len().to_string());
                    Self::stat_chip(ui, "传输中", self.active_transfers.len().to_string());
                    ui.checkbox(&mut self.show_selected_logs_only, "仅显示已选设备日志");
                });
            });

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.label(RichText::new("请求状态").heading());
                ui.separator();
                if self.pending_requests.is_empty() {
                    ui.label("当前没有待应答请求。");
                } else {
                    let now = Instant::now();
                    TableBuilder::new(ui)
                        .striped(true)
                        .column(Column::initial(120.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(90.0))
                        .column(Column::remainder())
                        .min_scrolled_height(110.0)
                        .header(24.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("设备");
                            });
                            header.col(|ui| {
                                ui.strong("请求");
                            });
                            header.col(|ui| {
                                ui.strong("等待");
                            });
                            header.col(|ui| {
                                ui.strong("时间戳");
                            });
                            header.col(|ui| {
                                ui.strong("主题");
                            });
                        })
                        .body(|mut body| {
                            for request in &self.pending_requests {
                                body.row(24.0, |mut row| {
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
                            }
                        });
                }
            });

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("传输状态").heading());
                    if self
                        .active_transfers
                        .iter()
                        .any(|transfer| transfer.paused && !transfer.terminal)
                        && Self::secondary_button(ui, "继续待传输").clicked()
                    {
                        let ids = self
                            .active_transfers
                            .iter()
                            .filter(|transfer| transfer.paused && !transfer.terminal)
                            .map(|transfer| transfer.device_local_id)
                            .collect::<Vec<_>>();
                        self.resume_transfers_for_devices(&ids);
                    }
                    if self
                        .active_transfers
                        .iter()
                        .any(|transfer| transfer.terminal && !transfer.succeeded)
                        && Self::secondary_button(ui, "重试失败").clicked()
                    {
                        let ids = self
                            .active_transfers
                            .iter()
                            .filter(|transfer| transfer.terminal && !transfer.succeeded)
                            .map(|transfer| transfer.device_local_id)
                            .collect::<Vec<_>>();
                        self.retry_failed_transfers_for_devices(&ids);
                    }
                    if self
                        .active_transfers
                        .iter()
                        .any(|transfer| transfer.terminal)
                        && Self::secondary_button(ui, "清除终态").clicked()
                    {
                        self.clear_terminal_transfers();
                    }
                    if !self.active_transfers.is_empty()
                        && Self::secondary_button(ui, "取消全部").clicked()
                    {
                        let ids = self
                            .active_transfers
                            .iter()
                            .map(|transfer| transfer.device_local_id)
                            .collect::<Vec<_>>();
                        self.cancel_transfers_for_devices(&ids);
                    }
                });
                ui.separator();
                if self.active_transfers.is_empty() {
                    ui.label("当前没有活跃传输。");
                } else {
                    let mut cancel_ids = Vec::new();
                    let mut resume_ids = Vec::new();
                    let mut retry_ids = Vec::new();
                    let mut clear_ids = Vec::new();
                    TableBuilder::new(ui)
                        .striped(true)
                        .column(Column::initial(120.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(90.0))
                        .column(Column::initial(180.0))
                        .column(Column::initial(70.0))
                        .column(Column::initial(70.0))
                        .column(Column::remainder())
                        .min_scrolled_height(110.0)
                        .header(24.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("设备");
                            });
                            header.col(|ui| {
                                ui.strong("类型");
                            });
                            header.col(|ui| {
                                ui.strong("进度");
                            });
                            header.col(|ui| {
                                ui.strong("等待ACK");
                            });
                            header.col(|ui| {
                                ui.strong("失败点");
                            });
                            header.col(|ui| {
                                ui.strong("失败原因");
                            });
                            header.col(|ui| {
                                ui.strong("重试");
                            });
                            header.col(|ui| {
                                ui.strong("操作");
                            });
                            header.col(|ui| {
                                ui.strong("状态");
                            });
                        })
                        .body(|mut body| {
                            for transfer in &self.active_transfers {
                                body.row(24.0, |mut row| {
                                    row.col(|ui| {
                                        ui.label(&transfer.device_name);
                                    });
                                    row.col(|ui| {
                                        ui.label(transfer.kind.label());
                                    });
                                    row.col(|ui| {
                                        ui.label(format!(
                                            "{}/{}",
                                            transfer.next_index,
                                            transfer.packets.len()
                                        ));
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
                                        ui.label(
                                            transfer
                                                .failure_packet_index
                                                .map(|index| format!("#{}", index + 1))
                                                .unwrap_or_else(|| "-".into()),
                                        );
                                    });
                                    row.col(|ui| {
                                        if transfer.last_failure_reason.is_empty() {
                                            ui.label("-");
                                        } else {
                                            ui.label(&transfer.last_failure_reason);
                                        }
                                    });
                                    row.col(|ui| {
                                        ui.label(format!(
                                            "{}/{}",
                                            transfer.retry_count, transfer.max_retries
                                        ));
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
                                        ui.label(&transfer.status);
                                    });
                                });
                            }
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
                        self.active_transfers
                            .retain(|transfer| !clear_ids.contains(&transfer.device_local_id));
                    }
                }
            });

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("最近操作").heading());
                    if !self.recent_operations.is_empty()
                        && Self::secondary_button(ui, "清空操作").clicked()
                    {
                        self.recent_operations.clear();
                    }
                });
                ui.separator();
                if self.recent_operations.is_empty() {
                    ui.label("当前没有最近操作记录。");
                } else {
                    TableBuilder::new(ui)
                        .striped(true)
                        .column(Column::initial(140.0))
                        .column(Column::initial(120.0))
                        .column(Column::initial(110.0))
                        .column(Column::initial(80.0))
                        .column(Column::initial(80.0))
                        .column(Column::remainder())
                        .min_scrolled_height(120.0)
                        .header(24.0, |mut header| {
                            header.col(|ui| {
                                ui.strong("时间");
                            });
                            header.col(|ui| {
                                ui.strong("设备");
                            });
                            header.col(|ui| {
                                ui.strong("操作");
                            });
                            header.col(|ui| {
                                ui.strong("状态");
                            });
                            header.col(|ui| {
                                ui.strong("RTT");
                            });
                            header.col(|ui| {
                                ui.strong("详情");
                            });
                        })
                        .body(|mut body| {
                            for op in self.recent_operations.iter().rev().take(20) {
                                body.row(24.0, |mut row| {
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
                            }
                        });
                }
            });

            let filtered_logs: Vec<(usize, LogEntry)> = self
                .selected_logs()
                .into_iter()
                .map(|(index, entry)| (index, entry.clone()))
                .collect();

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.label(RichText::new("消息日志").heading());
                ui.separator();
                ui.horizontal(|ui| {
                    ui.label("筛选");
                    ui.add(
                        TextEdit::singleline(&mut self.log_filter_text)
                            .desired_width(220.0)
                            .hint_text("设备 / opcode / 状态 / 主题"),
                    );
                    if Self::secondary_button(ui, "清空日志").clicked() {
                        self.logs.clear();
                        self.selected_log_index = None;
                    }
                });
                ui.separator();
                TableBuilder::new(ui)
                    .striped(true)
                    .column(Column::initial(140.0))
                    .column(Column::initial(70.0))
                    .column(Column::initial(120.0))
                    .column(Column::initial(90.0))
                    .column(Column::initial(90.0))
                    .column(Column::initial(160.0))
                    .column(Column::remainder())
                    .min_scrolled_height(320.0)
                    .header(24.0, |mut header| {
                        header.col(|ui| {
                            ui.strong("时间");
                        });
                        header.col(|ui| {
                            ui.strong("方向");
                        });
                        header.col(|ui| {
                            ui.strong("设备");
                        });
                        header.col(|ui| {
                            ui.strong("操作码");
                        });
                        header.col(|ui| {
                            ui.strong("状态");
                        });
                        header.col(|ui| {
                            ui.strong("主题");
                        });
                        header.col(|ui| {
                            ui.strong("摘要");
                        });
                    })
                    .body(|mut body| {
                        for (index, entry) in filtered_logs {
                            body.row(24.0, |mut row| {
                                row.col(|ui| {
                                    if ui
                                        .selectable_label(
                                            self.selected_log_index == Some(index),
                                            &entry.timestamp,
                                        )
                                        .clicked()
                                    {
                                        self.selected_log_index = Some(index);
                                    }
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
                            });
                        }
                    });
            });

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.label(RichText::new("解析详情").heading());
                ui.separator();
                if let Some(index) = self.selected_log_index {
                    if let Some(entry) = self.logs.get(index) {
                        if let Ok(payload) = serde_json::from_str::<Value>(&entry.payload) {
                            let details = decode_payload_details(&payload);
                            if details.is_empty() {
                                ui.label("当前负载暂无结构化解析。");
                            } else {
                                TableBuilder::new(ui)
                                    .striped(true)
                                    .column(Column::initial(160.0))
                                    .column(Column::remainder())
                                    .min_scrolled_height(120.0)
                                    .body(|mut body| {
                                        for (key, value) in details {
                                            body.row(24.0, |mut row| {
                                                row.col(|ui| {
                                                    ui.strong(key);
                                                });
                                                row.col(|ui| {
                                                    ui.label(value);
                                                });
                                            });
                                        }
                                    });
                            }
                        } else {
                            ui.label("当前负载不是可解析的 JSON。");
                        }
                    } else {
                        ui.label("未选择日志。");
                    }
                } else {
                    ui.label("未选择日志。");
                }
            });

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.label(RichText::new("当前负载").heading());
                ui.separator();
                if let Some(index) = self.selected_log_index {
                    if let Some(entry) = self.logs.get(index) {
                        let mut payload = entry.payload.clone();
                        ui.add(
                            TextEdit::multiline(&mut payload)
                                .font(egui::TextStyle::Monospace)
                                .desired_rows(12)
                                .desired_width(f32::INFINITY)
                                .interactive(false),
                        );
                    } else {
                        ui.label("未选择日志。");
                    }
                } else {
                    ui.label("未选择日志。");
                }
            });
        });
    }

    fn on_exit(&mut self) {
        self.config.broker = self.broker_editor.clone();
        self.config.transfer_packet_delay_ms = self.transfer_packet_delay_ms;
        self.config.transfer_ack_timeout_secs = self.transfer_ack_timeout_secs;
        self.config.transfer_max_retries = self.transfer_max_retries;
        let _ = save_config(&self.config);
        self.mqtt.disconnect();
    }
}

fn compact_transfer_payload_log(payload: &Value) -> String {
    let mut redacted = redact_json(payload);
    if let Some(object) = redacted.as_object_mut() {
        if let Some(value) = object.get_mut("value") {
            if let Some(text) = value.as_str() {
                let abbreviated = if text.len() > 32 {
                    format!("{}...(len={})", &text[..32], text.len())
                } else {
                    text.to_string()
                };
                *value = Value::String(abbreviated);
            }
        }
    }
    serde_json::to_string_pretty(&redacted).unwrap_or_else(|_| payload.to_string())
}

fn transfer_expected_ack_opcode(
    kind: TransferKind,
    opcode: u32,
    next_index: usize,
    packet_count: usize,
) -> Option<u32> {
    match kind {
        TransferKind::BcOta => (opcode == 0x40).then_some(0x41),
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

#[inline]
fn rgb(r: u8, g: u8, b: u8) -> egui::Color32 {
    egui::Color32::from_rgb(r, g, b)
}

#[inline]
fn stroke(color: egui::Color32) -> egui::Stroke {
    egui::Stroke::new(1.0_f32, color)
}

#[derive(Clone, Copy)]
struct VisualPalette {
    panel_fill: egui::Color32,
    window_fill: egui::Color32,
    faint_bg_color: egui::Color32,
    extreme_bg_color: egui::Color32,
    code_bg_color: egui::Color32,
    window_stroke: egui::Color32,
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
            panel_fill: rgb(14, 17, 22),
            window_fill: rgb(17, 20, 26),
            faint_bg_color: rgb(24, 30, 38),
            extreme_bg_color: rgb(8, 11, 16),
            code_bg_color: rgb(10, 20, 28),
            window_stroke: rgb(52, 70, 85),
            selection_bg_fill: rgb(22, 88, 120),
            selection_stroke: rgb(152, 226, 255),
            hyperlink_color: rgb(94, 204, 255),
            inactive_bg_fill: rgb(28, 35, 44),
            inactive_bg_stroke: rgb(52, 70, 85),
            hovered_bg_fill: rgb(34, 49, 61),
            hovered_bg_stroke: rgb(70, 113, 138),
            active_bg_fill: rgb(27, 84, 108),
            active_bg_stroke: rgb(86, 171, 211),
            open_bg_fill: rgb(33, 57, 73),
            open_bg_stroke: rgb(72, 122, 150),
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
    visuals.widgets.inactive.weak_bg_fill = palette.inactive_bg_fill;
    visuals.widgets.inactive.bg_stroke = stroke(palette.inactive_bg_stroke);
    visuals.widgets.hovered.weak_bg_fill = palette.hovered_bg_fill;
    visuals.widgets.hovered.bg_stroke = stroke(palette.hovered_bg_stroke);
    visuals.widgets.active.weak_bg_fill = palette.active_bg_fill;
    visuals.widgets.active.bg_stroke = stroke(palette.active_bg_stroke);
    visuals.widgets.open.weak_bg_fill = palette.open_bg_fill;
    visuals.widgets.open.bg_stroke = stroke(palette.open_bg_stroke);
}

impl MeshBcTesterApp {
    fn apply_visual_style(ctx: &egui::Context) {
        ctx.global_style_mut(|style| {
            style.spacing.item_spacing = egui::vec2(8.0, 5.0);
            style.spacing.button_padding = egui::vec2(9.0, 3.0);
            style.spacing.interact_size = egui::vec2(44.0, 26.0);
            style.spacing.slider_width = 160.0;
            style.spacing.combo_width = 180.0;
            style.spacing.indent = 14.0;

            style.text_styles.insert(
                egui::TextStyle::Heading,
                egui::FontId::new(18.0, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Body,
                egui::FontId::new(13.0, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Button,
                egui::FontId::new(13.0, egui::FontFamily::Proportional),
            );
            style.text_styles.insert(
                egui::TextStyle::Monospace,
                egui::FontId::new(12.5, egui::FontFamily::Monospace),
            );
            style.text_styles.insert(
                egui::TextStyle::Small,
                egui::FontId::new(11.5, egui::FontFamily::Proportional),
            );

            let visuals = &mut style.visuals;
            *visuals = egui::Visuals::dark();
            let palette = VisualPalette::dark();
            apply_visual_palette(visuals, palette);
            let radius = egui::CornerRadius::same(5);
            visuals.widgets.noninteractive.corner_radius = radius;
            visuals.widgets.inactive.corner_radius = radius;
            visuals.widgets.hovered.corner_radius = radius;
            visuals.widgets.active.corner_radius = radius;
            visuals.widgets.open.corner_radius = radius;
        });
    }

    fn side_panel_frame(style: &egui::Style) -> egui::Frame {
        egui::Frame::side_top_panel(style)
            .inner_margin(egui::Margin::symmetric(10, 10))
            .fill(style.visuals.panel_fill)
            .stroke(egui::Stroke::new(
                1.0_f32,
                style.visuals.widgets.noninteractive.bg_stroke.color,
            ))
    }

    fn top_bottom_panel_frame(style: &egui::Style) -> egui::Frame {
        egui::Frame::side_top_panel(style)
            .inner_margin(egui::Margin::symmetric(10, 6))
            .fill(style.visuals.panel_fill)
            .stroke(egui::Stroke::new(
                1.0_f32,
                style.visuals.widgets.noninteractive.bg_stroke.color,
            ))
    }

    fn panel_card_frame(ui: &egui::Ui) -> egui::Frame {
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(10, 8))
            .outer_margin(egui::Margin::symmetric(0, 4))
            .corner_radius(egui::CornerRadius::same(5))
            .fill(ui.visuals().faint_bg_color)
            .stroke(egui::Stroke::new(
                1.0_f32,
                ui.visuals().widgets.noninteractive.bg_stroke.color,
            ))
    }

    fn panel_card_collapsible(
        ui: &mut egui::Ui,
        id_salt: impl std::hash::Hash,
        title: impl Into<egui::WidgetText>,
        add_body: impl FnOnce(&mut egui::Ui),
    ) {
        let title = title.into().heading();
        Self::panel_card_frame(ui).show(ui, |ui| {
            egui::CollapsingHeader::new(title)
                .id_salt(id_salt)
                .default_open(true)
                .show(ui, |ui| {
                    add_body(ui);
                });
        });
    }

    fn primary_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
        ui.add(
            egui::Button::new(text)
                .fill(rgb(20, 94, 128))
                .stroke(stroke(rgb(98, 199, 232))),
        )
    }

    fn secondary_button(ui: &mut egui::Ui, text: &str) -> egui::Response {
        ui.add(
            egui::Button::new(text)
                .fill(rgb(28, 35, 44))
                .stroke(stroke(rgb(52, 70, 85))),
        )
    }

    fn stat_chip(ui: &mut egui::Ui, label: &str, value: String) {
        egui::Frame::group(ui.style())
            .inner_margin(egui::Margin::symmetric(8, 4))
            .fill(ui.visuals().extreme_bg_color)
            .stroke(stroke(ui.visuals().widgets.noninteractive.bg_stroke.color))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.small(label);
                    ui.separator();
                    ui.label(RichText::new(value).strong());
                });
            });
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

fn configure_chinese_fonts(ctx: &egui::Context) {
    let mut fonts = FontDefinitions::default();

    if let Some((name, font_data)) = load_chinese_font() {
        fonts
            .font_data
            .insert(name.clone().into(), std::sync::Arc::new(font_data));
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .push(name.clone().into());
        fonts
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .push(name.into());
    }

    ctx.set_fonts(fonts);
}

fn load_chinese_font() -> Option<(String, FontData)> {
    let candidates: &[(&str, u32)] = &[
        ("assets/fonts/NotoSansSC-Regular.otf", 0),
        ("assets/fonts/SourceHanSansCN-Regular.otf", 0),
        ("assets/fonts/SourceHanSansSC-Regular.otf", 0),
        ("/System/Library/Fonts/Hiragino Sans GB.ttc", 0),
        ("/System/Library/Fonts/STHeiti Medium.ttc", 0),
        ("/System/Library/Fonts/STHeiti Light.ttc", 0),
        ("/System/Library/Fonts/PingFang.ttc", 0),
        ("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc", 0),
        (
            "/usr/share/fonts/opentype/noto/NotoSansCJKSC-Regular.otf",
            0,
        ),
        ("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc", 0),
        ("C:/Windows/Fonts/msyh.ttc", 0),
        ("C:/Windows/Fonts/msyhbd.ttc", 0),
        ("C:/Windows/Fonts/simhei.ttf", 0),
        ("C:/Windows/Fonts/simsun.ttc", 0),
        ("C:/Windows/Fonts/Deng.ttf", 0),
    ];

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
                return Some(("zh_cn_ui".into(), data));
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

    fn sample_transfer() -> ActiveTransfer {
        ActiveTransfer {
            device_local_id: 1,
            device_name: "设备A".into(),
            device_id: "dev-a".into(),
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
