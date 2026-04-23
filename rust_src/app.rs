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
    COMMANDS, FieldKind, build_command_payload, build_transfer_packets, command_by_key,
    current_time_stamp, expected_response_opcode, parse_opcode, redact_json, summarize_payload,
    transfer_preview,
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
    show_selected_logs_only: bool,
    transfer_kind: TransferKind,
    transfer_file: String,
    transfer_version: u8,
    transfer_voice_name: String,
    system_notice: String,
    pending_requests: Vec<PendingRequest>,
    active_transfers: Vec<ActiveTransfer>,
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
    status: String,
}

const TRANSFER_PACKET_DELAY_MS: u64 = 15;
const TRANSFER_ACK_TIMEOUT_SECS: u64 = 10;
const MAX_TRANSFER_BYTES: usize = 2 * 1024 * 1024;
const MAX_TRANSFER_PACKETS: usize = 6000;
const MAX_TRANSFER_TOTAL_PUBLISHES: usize = 12000;

impl MeshBcTesterApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        configure_chinese_fonts(&cc.egui_ctx);
        let mut config = load_config();
        if config.next_device_id == 0 {
            config.next_device_id = 1;
        }
        let broker_editor = config.broker.clone();
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
            show_selected_logs_only: false,
            transfer_kind: TransferKind::BcOta,
            transfer_file: String::new(),
            transfer_version: 1,
            transfer_voice_name: "voice.adpcm".into(),
            system_notice: String::new(),
            pending_requests: Vec::new(),
            active_transfers: Vec::new(),
        }
    }

    fn append_log(&mut self, entry: LogEntry) {
        self.logs.push(entry);
        if self.logs.len() > 2000 {
            self.logs.drain(0..self.logs.len().saturating_sub(2000));
        }
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
            .find(|device| topic == device.up_topic || topic == device.down_topic)
    }

    fn process_mqtt_events(&mut self) {
        self.collect_pending_timeouts();
        self.collect_transfer_timeouts();
        while let Ok(event) = self.mqtt.events_rx.try_recv() {
            match event {
                MqttEvent::Connection { message } => {
                    self.connection_status = message.clone();
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
                    if let (Some(device), Some(parsed)) = (&device, &parsed) {
                        self.resolve_transfer_ack(device, parsed);
                        self.resolve_pending_request(device, parsed);
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
                        state.last_opcode = opcode.clone();
                        state.last_summary = summary.clone();
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
                        status: "接收".into(),
                        summary,
                        payload: display_payload,
                    });
                }
            }
        }
    }

    fn connect(&mut self) {
        self.config.broker = self.broker_editor.clone();
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
        state.last_result = format!("已发送({source})");
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
                        state.last_result = format!("等待应答(0x{:02X})", expected_opcode);
                    } else {
                        state.last_result = format!("已发送({source}, 未跟踪应答)");
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
            opcode,
            status: source.to_uppercase(),
            summary: summarize_payload(&redacted),
            payload: logged_payload,
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
        let position = self.pending_requests.iter().position(|request| {
            request.device_local_id == device.local_id
                && request.expected_opcode == opcode
                && request
                    .time_stamp
                    .zip(response_time_stamp)
                    .map(|(expected, actual)| expected == actual)
                    .unwrap_or(false)
        });

        let Some(position) = position else {
            return;
        };
        let request = self.pending_requests.remove(position);
        let state = self.runtime_states.entry(device.local_id).or_default();
        state.pending_count = state.pending_count.saturating_sub(1);
        let value_text = payload
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("ACK");
        let status = if value_text.contains("Error") || value_text.contains("失败") {
            "错误"
        } else {
            "ACK"
        };
        state.last_result = format!(
            "{} 0x{:02X} -> 0x{:02X}",
            status, request.opcode, request.expected_opcode
        );
        self.append_log(LogEntry {
            timestamp: now_display(),
            direction: LogDirection::System,
            device_name: request.device_name,
            device_id: request.device_id,
            topic: request.topic,
            opcode: format!("0x{:02X}", request.expected_opcode),
            status: status.into(),
            summary: summarize_payload(&redact_json(payload)),
            payload: serde_json::to_string_pretty(&redact_json(payload))
                .unwrap_or_else(|_| payload.to_string()),
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
                state.last_result = format!(
                    "超时 0x{:02X} -> 0x{:02X}",
                    request.opcode, request.expected_opcode
                );
            }
            self.append_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: request.device_name,
                device_id: request.device_id,
                topic: request.topic,
                opcode: format!("0x{:02X}", request.expected_opcode),
                status: "超时".into(),
                summary: "等待应答超时(10秒)".into(),
                payload: String::new(),
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
                    return true;
                }
                self.config.devices.iter().any(|device| {
                    self.selected_devices.contains(&device.local_id)
                        && device.device_id == entry.device_id
                })
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
        let total_publishes = packets.len().saturating_mul(devices.len());
        if total_publishes > MAX_TRANSFER_TOTAL_PUBLISHES {
            self.system_notice = format!(
                "本次传输总发送量过大：{}，当前上限 {}。请减少设备数量或文件大小。",
                total_publishes, MAX_TRANSFER_TOTAL_PUBLISHES
            );
            return;
        }
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
                self.transfer_kind.label(),
                bytes.len(),
                packets.len()
            ),
            payload: serde_json::to_string_pretty(&redact_json(&preview))
                .unwrap_or_else(|_| preview.to_string()),
        });
        for device in devices {
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
                status: "已排队".into(),
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
                continue;
            };
            let packet = self.active_transfers[index].packets
                [self.active_transfers[index].next_index]
                .clone();
            let opcode = packet.get("opcode").and_then(Value::as_u64).unwrap_or(0) as u32;
            match self.send_payload_to_device(device, &packet, "transfer", None) {
                Ok(()) => {
                    self.active_transfers[index].next_index += 1;
                    self.active_transfers[index].status = format!(
                        "发送中 {}/{}",
                        self.active_transfers[index].next_index,
                        self.active_transfers[index].packets.len()
                    );
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
                    } else {
                        self.active_transfers[index].next_send_at =
                            now + Duration::from_millis(TRANSFER_PACKET_DELAY_MS);
                    }
                }
                Err(err) => {
                    self.active_transfers[index].status = format!("发送失败: {err}");
                    self.system_notice = err;
                }
            }
        }

        let mut completed = Vec::new();
        for (index, transfer) in self.active_transfers.iter().enumerate() {
            if transfer.next_index >= transfer.packets.len()
                && transfer.waiting_ack_opcode.is_none()
            {
                completed.push(index);
            }
        }
        for index in completed.into_iter().rev() {
            let transfer = self.active_transfers.remove(index);
            self.append_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: transfer.device_name,
                device_id: transfer.device_id,
                topic: transfer.down_topic,
                opcode: "-".into(),
                status: "完成".into(),
                summary: format!("{} 传输完成", transfer.kind.label()),
                payload: String::new(),
            });
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
            transfer.device_local_id == device.local_id
                && transfer.waiting_ack_opcode == Some(opcode)
        });
        let Some(position) = position else {
            return;
        };
        let transfer = &mut self.active_transfers[position];
        if matches!(opcode, 0x41 | 0x44) {
            let consent = payload.get("value").and_then(Value::as_u64).unwrap_or(0);
            if consent != 1 {
                transfer.status = "设备拒绝继续传输".into();
                self.append_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::System,
                    device_name: transfer.device_name.clone(),
                    device_id: transfer.device_id.clone(),
                    topic: transfer.down_topic.clone(),
                    opcode: format!("0x{:02X}", opcode),
                    status: "拒绝".into(),
                    summary: "设备未同意继续传输".into(),
                    payload: serde_json::to_string_pretty(&redact_json(payload))
                        .unwrap_or_else(|_| payload.to_string()),
                });
                transfer.next_index = transfer.packets.len();
                transfer.waiting_ack_opcode = None;
                transfer.waiting_since = None;
                return;
            }
        }
        transfer.waiting_ack_opcode = None;
        transfer.waiting_since = None;
        transfer.next_send_at = Instant::now() + Duration::from_millis(TRANSFER_PACKET_DELAY_MS);
        transfer.status = "收到ACK".into();
    }

    fn collect_transfer_timeouts(&mut self) {
        let timeout = Duration::from_secs(TRANSFER_ACK_TIMEOUT_SECS);
        for transfer in &mut self.active_transfers {
            if let (Some(expected_ack), Some(waiting_since)) =
                (transfer.waiting_ack_opcode, transfer.waiting_since)
            {
                if Instant::now().duration_since(waiting_since) >= timeout {
                    transfer.status = format!("ACK超时 0x{:02X}", expected_ack);
                    transfer.next_index = transfer.packets.len();
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                }
            }
        }
    }
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
                    ui.label(format!("状态: {}", self.connection_status));
                });
                if !self.system_notice.is_empty() {
                    ui.colored_label(egui::Color32::YELLOW, &self.system_notice);
                }
            });

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
                                        if !state.last_result.is_empty() {
                                            ui.small(format!("结果: {}", state.last_result));
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
                            for device in devices {
                                match build_command_payload(spec, &device, &self.command_form) {
                                    Ok(payload) => match self
                                        .send_payload_to_device(&device, &payload, "preset", None)
                                    {
                                        Ok(()) => {}
                                        Err(err) => {
                                            self.system_notice = err;
                                            return;
                                        }
                                    },
                                    Err(err) => self.system_notice = err,
                                }
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
                                        for device in devices {
                                            match self.normalize_raw_payload_for_device(
                                                payload.clone(),
                                                &device,
                                            ) {
                                                Ok(normalized) => {
                                                    if let Err(err) = self.send_payload_to_device(
                                                        &device,
                                                        &normalized,
                                                        "raw",
                                                        expected_override,
                                                    ) {
                                                        self.system_notice = err;
                                                        return;
                                                    }
                                                }
                                                Err(err) => {
                                                    self.system_notice = err;
                                                    return;
                                                }
                                            }
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
                    ui.checkbox(&mut self.show_selected_logs_only, "仅显示已选设备日志");
                });
            });

            let filtered_logs: Vec<(usize, LogEntry)> = self
                .selected_logs()
                .into_iter()
                .map(|(index, entry)| (index, entry.clone()))
                .collect();

            Self::panel_card_frame(ui).show(ui, |ui| {
                ui.label(RichText::new("消息日志").heading());
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
        let _ = save_config(&self.config);
        self.mqtt.disconnect();
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
