use std::collections::BTreeSet;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rumqttc::{Client, QoS};
use serde_json::Value;

use crate::models::{
    ActiveTransfer, DeviceProfile, LogDirection, LogEntry, OperationRecord, TransferKind,
    TransferSnapshot, now_display,
};
use crate::mqtt::MqttEvent;
use crate::protocol::{
    classify_execution_result, redact_json, response_can_omit_timestamp, summarize_payload,
};

const TRANSFER_ENGINE_TICK_MS: u64 = 5;
const TRANSFER_PACKET_LOG_INTERVAL: usize = 100;

#[derive(Clone, Copy, Debug)]
pub struct TransferEngineSettings {
    pub packet_delay_ms: u64,
    pub ack_timeout_secs: u64,
    pub bc_ota_start_ack_timeout_secs: u64,
    pub max_retries: u8,
}

pub enum TransferEngineCommand {
    SetPublisher(Option<Client>),
    QueueTransfer {
        devices: Vec<DeviceProfile>,
        packets: Vec<Value>,
        kind: TransferKind,
        settings: TransferEngineSettings,
    },
    CancelDevices(Vec<u64>),
    RetryDevices(Vec<u64>),
    ResumeDevices(Vec<u64>),
    ClearTerminalDevices(Vec<u64>),
    ClearTerminal,
    Shutdown,
}

#[derive(Debug)]
pub enum TransferEngineEvent {
    Snapshot(Vec<TransferSnapshot>),
    Log(LogEntry),
    Operation(OperationRecord),
    DeviceResult {
        device_local_id: u64,
        label: String,
        text: String,
    },
    SystemNotice(String),
}

pub struct TransferEngine {
    command_tx: Sender<TransferEngineCommand>,
    mqtt_tx: Sender<MqttEvent>,
    events_rx: Receiver<TransferEngineEvent>,
    worker: Option<JoinHandle<()>>,
}

impl Default for TransferEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TransferEngine {
    pub fn new() -> Self {
        let (command_tx, command_rx) = mpsc::channel();
        let (mqtt_tx, mqtt_rx) = mpsc::channel();
        let (events_tx, events_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            TransferEngineWorker::new(command_rx, mqtt_rx, events_tx).run();
        });

        Self {
            command_tx,
            mqtt_tx,
            events_rx,
            worker: Some(worker),
        }
    }

    pub fn mqtt_event_sender(&self) -> Sender<MqttEvent> {
        self.mqtt_tx.clone()
    }

    pub fn send(&self, command: TransferEngineCommand) {
        let _ = self.command_tx.send(command);
    }

    pub fn try_recv_event(&self) -> Result<TransferEngineEvent, TryRecvError> {
        self.events_rx.try_recv()
    }
}

impl Drop for TransferEngine {
    fn drop(&mut self) {
        let _ = self.command_tx.send(TransferEngineCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct TransferEngineWorker {
    command_rx: Receiver<TransferEngineCommand>,
    mqtt_rx: Receiver<MqttEvent>,
    events_tx: Sender<TransferEngineEvent>,
    active_transfers: Vec<ActiveTransfer>,
    publisher: Option<Client>,
    current_generation: Option<u64>,
    packet_delay_ms: u64,
    ack_timeout_secs: u64,
    bc_ota_start_ack_timeout_secs: u64,
}

impl TransferEngineWorker {
    fn new(
        command_rx: Receiver<TransferEngineCommand>,
        mqtt_rx: Receiver<MqttEvent>,
        events_tx: Sender<TransferEngineEvent>,
    ) -> Self {
        Self {
            command_rx,
            mqtt_rx,
            events_tx,
            active_transfers: Vec::new(),
            publisher: None,
            current_generation: None,
            packet_delay_ms: 15,
            ack_timeout_secs: 10,
            bc_ota_start_ack_timeout_secs: 20,
        }
    }

    fn run(&mut self) {
        loop {
            self.drain_mqtt_events();
            self.collect_transfer_timeouts();
            self.send_due_packets();
            self.finish_ready_transfers();

            match self
                .command_rx
                .recv_timeout(Duration::from_millis(TRANSFER_ENGINE_TICK_MS))
            {
                Ok(TransferEngineCommand::Shutdown) => break,
                Ok(command) => self.handle_command(command),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }

            while let Ok(command) = self.command_rx.try_recv() {
                if matches!(command, TransferEngineCommand::Shutdown) {
                    return;
                }
                self.handle_command(command);
            }
        }
    }

    fn handle_command(&mut self, command: TransferEngineCommand) {
        match command {
            TransferEngineCommand::SetPublisher(publisher) => {
                self.publisher = publisher;
            }
            TransferEngineCommand::QueueTransfer {
                devices,
                packets,
                kind,
                settings,
            } => {
                self.packet_delay_ms = settings.packet_delay_ms;
                self.ack_timeout_secs = settings.ack_timeout_secs;
                self.bc_ota_start_ack_timeout_secs = settings.bc_ota_start_ack_timeout_secs;
                self.queue_transfer(devices, packets, kind, settings.max_retries);
            }
            TransferEngineCommand::CancelDevices(device_ids) => {
                self.cancel_transfers_for_devices(&device_ids);
            }
            TransferEngineCommand::RetryDevices(device_ids) => {
                self.retry_failed_transfers_for_devices(&device_ids);
            }
            TransferEngineCommand::ResumeDevices(device_ids) => {
                self.resume_transfers_for_devices(&device_ids);
            }
            TransferEngineCommand::ClearTerminalDevices(device_ids) => {
                self.clear_terminal_transfers_for_devices(&device_ids);
            }
            TransferEngineCommand::ClearTerminal => {
                self.active_transfers.retain(|transfer| !transfer.terminal);
                self.emit_snapshot();
            }
            TransferEngineCommand::Shutdown => {}
        }
    }

    fn queue_transfer(
        &mut self,
        devices: Vec<DeviceProfile>,
        packets: Vec<Value>,
        kind: TransferKind,
        max_retries: u8,
    ) {
        for device in devices {
            if self
                .active_transfers
                .iter()
                .any(|transfer| transfer.device_local_id == device.local_id && !transfer.terminal)
            {
                self.emit_log(LogEntry {
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

            self.emit_device_result(
                device.local_id,
                "传输",
                format!(
                    "{} 已排队，共{}包",
                    kind.label(),
                    transfer_display_total_packets(kind, packets.len())
                ),
            );
            self.active_transfers.push(ActiveTransfer {
                device_local_id: device.local_id,
                device_name: device.name.clone(),
                device_id: device.device_id.clone(),
                up_topic: device.up_topic.clone(),
                down_topic: device.down_topic.clone(),
                kind,
                packets: packets.clone(),
                next_index: 0,
                next_send_at: Instant::now(),
                waiting_ack_opcode: None,
                waiting_since: None,
                last_sent_index: None,
                last_sent_time_stamp: None,
                retry_count: 0,
                max_retries,
                status: "已排队".into(),
                terminal: false,
                succeeded: false,
                paused: false,
                failure_packet_index: None,
                last_failure_reason: String::new(),
            });
        }
        self.emit_snapshot();
    }

    fn drain_mqtt_events(&mut self) {
        while let Ok(event) = self.mqtt_rx.try_recv() {
            self.handle_mqtt_event(event);
        }
    }

    fn handle_mqtt_event(&mut self, event: MqttEvent) {
        match event {
            MqttEvent::Connection {
                generation,
                connected,
                reconnecting,
                ..
            } => {
                if self
                    .current_generation
                    .map(|current| generation < current)
                    .unwrap_or(false)
                {
                    return;
                }
                self.current_generation = Some(generation);
                if connected {
                    self.resume_transfers_after_reconnect();
                } else {
                    self.pause_transfers_for_connection_loss(reconnecting);
                }
            }
            MqttEvent::Message {
                generation,
                topic,
                payload,
            } => {
                if self.current_generation != Some(generation) {
                    return;
                }
                let Ok(payload) = serde_json::from_str::<Value>(&payload) else {
                    return;
                };
                self.resolve_transfer_ack(&topic, &payload);
            }
        }
    }

    fn send_due_packets(&mut self) {
        let now = Instant::now();
        let mut changed = false;
        for index in 0..self.active_transfers.len() {
            if self.active_transfers[index].terminal
                || self.active_transfers[index].paused
                || self.active_transfers[index].waiting_ack_opcode.is_some()
                || now < self.active_transfers[index].next_send_at
                || self.active_transfers[index].next_index
                    >= self.active_transfers[index].packets.len()
            {
                continue;
            }

            let packet_index = self.active_transfers[index].next_index;
            let packet_count = self.active_transfers[index].packets.len();
            let packet = self.active_transfers[index].packets[packet_index].clone();
            let down_topic = self.active_transfers[index].down_topic.clone();
            let opcode = packet.get("opcode").and_then(Value::as_u64).unwrap_or(0) as u32;
            let publish_result = self.publish_json(&down_topic, &packet);

            match publish_result {
                Ok(()) => {
                    if should_log_transfer_packet(packet_index, packet_count) {
                        self.emit_transfer_packet_log(index, opcode, &packet);
                    }
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
                    self.emit_device_result(
                        self.active_transfers[index].device_local_id,
                        "传输",
                        format!(
                            "{} 发送中 {}/{}",
                            self.active_transfers[index].kind.label(),
                            display_completed,
                            display_total
                        ),
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
                        self.emit_device_result(
                            self.active_transfers[index].device_local_id,
                            "待应答",
                            format!("传输等待ACK 0x{:02X}", expected_ack),
                        );
                    } else {
                        self.active_transfers[index].next_send_at =
                            now + Duration::from_millis(self.packet_delay_ms);
                    }
                    changed = true;
                }
                Err(err) => {
                    self.retry_or_fail_transfer(index, packet_index, format!("发送失败: {err}"));
                    self.emit_device_result(
                        self.active_transfers[index].device_local_id,
                        "错误",
                        format!("传输发送失败: {err}"),
                    );
                    self.emit_notice(err);
                    changed = true;
                }
            }
        }
        if changed {
            self.emit_snapshot();
        }
    }

    fn publish_json(&self, topic: &str, payload: &Value) -> Result<(), String> {
        let client = self
            .publisher
            .as_ref()
            .ok_or_else(|| "MQTT 客户端未连接".to_string())?;
        client
            .publish(
                topic,
                QoS::AtMostOnce,
                false,
                payload.to_string().into_bytes(),
            )
            .map_err(|err| err.to_string())
    }

    fn emit_transfer_packet_log(&self, index: usize, opcode: u32, payload: &Value) {
        let transfer = &self.active_transfers[index];
        let redacted = redact_json(payload);
        self.emit_log(LogEntry {
            timestamp: now_display(),
            direction: LogDirection::Tx,
            device_name: transfer.device_name.clone(),
            device_id: transfer.device_id.clone(),
            topic: transfer.down_topic.clone(),
            opcode: format!("0x{opcode:02X}"),
            status: "TRANSFER".into(),
            summary: summarize_payload(&redacted),
            payload: compact_transfer_payload_log(&redacted),
        });
    }

    fn resolve_transfer_ack(&mut self, topic: &str, payload: &Value) {
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
            topic_matches_device_up_topic(&transfer.up_topic, topic)
                && transfer.waiting_ack_opcode == Some(opcode)
                && timestamp_matches
        });
        let Some(position) = position else {
            return;
        };

        if matches!(self.active_transfers[position].kind, TransferKind::BcOta) && opcode == 0x41 {
            self.resolve_bc_ota_ack(position, payload);
            self.emit_snapshot();
            return;
        }

        if let Some((status, summary)) = classify_execution_result(payload)
            && status == "错误"
        {
            let transfer = &mut self.active_transfers[position];
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
            self.emit_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name,
                device_id,
                topic: down_topic,
                opcode: format!("0x{:02X}", opcode),
                status: "错误".into(),
                summary,
                payload: serde_json::to_string_pretty(&redact_json(payload))
                    .unwrap_or_else(|_| payload.to_string()),
            });
            self.emit_operation(OperationRecord {
                timestamp: now_display(),
                device_name: op_device_name,
                opcode: format!("0x{:02X}", opcode),
                status: "错误".into(),
                detail: op_summary,
                rtt_ms: String::new(),
            });
            self.emit_snapshot();
            return;
        }

        if matches!(opcode, 0x41 | 0x44) {
            let consent = payload.get("value").and_then(Value::as_u64).unwrap_or(0);
            if consent != 1 {
                let (device_local_id, device_name, device_id, down_topic, op_device_name) = {
                    let transfer = &mut self.active_transfers[position];
                    let device_name = transfer.device_name.clone();
                    let op_device_name = device_name.clone();
                    transfer.status = "设备拒绝继续传输".into();
                    transfer.next_index = transfer.packets.len();
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.terminal = true;
                    transfer.succeeded = false;
                    transfer.failure_packet_index = transfer.last_sent_index;
                    transfer.last_failure_reason = "设备未同意继续传输".into();
                    (
                        transfer.device_local_id,
                        device_name,
                        transfer.device_id.clone(),
                        transfer.down_topic.clone(),
                        op_device_name,
                    )
                };
                self.emit_device_result(device_local_id, "拒绝", "设备未同意继续传输");
                self.emit_log(LogEntry {
                    timestamp: now_display(),
                    direction: LogDirection::System,
                    device_name,
                    device_id,
                    topic: down_topic,
                    opcode: format!("0x{:02X}", opcode),
                    status: "拒绝".into(),
                    summary: "设备未同意继续传输".into(),
                    payload: serde_json::to_string_pretty(&redact_json(payload))
                        .unwrap_or_else(|_| payload.to_string()),
                });
                self.emit_operation(OperationRecord {
                    timestamp: now_display(),
                    device_name: op_device_name,
                    opcode: format!("0x{:02X}", opcode),
                    status: "拒绝".into(),
                    detail: "传输已拒绝".into(),
                    rtt_ms: String::new(),
                });
                self.emit_snapshot();
                return;
            }
        }

        let (device_local_id, label, text) = {
            let transfer = &mut self.active_transfers[position];
            transfer.waiting_ack_opcode = None;
            transfer.waiting_since = None;
            transfer.paused = false;
            transfer.next_send_at = Instant::now() + Duration::from_millis(self.packet_delay_ms);
            if matches!(opcode, 0x41 | 0x44) {
                transfer.status = "已获同意，继续发送".into();
                (transfer.device_local_id, "传输", "已获同意，继续发送")
            } else {
                transfer.status = "收到ACK".into();
                (transfer.device_local_id, "成功", "收到传输ACK")
            }
        };
        self.emit_device_result(device_local_id, label, text);
        self.emit_snapshot();
    }

    fn resolve_bc_ota_ack(&mut self, position: usize, payload: &Value) {
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
                self.emit_device_result(
                    transfer.device_local_id,
                    if transfer.next_index >= transfer.packets.len() {
                        "失败"
                    } else {
                        "重试"
                    },
                    transfer.status.clone(),
                );
            }
            1 | 3 if last_packet_sent => {
                let device_local_id = {
                    let transfer = &mut self.active_transfers[position];
                    transfer.paused = false;
                    transfer.waiting_since = Some(now);
                    transfer.status = "最后包已确认，等待升级结果".into();
                    transfer.device_local_id
                };
                self.emit_device_result(device_local_id, "传输", "最后包已确认，等待升级结果");
            }
            1 | 3 => {
                let device_local_id = {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA ACK成功，继续发送".into();
                    transfer.device_local_id
                };
                self.emit_device_result(device_local_id, "传输", "BC OTA ACK成功，继续发送");
            }
            5 if last_packet_sent => {
                let device_local_id = {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA升级成功".into();
                    transfer.device_local_id
                };
                self.emit_device_result(device_local_id, "成功", "BC OTA升级成功");
            }
            4 if last_packet_sent => {
                let device_local_id = {
                    let transfer = &mut self.active_transfers[position];
                    transfer.waiting_ack_opcode = None;
                    transfer.waiting_since = None;
                    transfer.paused = false;
                    transfer.next_send_at = now;
                    transfer.status = "BC OTA升级失败".into();
                    transfer.failure_packet_index = transfer.last_sent_index;
                    transfer.last_failure_reason = "BC OTA升级失败".into();
                    transfer.device_local_id
                };
                self.emit_device_result(device_local_id, "失败", "BC OTA升级失败");
            }
            _ => {
                let reason = format!("BC OTA ACK失败，异常值 {}", ack_value);
                self.retry_or_fail_transfer(position, retry_from, reason);
                let transfer = &self.active_transfers[position];
                self.emit_device_result(
                    transfer.device_local_id,
                    if transfer.next_index >= transfer.packets.len() {
                        "失败"
                    } else {
                        "重试"
                    },
                    transfer.status.clone(),
                );
            }
        }
    }

    fn transfer_ack_timeout_for(&self, transfer: &ActiveTransfer) -> Duration {
        let timeout_secs = if transfer.kind == TransferKind::BcOta
            && transfer.waiting_ack_opcode == Some(0x41)
            && transfer.last_sent_index == Some(0)
        {
            self.bc_ota_start_ack_timeout_secs
        } else {
            self.ack_timeout_secs
        };
        Duration::from_secs(timeout_secs)
    }

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
            let retry_from = self.active_transfers[index]
                .last_sent_index
                .unwrap_or(self.active_transfers[index].next_index.saturating_sub(1));
            self.retry_or_fail_transfer(
                index,
                retry_from,
                format!("ACK超时 0x{:02X}", expected_ack),
            );
            self.emit_device_result(
                self.active_transfers[index].device_local_id,
                "超时",
                format!("传输ACK超时 0x{:02X}", expected_ack),
            );
            self.emit_snapshot();
        }
    }

    fn retry_or_fail_transfer(&mut self, index: usize, retry_packet_index: usize, reason: String) {
        let transfer = &mut self.active_transfers[index];
        apply_transfer_retry_state(transfer, retry_packet_index, reason, self.packet_delay_ms);
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
                    transfer.device_local_id,
                    transfer.device_name.clone(),
                    transfer.device_id.clone(),
                    transfer.down_topic.clone(),
                    transfer.kind.label().to_string(),
                ));
            }
        }
        for (device_local_id, device_name, device_id, down_topic, kind_label) in cancelled {
            self.emit_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: device_name.clone(),
                device_id,
                topic: down_topic,
                opcode: "-".into(),
                status: "取消".into(),
                summary: format!("{kind_label} 传输已取消"),
                payload: String::new(),
            });
            self.emit_operation(OperationRecord {
                timestamp: now_display(),
                device_name,
                opcode: kind_label.clone(),
                status: "取消".into(),
                detail: "传输已取消".into(),
                rtt_ms: String::new(),
            });
            self.emit_device_result(device_local_id, "取消", format!("{kind_label} 传输已取消"));
        }
        self.emit_snapshot();
    }

    fn retry_failed_transfers_for_devices(&mut self, device_ids: &[u64]) {
        let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut changed = false;
        let mut results = Vec::new();
        for transfer in &mut self.active_transfers {
            if device_ids.contains(&transfer.device_local_id)
                && transfer.terminal
                && !transfer.succeeded
            {
                transfer.terminal = false;
                transfer.status = "重新排队".into();
                transfer.next_index = transfer.last_sent_index.unwrap_or(0);
                transfer.waiting_ack_opcode = None;
                transfer.waiting_since = None;
                transfer.next_send_at = Instant::now();
                transfer.retry_count = 0;
                transfer.paused = false;
                results.push(transfer.device_local_id);
                changed = true;
            }
        }
        for device_local_id in results {
            self.emit_device_result(device_local_id, "重试", "失败传输重新排队");
        }
        if changed {
            self.emit_snapshot();
        }
    }

    fn resume_transfers_for_devices(&mut self, device_ids: &[u64]) {
        let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut results = Vec::new();
        for transfer in &mut self.active_transfers {
            if device_ids.contains(&transfer.device_local_id)
                && transfer.paused
                && !transfer.terminal
            {
                transfer.paused = false;
                transfer.status = "继续传输".into();
                transfer.next_send_at = Instant::now();
                results.push(transfer.device_local_id);
            }
        }
        for device_local_id in results {
            self.emit_device_result(device_local_id, "继续", "继续传输");
        }
        self.emit_snapshot();
    }

    fn clear_terminal_transfers_for_devices(&mut self, device_ids: &[u64]) {
        let device_ids = device_ids.iter().copied().collect::<BTreeSet<_>>();
        self.active_transfers.retain(|transfer| {
            !(transfer.terminal && device_ids.contains(&transfer.device_local_id))
        });
        self.emit_snapshot();
    }

    fn pause_transfers_for_connection_loss(&mut self, reconnecting: bool) {
        let mut results = Vec::new();
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
            results.push((transfer.device_local_id, transfer.status.clone()));
        }
        for (device_local_id, text) in results {
            self.emit_device_result(device_local_id, "断线", text);
        }
        self.emit_snapshot();
    }

    fn resume_transfers_after_reconnect(&mut self) {
        let resume_at = Instant::now() + Duration::from_millis(self.packet_delay_ms);
        let mut results = Vec::new();
        for transfer in &mut self.active_transfers {
            if transfer.terminal || !transfer.paused {
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
            results.push((transfer.device_local_id, transfer.status.clone()));
        }
        for (device_local_id, text) in results {
            self.emit_device_result(device_local_id, "恢复", text);
        }
        self.emit_snapshot();
    }

    fn finish_ready_transfers(&mut self) {
        let mut completed = Vec::new();
        for (index, transfer) in self.active_transfers.iter().enumerate() {
            if transfer.next_index >= transfer.packets.len()
                && transfer.waiting_ack_opcode.is_none()
                && !transfer.terminal
            {
                completed.push(index);
            }
        }
        let had_completed = !completed.is_empty();
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
            self.emit_log(LogEntry {
                timestamp: now_display(),
                direction: LogDirection::System,
                device_name: device_name.clone(),
                device_id,
                topic: down_topic,
                opcode: "-".into(),
                status: status.clone(),
                summary: summary.clone(),
                payload: String::new(),
            });
            self.emit_operation(OperationRecord {
                timestamp: now_display(),
                device_name,
                opcode: kind_label.clone(),
                status,
                detail: summary,
                rtt_ms: String::new(),
            });
            self.emit_device_result(
                device_local_id,
                if succeeded { "成功" } else { "失败" },
                if succeeded {
                    format!("{kind_label} 传输完成")
                } else {
                    format!("{kind_label} 传输失败: {transfer_status}")
                },
            );
        }
        if had_completed {
            self.emit_snapshot();
        }
    }

    fn emit_snapshot(&self) {
        let snapshots = self
            .active_transfers
            .iter()
            .map(TransferSnapshot::from)
            .collect();
        let _ = self
            .events_tx
            .send(TransferEngineEvent::Snapshot(snapshots));
    }

    fn emit_log(&self, entry: LogEntry) {
        let _ = self.events_tx.send(TransferEngineEvent::Log(entry));
    }

    fn emit_operation(&self, record: OperationRecord) {
        let _ = self.events_tx.send(TransferEngineEvent::Operation(record));
    }

    fn emit_notice(&self, text: String) {
        let _ = self.events_tx.send(TransferEngineEvent::SystemNotice(text));
    }

    fn emit_device_result(
        &self,
        device_local_id: u64,
        label: impl Into<String>,
        text: impl Into<String>,
    ) {
        let _ = self.events_tx.send(TransferEngineEvent::DeviceResult {
            device_local_id,
            label: label.into(),
            text: text.into(),
        });
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

pub(crate) fn transfer_expected_ack_opcode(
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

fn transfer_sent_last_packet(transfer: &ActiveTransfer) -> bool {
    transfer
        .last_sent_index
        .map(|index| index + 1 == transfer.packets.len())
        .unwrap_or(false)
}

pub(crate) fn transfer_resume_index_after_disconnect(transfer: &ActiveTransfer) -> usize {
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

pub(crate) fn transfer_display_total_packets(kind: TransferKind, packet_count: usize) -> usize {
    match kind {
        TransferKind::BcOta | TransferKind::AOta => packet_count.saturating_sub(1),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => packet_count,
    }
}

pub(crate) fn transfer_display_completed_packets(transfer: &ActiveTransfer) -> usize {
    let total = transfer_display_total_packets(transfer.kind, transfer.packets.len());
    match transfer.kind {
        TransferKind::BcOta | TransferKind::AOta => {
            transfer.next_index.saturating_sub(1).min(total)
        }
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => transfer.next_index.min(total),
    }
}

pub(crate) fn transfer_display_packet_number(kind: TransferKind, packet_index: usize) -> usize {
    match kind {
        TransferKind::BcOta | TransferKind::AOta => packet_index.max(1),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => packet_index + 1,
    }
}

pub(crate) fn should_log_transfer_packet(packet_index: usize, packet_count: usize) -> bool {
    packet_index == 0
        || packet_index + 1 >= packet_count
        || packet_index.is_multiple_of(TRANSFER_PACKET_LOG_INTERVAL)
}

pub(crate) fn apply_transfer_retry_state(
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

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_with_events() -> (TransferEngineWorker, Receiver<TransferEngineEvent>) {
        let (_command_tx, command_rx) = mpsc::channel();
        let (_mqtt_tx, mqtt_rx) = mpsc::channel();
        let (events_tx, events_rx) = mpsc::channel();
        (
            TransferEngineWorker::new(command_rx, mqtt_rx, events_tx),
            events_rx,
        )
    }

    fn worker() -> TransferEngineWorker {
        worker_with_events().0
    }

    fn sample_transfer() -> ActiveTransfer {
        ActiveTransfer {
            device_local_id: 1,
            device_name: "设备A".into(),
            device_id: "dev-a".into(),
            up_topic: "/application/AP-C-BM/device/dev-a/up".into(),
            down_topic: "/application/AP-C-BM/device/dev-a/down".into(),
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
            last_sent_time_stamp: None,
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

    fn final_packet_transfer() -> ActiveTransfer {
        let mut transfer = sample_transfer();
        transfer.next_index = transfer.packets.len();
        transfer.last_sent_index = Some(transfer.packets.len() - 1);
        transfer
    }

    fn drain_events(events_rx: &Receiver<TransferEngineEvent>) -> Vec<TransferEngineEvent> {
        let mut events = Vec::new();
        while let Ok(event) = events_rx.try_recv() {
            events.push(event);
        }
        events
    }

    #[test]
    fn engine_bc_ota_ack_value_3_releases_next_packet_immediately() {
        let mut worker = worker();
        worker.active_transfers.push(sample_transfer());

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up/gen/0",
            &serde_json::json!({"opcode": 0x41, "value": 3}),
        );

        let transfer = &worker.active_transfers[0];
        assert_eq!(transfer.status, "BC OTA ACK成功，继续发送");
        assert!(transfer.waiting_ack_opcode.is_none());
        assert!(transfer.next_send_at <= Instant::now());
    }

    #[test]
    fn engine_bc_ota_ack_value_2_requeues_current_packet() {
        let mut worker = worker();
        worker.active_transfers.push(sample_transfer());

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up/gen/0",
            &serde_json::json!({"opcode": 0x41, "value": 2}),
        );

        let transfer = &worker.active_transfers[0];
        assert_eq!(transfer.next_index, 0);
        assert_eq!(transfer.retry_count, 1);
        assert_eq!(transfer.failure_packet_index, Some(0));
        assert!(transfer.waiting_ack_opcode.is_none());
        assert!(transfer.status.contains("准备重试 1/2"));
    }

    #[test]
    fn engine_bc_ota_final_ack_value_3_waits_for_upgrade_result() {
        let mut worker = worker();
        worker.active_transfers.push(final_packet_transfer());

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up",
            &serde_json::json!({"opcode": 0x41, "value": 3}),
        );

        let transfer = &worker.active_transfers[0];
        assert!(!transfer.terminal);
        assert_eq!(transfer.waiting_ack_opcode, Some(0x41));
        assert!(transfer.waiting_since.is_some());
        assert_eq!(transfer.status, "最后包已确认，等待升级结果");
    }

    #[test]
    fn engine_bc_ota_final_ack_value_5_finishes_transfer() {
        let mut worker = worker();
        worker.active_transfers.push(final_packet_transfer());

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up",
            &serde_json::json!({"opcode": 0x41, "value": 5}),
        );
        worker.finish_ready_transfers();

        let transfer = &worker.active_transfers[0];
        assert!(transfer.terminal);
        assert!(transfer.succeeded);
        assert_eq!(transfer.status, "BC OTA升级成功");
    }

    #[test]
    fn engine_bc_ota_final_ack_value_4_marks_failure() {
        let mut worker = worker();
        worker.active_transfers.push(final_packet_transfer());

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up",
            &serde_json::json!({"opcode": 0x41, "value": 4}),
        );
        worker.finish_ready_transfers();

        let transfer = &worker.active_transfers[0];
        assert!(transfer.terminal);
        assert!(!transfer.succeeded);
        assert_eq!(transfer.status, "BC OTA升级失败");
        assert_eq!(transfer.failure_packet_index, Some(1));
    }

    #[test]
    fn engine_bc_ota_abnormal_ack_value_uses_retry_budget() {
        let mut worker = worker();
        let mut transfer = sample_transfer();
        transfer.max_retries = 0;
        worker.active_transfers.push(transfer);

        worker.resolve_transfer_ack(
            "/application/AP-C-BM/device/dev-a/up",
            &serde_json::json!({"opcode": 0x41, "value": 99}),
        );
        worker.finish_ready_transfers();

        let transfer = &worker.active_transfers[0];
        assert!(transfer.terminal);
        assert!(!transfer.succeeded);
        assert_eq!(transfer.next_index, transfer.packets.len());
        assert!(transfer.status.contains("异常值 99"));
    }

    #[test]
    fn engine_bc_ota_start_and_data_ack_timeouts_are_split() {
        let mut worker = worker();
        worker.ack_timeout_secs = 10;
        worker.bc_ota_start_ack_timeout_secs = 20;

        let start_transfer = sample_transfer();
        assert_eq!(
            worker.transfer_ack_timeout_for(&start_transfer),
            Duration::from_secs(20)
        );

        let mut data_transfer = sample_transfer();
        data_transfer.last_sent_index = Some(1);
        data_transfer.next_index = 2;
        assert_eq!(
            worker.transfer_ack_timeout_for(&data_transfer),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn engine_collect_transfer_timeouts_respects_start_timeout_split() {
        let mut worker = worker();
        worker.ack_timeout_secs = 1;
        worker.bc_ota_start_ack_timeout_secs = 20;
        let old_wait = Instant::now() - Duration::from_secs(2);

        let mut start_transfer = sample_transfer();
        start_transfer.waiting_since = Some(old_wait);
        let mut data_transfer = sample_transfer();
        data_transfer.device_local_id = 2;
        data_transfer.device_name = "设备B".into();
        data_transfer.device_id = "dev-b".into();
        data_transfer.up_topic = "/application/AP-C-BM/device/dev-b/up".into();
        data_transfer.last_sent_index = Some(1);
        data_transfer.next_index = 2;
        data_transfer.waiting_since = Some(old_wait);
        worker.active_transfers.push(start_transfer);
        worker.active_transfers.push(data_transfer);

        worker.collect_transfer_timeouts();

        assert_eq!(worker.active_transfers[0].retry_count, 0);
        assert_eq!(worker.active_transfers[1].retry_count, 1);
        assert_eq!(worker.active_transfers[1].failure_packet_index, Some(1));
    }

    #[test]
    fn engine_mqtt_generation_filter_ignores_stale_ack_and_accepts_current_ack() {
        let mut worker = worker();
        worker.active_transfers.push(sample_transfer());

        worker.handle_mqtt_event(MqttEvent::Connection {
            generation: 2,
            connected: true,
            reconnecting: false,
            message: "已连接".into(),
        });
        worker.handle_mqtt_event(MqttEvent::Message {
            generation: 1,
            topic: "/application/AP-C-BM/device/dev-a/up/gen/0".into(),
            payload: serde_json::json!({"opcode": 0x41, "value": 3}).to_string(),
        });
        assert_eq!(worker.active_transfers[0].waiting_ack_opcode, Some(0x41));

        worker.handle_mqtt_event(MqttEvent::Message {
            generation: 2,
            topic: "/application/AP-C-BM/device/dev-a/up/gen/0".into(),
            payload: serde_json::json!({"opcode": 0x41, "value": 3}).to_string(),
        });
        assert!(worker.active_transfers[0].waiting_ack_opcode.is_none());
        assert_eq!(
            worker.active_transfers[0].status,
            "BC OTA ACK成功，继续发送"
        );
    }

    #[test]
    fn engine_connection_loss_rewinds_and_recovery_resumes() {
        let mut worker = worker();
        worker.active_transfers.push(sample_transfer());

        worker.pause_transfers_for_connection_loss(true);
        let transfer = &worker.active_transfers[0];
        assert!(transfer.paused);
        assert_eq!(transfer.next_index, 0);
        assert!(transfer.waiting_ack_opcode.is_none());

        worker.resume_transfers_after_reconnect();
        let transfer = &worker.active_transfers[0];
        assert!(!transfer.paused);
        assert_eq!(transfer.status, "连接已恢复，从第1包继续");
    }

    #[test]
    fn engine_command_controls_cancel_retry_resume_and_clear_all() {
        let mut worker = worker();
        let mut paused = sample_transfer();
        paused.device_local_id = 2;
        paused.device_name = "设备B".into();
        paused.device_id = "dev-b".into();
        paused.paused = true;
        paused.waiting_ack_opcode = None;
        paused.status = "连接断开，等待重连后续传".into();

        worker.active_transfers.push(sample_transfer());
        worker.active_transfers.push(paused);

        worker.handle_command(TransferEngineCommand::CancelDevices(vec![1]));
        assert!(worker.active_transfers[0].terminal);
        assert_eq!(worker.active_transfers[0].status, "已取消");

        worker.handle_command(TransferEngineCommand::RetryDevices(vec![1]));
        assert!(!worker.active_transfers[0].terminal);
        assert_eq!(worker.active_transfers[0].retry_count, 0);
        assert_eq!(worker.active_transfers[0].status, "重新排队");

        worker.handle_command(TransferEngineCommand::ResumeDevices(vec![2]));
        assert!(!worker.active_transfers[1].paused);
        assert_eq!(worker.active_transfers[1].status, "继续传输");

        worker.active_transfers[0].terminal = true;
        worker.active_transfers[1].terminal = true;
        worker.handle_command(TransferEngineCommand::ClearTerminal);
        assert!(worker.active_transfers.is_empty());
    }

    #[test]
    fn engine_clear_terminal_devices_keeps_nonmatching_rows() {
        let mut worker = worker();
        let mut finished = sample_transfer();
        finished.terminal = true;
        finished.succeeded = true;

        let mut failed = sample_transfer();
        failed.device_local_id = 2;
        failed.device_name = "设备B".into();
        failed.device_id = "dev-b".into();
        failed.terminal = true;
        failed.succeeded = false;

        let mut active = sample_transfer();
        active.device_local_id = 3;
        active.device_name = "设备C".into();
        active.device_id = "dev-c".into();

        worker.active_transfers.push(finished);
        worker.active_transfers.push(failed);
        worker.active_transfers.push(active);

        worker.clear_terminal_transfers_for_devices(&[2, 3]);

        let remaining = worker
            .active_transfers
            .iter()
            .map(|transfer| transfer.device_local_id)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![1, 3]);
    }

    #[test]
    fn engine_mqtt_event_path_emits_snapshot_and_device_result() {
        let (mut worker, events_rx) = worker_with_events();
        worker.active_transfers.push(sample_transfer());
        worker.handle_mqtt_event(MqttEvent::Connection {
            generation: 1,
            connected: true,
            reconnecting: false,
            message: "已连接".into(),
        });
        let _ = drain_events(&events_rx);

        worker.handle_mqtt_event(MqttEvent::Message {
            generation: 1,
            topic: "/application/AP-C-BM/device/dev-a/up/gen/0".into(),
            payload: serde_json::json!({"opcode": 0x41, "value": 3}).to_string(),
        });

        let events = drain_events(&events_rx);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                TransferEngineEvent::DeviceResult {
                    device_local_id: 1,
                    label,
                    text,
                } if label == "传输" && text == "BC OTA ACK成功，继续发送"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                TransferEngineEvent::Snapshot(transfers)
                    if transfers.len() == 1 && transfers[0].waiting_ack_opcode.is_none()
            )
        }));
    }

    #[test]
    fn engine_snapshot_event_uses_lightweight_transfer_snapshot() {
        let (mut worker, events_rx) = worker_with_events();
        let mut transfer = sample_transfer();
        transfer.packets = (0..512)
            .map(|index| serde_json::json!({"opcode": 0x42, "value": format!("{index:04X}")}))
            .collect();
        transfer.next_index = 128;
        worker.active_transfers.push(transfer);

        worker.emit_snapshot();

        let events = drain_events(&events_rx);
        let snapshot = events
            .iter()
            .find_map(|event| match event {
                TransferEngineEvent::Snapshot(transfers) => transfers.first(),
                _ => None,
            })
            .expect("snapshot event should be emitted");
        assert_eq!(snapshot.packet_count, 512);
        assert_eq!(snapshot.next_index, 128);
    }
}
