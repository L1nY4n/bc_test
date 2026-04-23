from __future__ import annotations

import copy
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ..models import (
    AppState,
    BrokerProfile,
    DeviceProfile,
    DeviceRuntimeState,
    LogDirection,
    LogEntry,
    PendingRequest,
    TransferJob,
)
from ..mqtt import MqttController
from ..protocol import (
    CommandSpec,
    build_command_payload,
    current_time_stamp,
    expected_response_opcode,
    format_json,
    parse_opcode,
    redact_json_text,
    redact_secrets,
    summarize_payload,
)
from ..store import AppStateStore
from ..transfers import TransferManager
from .dialogs import BrokerDialog, DeviceDialog
from .forms import CommandPanel, RawJsonPanel, TransferPanel


def _now_display() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Mesh BC Tester")
        self.resize(1520, 920)

        self.store = AppStateStore()
        self.state = self.store.load()
        if not self.state.brokers:
            self.state.brokers = [BrokerProfile()]

        self.device_states: dict[str, DeviceRuntimeState] = {
            device.id: DeviceRuntimeState() for device in self.state.devices
        }
        self.logs: list[LogEntry] = []
        self.transfer_jobs: dict[str, TransferJob] = {}
        self.pending_requests: dict[str, PendingRequest] = {}
        self.log_limit = 2000

        self.mqtt = MqttController()
        self.transfer_manager = TransferManager(self._publish_payload)

        self._build_ui()
        self._connect_signals()
        self._load_state_into_ui()

        self.timeout_timer = QTimer(self)
        self.timeout_timer.setInterval(1000)
        self.timeout_timer.timeout.connect(self._check_pending_timeouts)
        self.timeout_timer.start()

    def _build_ui(self) -> None:
        root = QWidget()
        root_layout = QVBoxLayout(root)

        top_bar = QHBoxLayout()
        self.broker_combo = QComboBox()
        self.add_broker_button = QPushButton("Add Broker")
        self.edit_broker_button = QPushButton("Edit Broker")
        self.remove_broker_button = QPushButton("Remove Broker")
        self.connect_button = QPushButton("Connect")
        self.disconnect_button = QPushButton("Disconnect")
        self.export_button = QPushButton("Export Session")
        self.connection_label = QLabel("Disconnected")

        top_bar.addWidget(QLabel("Broker"))
        top_bar.addWidget(self.broker_combo, 1)
        top_bar.addWidget(self.add_broker_button)
        top_bar.addWidget(self.edit_broker_button)
        top_bar.addWidget(self.remove_broker_button)
        top_bar.addWidget(self.connect_button)
        top_bar.addWidget(self.disconnect_button)
        top_bar.addWidget(self.export_button)
        top_bar.addWidget(self.connection_label)
        root_layout.addLayout(top_bar)

        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        root_layout.addWidget(main_splitter, 1)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        self.selection_label = QLabel("Selected Devices: 0")
        self.device_table = QTableWidget(0, 7)
        self.device_table.setHorizontalHeaderLabels(
            ["Name", "Device ID", "Status", "Last Seen", "RX", "TX", "Last Result"]
        )
        self.device_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.device_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.device_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.device_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        device_actions = QHBoxLayout()
        self.add_device_button = QPushButton("Add Device")
        self.edit_device_button = QPushButton("Edit Device")
        self.remove_device_button = QPushButton("Remove Device")
        self.sync_topics_button = QPushButton("Sync Subs")
        device_actions.addWidget(self.add_device_button)
        device_actions.addWidget(self.edit_device_button)
        device_actions.addWidget(self.remove_device_button)
        device_actions.addWidget(self.sync_topics_button)

        left_layout.addWidget(self.selection_label)
        left_layout.addWidget(self.device_table, 1)
        left_layout.addLayout(device_actions)
        main_splitter.addWidget(left_panel)

        right_splitter = QSplitter(Qt.Orientation.Vertical)
        main_splitter.addWidget(right_splitter)
        main_splitter.setStretchFactor(1, 1)

        self.tabs = QTabWidget()
        right_splitter.addWidget(self.tabs)

        self.overview_table = QTableWidget(0, 8)
        self.overview_table.setHorizontalHeaderLabels(
            ["Name", "Device ID", "Up Topic", "Down Topic", "Status", "Last Seen", "RX", "TX"]
        )
        self.overview_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.overview_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        overview_page = QWidget()
        overview_layout = QVBoxLayout(overview_page)
        overview_layout.addWidget(QLabel("Configured devices and live MQTT activity."))
        overview_layout.addWidget(self.overview_table, 1)
        self.tabs.addTab(overview_page, "Overview")

        self.command_panel = CommandPanel()
        self.tabs.addTab(self.command_panel, "Preset Commands")

        self.raw_panel = RawJsonPanel()
        self.tabs.addTab(self.raw_panel, "Raw JSON")

        transfers_page = QWidget()
        transfers_layout = QVBoxLayout(transfers_page)
        self.transfer_panel = TransferPanel()
        self.transfer_table = QTableWidget(0, 7)
        self.transfer_table.setHorizontalHeaderLabels(
            ["Device", "Type", "Status", "Packets", "Bytes", "Progress", "Error"]
        )
        self.transfer_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.transfer_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.transfer_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.transfer_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        transfers_layout.addWidget(self.transfer_panel)
        transfers_layout.addWidget(self.transfer_table, 1)
        self.tabs.addTab(transfers_page, "Transfers")

        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        log_filter_bar = QHBoxLayout()
        self.selected_only_checkbox = QCheckBox("Show only selected devices")
        self.clear_logs_button = QPushButton("Clear Logs")
        log_filter_bar.addWidget(self.selected_only_checkbox)
        log_filter_bar.addStretch(1)
        log_filter_bar.addWidget(self.clear_logs_button)
        log_layout.addLayout(log_filter_bar)

        log_splitter = QSplitter(Qt.Orientation.Vertical)
        self.log_table = QTableWidget(0, 7)
        self.log_table.setHorizontalHeaderLabels(
            ["Time", "Dir", "Device", "Opcode", "Status", "Summary", "Topic"]
        )
        self.log_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.log_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.log_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.payload_view = QPlainTextEdit()
        self.payload_view.setReadOnly(True)
        log_splitter.addWidget(self.log_table)
        log_splitter.addWidget(self.payload_view)
        log_splitter.setStretchFactor(0, 2)
        log_splitter.setStretchFactor(1, 1)
        log_layout.addWidget(log_splitter, 1)
        right_splitter.addWidget(log_widget)
        right_splitter.setStretchFactor(0, 2)
        right_splitter.setStretchFactor(1, 1)

        self.setCentralWidget(root)
        status = QStatusBar()
        self.setStatusBar(status)

    def _connect_signals(self) -> None:
        self.add_broker_button.clicked.connect(self._add_broker)
        self.edit_broker_button.clicked.connect(self._edit_broker)
        self.remove_broker_button.clicked.connect(self._remove_broker)
        self.connect_button.clicked.connect(self._connect_broker)
        self.disconnect_button.clicked.connect(self.mqtt.disconnect_client)
        self.export_button.clicked.connect(self._export_session)

        self.add_device_button.clicked.connect(self._add_device)
        self.edit_device_button.clicked.connect(self._edit_device)
        self.remove_device_button.clicked.connect(self._remove_device)
        self.sync_topics_button.clicked.connect(self._sync_subscriptions)

        self.device_table.itemSelectionChanged.connect(self._on_device_selection_changed)
        self.selected_only_checkbox.toggled.connect(self._refresh_logs_table)
        self.clear_logs_button.clicked.connect(self._clear_logs)
        self.log_table.itemSelectionChanged.connect(self._show_selected_log_payload)

        self.command_panel.send_requested.connect(self._send_preset_command)
        self.raw_panel.send_requested.connect(self._send_raw_json)
        self.transfer_panel.start_requested.connect(self._start_transfer)
        self.transfer_panel.cancel_requested.connect(self._cancel_selected_transfers)

        self.mqtt.connection_changed.connect(self._on_connection_changed)
        self.mqtt.message_received.connect(self._on_message_received)
        self.mqtt.system_message.connect(self._log_system)
        self.transfer_manager.job_updated.connect(self._on_transfer_job_updated)
        self.transfer_manager.job_finished.connect(self._on_transfer_job_finished)
        self.transfer_manager.system_message.connect(self._log_system)

    def _load_state_into_ui(self) -> None:
        self._refresh_broker_combo()
        self._refresh_device_tables()
        self._refresh_logs_table()

    def _refresh_broker_combo(self) -> None:
        self.broker_combo.blockSignals(True)
        self.broker_combo.clear()
        for broker in self.state.brokers:
            self.broker_combo.addItem(f"{broker.name} ({broker.host}:{broker.port})", broker.id)
        if self.state.selected_broker_id:
            index = next(
                (i for i, broker in enumerate(self.state.brokers) if broker.id == self.state.selected_broker_id),
                0,
            )
            self.broker_combo.setCurrentIndex(index)
        self.broker_combo.blockSignals(False)

    def _refresh_device_tables(self) -> None:
        devices = self.state.devices
        self.device_table.setRowCount(len(devices))
        self.overview_table.setRowCount(len(devices))
        for row, device in enumerate(devices):
            state = self.device_states.setdefault(device.id, DeviceRuntimeState())
            self._set_table_row(
                self.device_table,
                row,
                [
                    device.name,
                    device.device_id,
                    "Online" if state.online else "Idle",
                    state.last_seen,
                    str(state.rx_count),
                    str(state.tx_count),
                    state.last_result,
                ],
                user_data=device.id,
            )
            self._set_table_row(
                self.overview_table,
                row,
                [
                    device.name,
                    device.device_id,
                    device.up_topic,
                    device.down_topic,
                    "Online" if state.online else "Idle",
                    state.last_seen,
                    str(state.rx_count),
                    str(state.tx_count),
                ],
                user_data=device.id,
            )
        self._on_device_selection_changed()

    def _set_table_row(self, table: QTableWidget, row: int, values: list[str], user_data: str | None = None) -> None:
        for column, value in enumerate(values):
            item = QTableWidgetItem(value)
            if column == 0 and user_data is not None:
                item.setData(Qt.ItemDataRole.UserRole, user_data)
            table.setItem(row, column, item)

    def _selected_device_ids(self) -> list[str]:
        rows = sorted({index.row() for index in self.device_table.selectionModel().selectedRows()})
        device_ids: list[str] = []
        for row in rows:
            item = self.device_table.item(row, 0)
            if item:
                device_id = item.data(Qt.ItemDataRole.UserRole)
                if device_id:
                    device_ids.append(device_id)
        return device_ids

    def _selected_devices(self) -> list[DeviceProfile]:
        selected_ids = set(self._selected_device_ids())
        return [device for device in self.state.devices if device.id in selected_ids]

    def _device_by_topic(self, topic: str) -> DeviceProfile | None:
        for device in self.state.devices:
            if topic == device.up_topic or topic == device.down_topic:
                return device
        return None

    def _current_broker(self) -> BrokerProfile:
        broker_id = self.broker_combo.currentData()
        broker = next((item for item in self.state.brokers if item.id == broker_id), None)
        if broker is None:
            raise ValueError("No broker profile selected.")
        return broker

    def _persist_state(self) -> None:
        self.state.selected_broker_id = self.broker_combo.currentData()
        self.store.save(self.state)

    def _on_device_selection_changed(self) -> None:
        selected = self._selected_devices()
        names = ", ".join(device.name for device in selected[:3])
        suffix = "" if len(selected) <= 3 else f" +{len(selected) - 3}"
        self.selection_label.setText(f"Selected Devices: {len(selected)} {names}{suffix}".strip())
        self.command_panel.update_preview_for_device(selected[0] if selected else None)
        self._refresh_logs_table()

    def _add_broker(self) -> None:
        dialog = BrokerDialog(parent=self)
        if dialog.exec():
            self.state.brokers.append(dialog.value())
            self._refresh_broker_combo()
            self._persist_state()

    def _edit_broker(self) -> None:
        broker = self._current_broker()
        dialog = BrokerDialog(broker, self)
        if dialog.exec():
            dialog.value()
            self._refresh_broker_combo()
            self._persist_state()

    def _remove_broker(self) -> None:
        if len(self.state.brokers) <= 1:
            self._message("At least one broker profile must remain.")
            return
        broker = self._current_broker()
        self.state.brokers = [item for item in self.state.brokers if item.id != broker.id]
        self._refresh_broker_combo()
        self._persist_state()

    def _connect_broker(self) -> None:
        try:
            broker = self._current_broker()
        except ValueError as exc:
            self._message(str(exc))
            return
        self.state.selected_broker_id = broker.id
        self._persist_state()
        self.mqtt.reset_subscriptions()
        self.mqtt.connect_profile(broker)

    def _add_device(self) -> None:
        dialog = DeviceDialog(parent=self)
        if dialog.exec():
            device = dialog.value()
            validation_error = self._validate_device(device)
            if validation_error:
                self._message(validation_error)
                return
            self.state.devices.append(device)
            self.device_states[device.id] = DeviceRuntimeState()
            self._refresh_device_tables()
            self._persist_state()
            self._sync_subscriptions()

    def _selected_device_for_edit(self) -> DeviceProfile | None:
        selected = self._selected_devices()
        if len(selected) != 1:
            self._message("Select exactly one device for this action.")
            return None
        return selected[0]

    def _edit_device(self) -> None:
        device = self._selected_device_for_edit()
        if device is None:
            return
        dialog = DeviceDialog(device, self)
        if dialog.exec():
            updated = dialog.value()
            validation_error = self._validate_device(updated)
            if validation_error:
                self._message(validation_error)
                return
            self._refresh_device_tables()
            self._persist_state()
            self._sync_subscriptions()

    def _remove_device(self) -> None:
        selected_ids = set(self._selected_device_ids())
        if not selected_ids:
            self._message("Select at least one device to remove.")
            return
        self.state.devices = [device for device in self.state.devices if device.id not in selected_ids]
        for device_id in selected_ids:
            self.device_states.pop(device_id, None)
        self._refresh_device_tables()
        self._persist_state()
        self._sync_subscriptions()

    def _sync_subscriptions(self) -> None:
        topics = [device.up_topic for device in self.state.devices if device.subscribe_enabled]
        self.mqtt.sync_subscriptions(topics)
        self._log_system(f"Subscription set refreshed with {len(topics)} uplink topics.")

    def _send_preset_command(self, spec: CommandSpec, values: dict[str, Any]) -> None:
        devices = self._selected_devices()
        if not devices:
            self._message("Select at least one device before sending a command.")
            return
        for device in devices:
            try:
                payload = build_command_payload(spec, device, values)
            except Exception as exc:  # noqa: BLE001
                self._message(f"{device.name}: {exc}")
                return
            self._publish_payload(device, payload, "preset")

    def _send_raw_json(self, payload: dict, auto_timestamp: bool) -> None:
        devices = self._selected_devices()
        if not devices:
            self._message("Select at least one device before sending raw JSON.")
            return
        try:
            opcode = parse_opcode(payload.get("opcode"))
        except ValueError:
            self._message("Raw JSON opcode must be an integer or 0x-prefixed hexadecimal string.")
            return
        for device in devices:
            device_payload = copy.deepcopy(payload)
            if auto_timestamp and "time_stamp" not in device_payload:
                device_payload["time_stamp"] = current_time_stamp()
            if "mesh_dev_type" not in device_payload and opcode is not None and opcode < 0x40:
                device_payload["mesh_dev_type"] = device.mesh_dev_type
            if "dest_addr" not in device_payload and opcode is not None and opcode < 0x40:
                device_payload["dest_addr"] = device.default_dest_addr
            self._publish_payload(device, device_payload, "raw")

    def _publish_payload(self, device: DeviceProfile, payload: dict, source: str) -> bool:
        if not device.down_topic:
            self._log_system(f"Publish skipped for '{device.name}' because the downlink topic is empty.")
            return False
        if not self.mqtt.publish_json(device.down_topic, payload):
            return False

        state = self.device_states.setdefault(device.id, DeviceRuntimeState())
        state.tx_count += 1
        try:
            opcode_value = parse_opcode(payload.get("opcode"))
        except ValueError:
            opcode_value = None
        opcode = opcode_value if opcode_value is not None else -1
        state.last_opcode = f"0x{opcode:02X}" if opcode >= 0 else "-"
        state.last_result = f"Sent via {source}"
        summary = summarize_payload(redact_secrets(payload))
        self._append_log(
            LogEntry(
                timestamp=_now_display(),
                direction=LogDirection.TX,
                device_name=device.name,
                device_id=device.device_id,
                topic=device.down_topic,
                payload=format_json(redact_secrets(payload)),
                opcode=f"0x{opcode:02X}" if opcode >= 0 else "-",
                status=source.upper(),
                summary=summary,
            )
        )

        if source != "transfer" and payload.get("time_stamp") is not None:
            request = PendingRequest(
                request_id=f"{device.id}:{time.monotonic()}",
                device_id=device.id,
                device_name=device.name,
                opcode=opcode,
                expected_opcode=expected_response_opcode(opcode),
                topic=device.down_topic,
                time_stamp=payload.get("time_stamp"),
                timeout_seconds=10,
                sent_monotonic=time.monotonic(),
            )
            self.pending_requests[request.request_id] = request
        elif source != "transfer":
            state.last_result = f"Sent via {source} (no request tracking)"
        self._refresh_device_tables()
        return True

    def _on_connection_changed(self, connected: bool, message: str) -> None:
        self.connection_label.setText(message)
        self.statusBar().showMessage(message, 5000)
        if connected:
            self._sync_subscriptions()

    def _on_message_received(self, topic: str, payload_text: str, parsed: object) -> None:
        device = self._device_by_topic(topic)
        device_name = device.name if device else "(unmapped)"
        device_id = device.device_id if device else "-"
        opcode_text = "-"
        status = "RX"
        summary = payload_text[:96]

        if device:
            state = self.device_states.setdefault(device.id, DeviceRuntimeState())
            state.online = True
            state.last_seen = _now_display()
            state.rx_count += 1

        if isinstance(parsed, dict):
            opcode = parsed.get("opcode")
            if isinstance(opcode, int):
                opcode_text = f"0x{opcode:02X}"
                summary = summarize_payload(redact_secrets(parsed))
                if device:
                    state = self.device_states[device.id]
                    state.last_opcode = opcode_text
                    state.last_summary = summary
                    self._resolve_pending_request(device, parsed)

        self._append_log(
            LogEntry(
                timestamp=_now_display(),
                direction=LogDirection.RX,
                device_name=device_name,
                device_id=device_id,
                topic=topic,
                payload=redact_json_text(payload_text) if not isinstance(parsed, dict) else format_json(redact_secrets(parsed)),
                opcode=opcode_text,
                status=status,
                summary=summary,
            )
        )
        self._refresh_device_tables()

    def _resolve_pending_request(self, device: DeviceProfile, payload: dict[str, Any]) -> None:
        payload_opcode = int(payload.get("opcode", -1))
        payload_time_stamp = payload.get("time_stamp")
        matched_request_id: str | None = None
        for request_id, request in self.pending_requests.items():
            if request.device_id != device.id:
                continue
            if request.expected_opcode != payload_opcode:
                continue
            if request.time_stamp is not None and payload_time_stamp not in (None, request.time_stamp):
                continue
            matched_request_id = request_id
            break

        if matched_request_id is None:
            return

        request = self.pending_requests.pop(matched_request_id)
        result_text = str(payload.get("value", "ACK"))
        status = "ACK"
        if isinstance(payload.get("value"), str) and "error" in payload["value"].lower():
            status = "ERROR"
        state = self.device_states.setdefault(device.id, DeviceRuntimeState())
        state.last_result = f"{status} for 0x{request.opcode:02X}"
        self._append_log(
            LogEntry(
                timestamp=_now_display(),
                direction=LogDirection.SYSTEM,
                device_name=device.name,
                device_id=device.device_id,
                topic=request.topic,
                payload=format_json(redact_secrets(payload)),
                opcode=f"0x{payload_opcode:02X}",
                status=status,
                summary=result_text,
            )
        )

    def _check_pending_timeouts(self) -> None:
        now = time.monotonic()
        expired: list[str] = []
        for request_id, request in self.pending_requests.items():
            if now - request.sent_monotonic < request.timeout_seconds:
                continue
            expired.append(request_id)
        for request_id in expired:
            request = self.pending_requests.pop(request_id)
            device = next((item for item in self.state.devices if item.id == request.device_id), None)
            if device is not None:
                state = self.device_states.setdefault(device.id, DeviceRuntimeState())
                state.last_result = f"Timeout waiting for 0x{request.expected_opcode:02X}"
            self._append_log(
                LogEntry(
                    timestamp=_now_display(),
                    direction=LogDirection.SYSTEM,
                    device_name=request.device_name,
                    device_id=device.device_id if device else "-",
                    topic=request.topic,
                    payload="",
                    opcode=f"0x{request.expected_opcode:02X}",
                    status="TIMEOUT",
                    summary="No matching response within 10 seconds.",
                )
            )
        if expired:
            self._refresh_device_tables()

    def _append_log(self, entry: LogEntry) -> None:
        self.logs.append(entry)
        if len(self.logs) > self.log_limit:
            self.logs = self.logs[-self.log_limit :]
        self._refresh_logs_table()

    def _refresh_logs_table(self) -> None:
        selected_device_ids = {device.device_id for device in self._selected_devices()}
        entries = self.logs
        if self.selected_only_checkbox.isChecked() and selected_device_ids:
            entries = [entry for entry in entries if entry.device_id in selected_device_ids]
        self.log_table.setRowCount(len(entries))
        for row, entry in enumerate(entries):
            self._set_table_row(
                self.log_table,
                row,
                [
                    entry.timestamp,
                    entry.direction.value,
                    entry.device_name,
                    entry.opcode,
                    entry.status,
                    entry.summary,
                    entry.topic,
                ],
                user_data=str(len(entries) and row),
            )
            self.log_table.item(row, 0).setData(Qt.ItemDataRole.UserRole, entry.payload)
        if entries:
            self.log_table.scrollToBottom()

    def _show_selected_log_payload(self) -> None:
        rows = self.log_table.selectionModel().selectedRows()
        if not rows:
            self.payload_view.clear()
            return
        item = self.log_table.item(rows[0].row(), 0)
        payload = item.data(Qt.ItemDataRole.UserRole) if item else ""
        self.payload_view.setPlainText(payload or "")

    def _clear_logs(self) -> None:
        self.logs.clear()
        self.payload_view.clear()
        self._refresh_logs_table()

    def _start_transfer(self, request: dict[str, Any]) -> None:
        devices = self._selected_devices()
        if not devices:
            self._message("Select at least one device before starting a transfer.")
            return
        file_path = request["file_path"]
        if not file_path:
            self._message("Choose a transfer file first.")
            return
        if not Path(file_path).exists():
            self._message("Selected transfer file does not exist.")
            return
        if Path(file_path).stat().st_size <= 0:
            self._message("Transfer file must not be empty.")
            return
        jobs = self.transfer_manager.start_jobs(
            devices=devices,
            kind=request["kind"],
            file_path=file_path,
            version=request["version"],
            voice_name=request["voice_name"],
            packet_interval_ms=request["packet_interval_ms"],
        )
        for job in jobs:
            self.transfer_jobs[job.id] = job
            self._append_log(
                LogEntry(
                    timestamp=_now_display(),
                    direction=LogDirection.SYSTEM,
                    device_name=job.device_name,
                    device_id=next(
                        (device.device_id for device in self.state.devices if device.id == job.device_id),
                        "-",
                    ),
                    topic="transfer",
                    payload=file_path,
                    opcode="-",
                    status="TRANSFER",
                    summary=f"Started {job.kind.value}",
                )
            )
        self._refresh_transfer_table()

    def _on_transfer_job_updated(self, job: TransferJob) -> None:
        self.transfer_jobs[job.id] = job
        self._refresh_transfer_table()

    def _on_transfer_job_finished(self, job: TransferJob) -> None:
        self.transfer_jobs[job.id] = job
        self._append_log(
            LogEntry(
                timestamp=_now_display(),
                direction=LogDirection.SYSTEM,
                device_name=job.device_name,
                device_id=next((device.device_id for device in self.state.devices if device.id == job.device_id), "-"),
                topic="transfer",
                payload=job.file_path,
                opcode="-",
                status=job.status.upper(),
                summary=f"{job.kind.value} {job.status}",
            )
        )
        self._refresh_transfer_table()

    def _refresh_transfer_table(self) -> None:
        jobs = list(self.transfer_jobs.values())
        self.transfer_table.setRowCount(len(jobs))
        for row, job in enumerate(jobs):
            self._set_table_row(
                self.transfer_table,
                row,
                [
                    job.device_name,
                    job.kind.value,
                    job.status,
                    f"{job.sent_packets}/{job.total_packets}",
                    f"{job.sent_bytes}/{job.total_bytes}",
                    f"{job.progress_percent}%",
                    job.last_error,
                ],
                user_data=job.id,
            )

    def _cancel_selected_transfers(self) -> None:
        rows = self.transfer_table.selectionModel().selectedRows()
        job_ids: list[str] = []
        for row in rows:
            item = self.transfer_table.item(row.row(), 0)
            if item:
                job_id = item.data(Qt.ItemDataRole.UserRole)
                if job_id:
                    job_ids.append(job_id)
        if not job_ids:
            active_job_ids = [job.id for job in self.transfer_jobs.values() if job.status == "running"]
            job_ids = active_job_ids
        if not job_ids:
            self._message("No running transfer selected.")
            return
        self.transfer_manager.cancel_jobs(job_ids)

    def _export_session(self) -> None:
        output_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Session",
            str(Path.cwd() / "mesh-bc-session.json"),
            "JSON Files (*.json)",
        )
        if not output_path:
            return
        payload = {
            "generated_at": _now_display(),
            "broker": self._current_broker().to_dict(),
            "devices": [device.to_dict() for device in self.state.devices],
            "device_states": {
                device_id: {
                    "online": state.online,
                    "last_seen": state.last_seen,
                    "tx_count": state.tx_count,
                    "rx_count": state.rx_count,
                    "last_opcode": state.last_opcode,
                    "last_result": state.last_result,
                    "last_summary": state.last_summary,
                }
                for device_id, state in self.device_states.items()
            },
            "logs": [entry.to_dict() for entry in self.logs],
            "transfers": {
                job_id: {
                    "device_name": job.device_name,
                    "kind": job.kind.value,
                    "status": job.status,
                    "sent_packets": job.sent_packets,
                    "total_packets": job.total_packets,
                    "sent_bytes": job.sent_bytes,
                    "total_bytes": job.total_bytes,
                    "last_error": job.last_error,
                }
                for job_id, job in self.transfer_jobs.items()
            },
        }
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.statusBar().showMessage(f"Session exported to {output_path}", 5000)

    def _log_system(self, message: str) -> None:
        self._append_log(
            LogEntry(
                timestamp=_now_display(),
                direction=LogDirection.SYSTEM,
                device_name="System",
                device_id="-",
                topic="-",
                payload=message,
                opcode="-",
                status="INFO",
                summary=message,
            )
        )

    def _message(self, message: str) -> None:
        QMessageBox.warning(self, "Mesh BC Tester", message)

    def _validate_device(self, device: DeviceProfile) -> str | None:
        if not device.device_id or not device.up_topic or not device.down_topic:
            return "Device ID, uplink topic, and downlink topic are required."
        if any(char in device.up_topic for char in "#+") or any(char in device.down_topic for char in "#+"):
            return "Wildcard topic characters '#' and '+' are not allowed in device topics."
        for other in self.state.devices:
            if other.id == device.id:
                continue
            if other.device_id == device.device_id:
                return "Device ID must be unique."
            if other.up_topic == device.up_topic:
                return "Uplink topic must be unique."
            if other.down_topic == device.down_topic:
                return "Downlink topic must be unique."
        return None

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._persist_state()
        self.mqtt.disconnect_client()
        super().closeEvent(event)
