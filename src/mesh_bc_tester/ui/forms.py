from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ..models import TransferKind
from ..protocol import COMMAND_SPECS, COMMAND_MAP, CommandSpec, FieldSpec, build_command_payload, format_json


class CommandPanel(QWidget):
    send_requested = Signal(object, dict)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._field_widgets: dict[str, Any] = {}
        self._preview_device = None

        self.command_combo = QComboBox()
        for spec in COMMAND_SPECS:
            self.command_combo.addItem(spec.label, spec.key)
        self.description_label = QLabel()
        self.description_label.setWordWrap(True)
        self.form_box = QGroupBox("Command Parameters")
        self.form_layout = QFormLayout(self.form_box)
        self.preview = QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.send_button = QPushButton("Send To Selected Devices")

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Command"))
        layout.addWidget(self.command_combo)
        layout.addWidget(self.description_label)
        layout.addWidget(self.form_box)
        layout.addWidget(QLabel("Preview"))
        layout.addWidget(self.preview, 1)
        layout.addWidget(self.send_button)

        self.command_combo.currentIndexChanged.connect(self._rebuild_form)
        self.send_button.clicked.connect(self._emit_send_requested)
        self._rebuild_form()

    @property
    def current_spec(self) -> CommandSpec:
        return COMMAND_MAP[self.command_combo.currentData()]

    def update_preview_for_device(self, device) -> None:
        self._preview_device = device
        if device is None:
            self.preview.setPlainText("{}")
            return
        try:
            payload = build_command_payload(self.current_spec, device, self.values())
            self.preview.setPlainText(format_json(payload))
        except Exception as exc:  # noqa: BLE001
            self.preview.setPlainText(f"Preview unavailable: {exc}")

    def values(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for field_name, widget in self._field_widgets.items():
            if isinstance(widget, QSpinBox):
                values[field_name] = widget.value()
            elif isinstance(widget, QComboBox):
                values[field_name] = widget.currentData()
            elif isinstance(widget, QLineEdit):
                values[field_name] = widget.text()
        return values

    def _clear_form(self) -> None:
        while self.form_layout.count():
            item = self.form_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._field_widgets.clear()

    def _widget_for_field(self, field_spec: FieldSpec):
        if field_spec.field_type == "int":
            if (field_spec.minimum is not None and field_spec.minimum < -2147483648) or (
                field_spec.maximum is not None and field_spec.maximum > 2147483647
            ):
                widget = QLineEdit()
                if field_spec.default is not None:
                    widget.setText(str(field_spec.default))
                return widget
            widget = QSpinBox()
            widget.setRange(field_spec.minimum or 0, field_spec.maximum or 0x7FFFFFFF)
            if field_spec.default is not None:
                widget.setValue(int(field_spec.default))
            return widget
        if field_spec.field_type == "choice":
            widget = QComboBox()
            for choice in field_spec.choices:
                widget.addItem(choice.label, choice.value)
            default_index = next(
                (index for index, choice in enumerate(field_spec.choices) if choice.value == field_spec.default),
                0,
            )
            widget.setCurrentIndex(default_index)
            return widget
        widget = QLineEdit()
        if field_spec.default not in (None, ""):
            widget.setText(str(field_spec.default))
        if field_spec.placeholder:
            widget.setPlaceholderText(field_spec.placeholder)
        return widget

    def _rebuild_form(self) -> None:
        self._clear_form()
        spec = self.current_spec
        self.description_label.setText(spec.description)
        if spec.include_dest_addr:
            dest_widget = QSpinBox()
            dest_widget.setRange(1, 0xFFFF)
            dest_widget.setValue(1)
            self.form_layout.addRow("Dest Addr", dest_widget)
            self._field_widgets["dest_addr"] = dest_widget
        for field_spec in spec.fields:
            widget = self._widget_for_field(field_spec)
            self.form_layout.addRow(field_spec.label, widget)
            self._field_widgets[field_spec.name] = widget
            signal = getattr(widget, "valueChanged", None) or getattr(widget, "currentIndexChanged", None) or getattr(widget, "textChanged", None)
            if signal is not None:
                signal.connect(self._update_preview)
        self.update_preview_for_device(None)

    def _emit_send_requested(self) -> None:
        self.send_requested.emit(self.current_spec, self.values())

    def _update_preview(self, *_args) -> None:
        self.update_preview_for_device(self._preview_device)


class RawJsonPanel(QWidget):
    send_requested = Signal(dict, bool)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.editor = QPlainTextEdit()
        self.editor.setPlainText('{\n  "opcode": 70,\n  "value": 0,\n  "time_stamp": 0\n}')
        self.auto_timestamp_combo = QComboBox()
        self.auto_timestamp_combo.addItem("Auto-fill time_stamp when missing", True)
        self.auto_timestamp_combo.addItem("Send payload unchanged", False)
        self.send_button = QPushButton("Send Raw JSON To Selected Devices")

        layout = QVBoxLayout(self)
        layout.addWidget(self.auto_timestamp_combo)
        layout.addWidget(self.editor, 1)
        layout.addWidget(self.send_button)

        self.send_button.clicked.connect(self._emit_send)

    def _emit_send(self) -> None:
        try:
            payload = json.loads(self.editor.toPlainText())
        except json.JSONDecodeError as exc:
            QMessageBox.warning(self, "Invalid JSON", str(exc))
            return
        if not isinstance(payload, dict):
            QMessageBox.warning(self, "Invalid JSON", "Raw payload must be a JSON object.")
            return
        self.send_requested.emit(payload, bool(self.auto_timestamp_combo.currentData()))


class TransferPanel(QWidget):
    start_requested = Signal(dict)
    cancel_requested = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.kind_combo = QComboBox()
        for kind in TransferKind:
            self.kind_combo.addItem(kind.value, kind)
        self.file_edit = QLineEdit()
        self.file_button = QPushButton("Browse")
        self.version_spin = QSpinBox()
        self.version_spin.setRange(0, 255)
        self.version_spin.setValue(1)
        self.voice_name_edit = QLineEdit("voice.adpcm")
        self.packet_interval_spin = QSpinBox()
        self.packet_interval_spin.setRange(1, 10000)
        self.packet_interval_spin.setValue(60)
        self.start_button = QPushButton("Start Transfer For Selected Devices")
        self.cancel_button = QPushButton("Cancel Selected Transfer")

        form = QFormLayout()
        file_row = QHBoxLayout()
        file_row.addWidget(self.file_edit, 1)
        file_row.addWidget(self.file_button)
        form.addRow("Transfer Type", self.kind_combo)
        file_widget = QWidget()
        file_widget.setLayout(file_row)
        form.addRow("File", file_widget)
        form.addRow("Version", self.version_spin)
        form.addRow("Voice Name", self.voice_name_edit)
        form.addRow("Packet Interval (ms)", self.packet_interval_spin)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(self.start_button)
        layout.addWidget(self.cancel_button)
        layout.addStretch(1)

        self.file_button.clicked.connect(self._pick_file)
        self.start_button.clicked.connect(self._emit_start)
        self.cancel_button.clicked.connect(self.cancel_requested.emit)

    def _pick_file(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Transfer File", str(Path.cwd()))
        if file_path:
            self.file_edit.setText(file_path)

    def _emit_start(self) -> None:
        self.start_requested.emit(
            {
                "kind": self.kind_combo.currentData(),
                "file_path": self.file_edit.text().strip(),
                "version": self.version_spin.value(),
                "voice_name": self.voice_name_edit.text().strip() or "voice.adpcm",
                "packet_interval_ms": self.packet_interval_spin.value(),
            }
        )
