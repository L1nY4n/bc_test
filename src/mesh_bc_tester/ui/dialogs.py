from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QSpinBox,
    QVBoxLayout,
)

from ..models import BrokerProfile, DeviceProfile


class BrokerDialog(QDialog):
    def __init__(self, broker: BrokerProfile | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Broker Profile")
        self._broker = broker

        self.name_edit = QLineEdit(broker.name if broker else "Default Broker")
        self.host_edit = QLineEdit(broker.host if broker else "127.0.0.1")
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(broker.port if broker else 1883)
        self.username_edit = QLineEdit(broker.username if broker else "")
        self.password_edit = QLineEdit(broker.password if broker else "")
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.client_id_edit = QLineEdit(broker.client_id if broker else "mesh-bc-tester")
        self.keepalive_spin = QSpinBox()
        self.keepalive_spin.setRange(5, 3600)
        self.keepalive_spin.setValue(broker.keepalive if broker else 60)
        self.tls_checkbox = QCheckBox("Use TLS")
        self.tls_checkbox.setChecked(broker.use_tls if broker else False)

        form = QFormLayout()
        form.addRow("Name", self.name_edit)
        form.addRow("Host", self.host_edit)
        form.addRow("Port", self.port_spin)
        form.addRow("Username", self.username_edit)
        form.addRow("Password", self.password_edit)
        form.addRow("Client ID", self.client_id_edit)
        form.addRow("Keepalive", self.keepalive_spin)
        form.addRow("", self.tls_checkbox)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)

    def value(self) -> BrokerProfile:
        broker = self._broker or BrokerProfile()
        broker.name = self.name_edit.text().strip() or "Default Broker"
        broker.host = self.host_edit.text().strip() or "127.0.0.1"
        broker.port = self.port_spin.value()
        broker.username = self.username_edit.text().strip()
        broker.password = self.password_edit.text()
        broker.client_id = self.client_id_edit.text().strip() or "mesh-bc-tester"
        broker.keepalive = self.keepalive_spin.value()
        broker.use_tls = self.tls_checkbox.isChecked()
        return broker


class DeviceDialog(QDialog):
    def __init__(self, device: DeviceProfile | None = None, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Device Profile")
        self._device = device

        self.name_edit = QLineEdit(device.name if device else "")
        self.device_id_edit = QLineEdit(device.device_id if device else "")
        self.up_topic_edit = QLineEdit(device.up_topic if device else "")
        self.down_topic_edit = QLineEdit(device.down_topic if device else "")
        self.mesh_type_spin = QSpinBox()
        self.mesh_type_spin.setRange(0, 255)
        self.mesh_type_spin.setValue(device.mesh_dev_type if device else 1)
        self.dest_addr_spin = QSpinBox()
        self.dest_addr_spin.setRange(1, 0xFFFF)
        self.dest_addr_spin.setValue(device.default_dest_addr if device else 1)
        self.subscribe_checkbox = QCheckBox("Subscribe to uplink topic")
        self.subscribe_checkbox.setChecked(device.subscribe_enabled if device else True)
        self.device_id_edit.textChanged.connect(self._sync_default_topics)

        form = QFormLayout()
        form.addRow("Name", self.name_edit)
        form.addRow("Device ID", self.device_id_edit)
        form.addRow("Uplink Topic", self.up_topic_edit)
        form.addRow("Downlink Topic", self.down_topic_edit)
        form.addRow("Mesh Device Type", self.mesh_type_spin)
        form.addRow("Default Dest Addr", self.dest_addr_spin)
        form.addRow("", self.subscribe_checkbox)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(buttons)

        if not device:
            self._sync_default_topics(self.device_id_edit.text())

    def _sync_default_topics(self, text: str) -> None:
        device_id = text.strip()
        if not device_id:
            return
        up_topic, down_topic = DeviceProfile.default_topics(device_id)
        if not self.up_topic_edit.text().strip():
            self.up_topic_edit.setText(up_topic)
        if not self.down_topic_edit.text().strip():
            self.down_topic_edit.setText(down_topic)

    def value(self) -> DeviceProfile:
        device = self._device or DeviceProfile()
        device.name = self.name_edit.text().strip() or self.device_id_edit.text().strip() or "Device"
        device.device_id = self.device_id_edit.text().strip()
        device.up_topic = self.up_topic_edit.text().strip()
        device.down_topic = self.down_topic_edit.text().strip()
        device.mesh_dev_type = self.mesh_type_spin.value()
        device.default_dest_addr = self.dest_addr_spin.value()
        device.subscribe_enabled = self.subscribe_checkbox.isChecked()
        return device
