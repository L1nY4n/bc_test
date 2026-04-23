from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4


def new_id() -> str:
    return uuid4().hex


class LogDirection(str, Enum):
    TX = "TX"
    RX = "RX"
    SYSTEM = "SYSTEM"


class TransferKind(str, Enum):
    BC_OTA = "BC OTA"
    A_OTA = "A OTA"
    VOICE_FILE = "Voice File"
    REALTIME_VOICE = "Realtime Voice"


@dataclass(slots=True)
class BrokerProfile:
    id: str = field(default_factory=new_id)
    name: str = "Default Broker"
    host: str = "127.0.0.1"
    port: int = 1883
    username: str = ""
    password: str = ""
    client_id: str = "mesh-bc-tester"
    keepalive: int = 60
    use_tls: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BrokerProfile":
        return cls(**data)

    def to_dict(self, *, include_password: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if not include_password:
            data["password"] = ""
        return data


@dataclass(slots=True)
class DeviceProfile:
    id: str = field(default_factory=new_id)
    name: str = "New Device"
    device_id: str = ""
    up_topic: str = ""
    down_topic: str = ""
    mesh_dev_type: int = 1
    default_dest_addr: int = 1
    subscribe_enabled: bool = True

    @classmethod
    def default_topics(cls, device_id: str) -> tuple[str, str]:
        root = f"/application/AP-C-BM/device/{device_id}"
        return f"{root}/up", f"{root}/down"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DeviceProfile":
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DeviceRuntimeState:
    online: bool = False
    last_seen: str = "-"
    tx_count: int = 0
    rx_count: int = 0
    last_opcode: str = "-"
    last_result: str = "-"
    last_summary: str = "-"


@dataclass(slots=True)
class LogEntry:
    timestamp: str
    direction: LogDirection
    device_name: str
    device_id: str
    topic: str
    payload: str
    opcode: str = "-"
    status: str = ""
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "direction": self.direction.value,
            "device_name": self.device_name,
            "device_id": self.device_id,
            "topic": self.topic,
            "payload": self.payload,
            "opcode": self.opcode,
            "status": self.status,
            "summary": self.summary,
        }


@dataclass(slots=True)
class PendingRequest:
    request_id: str
    device_id: str
    device_name: str
    opcode: int
    expected_opcode: int
    topic: str
    time_stamp: int | None
    timeout_seconds: int
    sent_monotonic: float


@dataclass(slots=True)
class TransferJob:
    id: str
    device_id: str
    device_name: str
    kind: TransferKind
    file_path: str
    status: str
    total_packets: int
    sent_packets: int = 0
    total_bytes: int = 0
    sent_bytes: int = 0
    last_error: str = ""

    @property
    def progress_percent(self) -> int:
        if self.total_bytes <= 0:
            return 0
        return int((self.sent_bytes / self.total_bytes) * 100)


@dataclass(slots=True)
class AppState:
    brokers: list[BrokerProfile] = field(default_factory=list)
    devices: list[DeviceProfile] = field(default_factory=list)
    selected_broker_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppState":
        return cls(
            brokers=[BrokerProfile.from_dict(item) for item in data.get("brokers", [])],
            devices=[DeviceProfile.from_dict(item) for item in data.get("devices", [])],
            selected_broker_id=data.get("selected_broker_id"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "brokers": [broker.to_dict() for broker in self.brokers],
            "devices": [device.to_dict() for device in self.devices],
            "selected_broker_id": self.selected_broker_id,
        }
