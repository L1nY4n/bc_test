from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .models import DeviceProfile, TransferKind


def current_time_stamp() -> int:
    return int(time.time())


def bytes_to_hex(data: bytes) -> str:
    return data.hex().upper()


def hex_to_bytes(value: str) -> bytes:
    cleaned = "".join(value.strip().split()).upper()
    if len(cleaned) % 2:
        raise ValueError("Hex string length must be even.")
    return bytes.fromhex(cleaned)


def crc16_xmodem(data: bytes) -> int:
    crc = 0x0000
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


def crc16_xmodem_bytes_little_endian(data: bytes) -> bytes:
    crc = crc16_xmodem(data)
    return bytes((crc & 0xFF, (crc >> 8) & 0xFF))


def chunk_bytes(data: bytes, chunk_size: int = 400) -> list[bytes]:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than zero.")
    return [data[index : index + chunk_size] for index in range(0, len(data), chunk_size)]


def transfer_chunk_count(data: bytes, chunk_size: int = 400) -> int:
    if chunk_size <= 0:
        raise ValueError("Chunk size must be greater than zero.")
    if not data:
        raise ValueError("Transfer data cannot be empty.")
    return (len(data) + chunk_size - 1) // chunk_size


@dataclass(frozen=True, slots=True)
class Choice:
    value: int | str
    label: str


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    label: str
    field_type: str = "text"
    required: bool = True
    default: Any = None
    minimum: int | None = None
    maximum: int | None = None
    placeholder: str = ""
    choices: tuple[Choice, ...] = ()


PayloadBuilder = Callable[[DeviceProfile, dict[str, Any], "CommandSpec"], dict[str, Any]]


@dataclass(frozen=True, slots=True)
class CommandSpec:
    key: str
    label: str
    opcode: int
    response_opcode: int
    description: str
    fields: tuple[FieldSpec, ...] = field(default_factory=tuple)
    include_dest_addr: bool = True
    include_mesh_dev_type: bool = True
    include_time_stamp: bool = True
    payload_builder: PayloadBuilder | None = None


def _int_field(name: str, label: str, default: int, minimum: int, maximum: int) -> FieldSpec:
    return FieldSpec(name=name, label=label, field_type="int", default=default, minimum=minimum, maximum=maximum)


def _choice_field(name: str, label: str, default: int | str, *choices: tuple[int | str, str]) -> FieldSpec:
    return FieldSpec(
        name=name,
        label=label,
        field_type="choice",
        default=default,
        choices=tuple(Choice(value=value, label=label_text) for value, label_text in choices),
    )


def _normalize_value(field_spec: FieldSpec, value: Any) -> Any:
    if field_spec.field_type == "int":
        return int(value)
    if field_spec.field_type == "choice":
        return value
    return str(value).strip()


def _generic_builder(device: DeviceProfile, values: dict[str, Any], spec: CommandSpec) -> dict[str, Any]:
    payload: dict[str, Any] = {"opcode": spec.opcode}
    if spec.include_mesh_dev_type:
        payload["mesh_dev_type"] = device.mesh_dev_type
    if spec.include_dest_addr:
        payload["dest_addr"] = int(values.get("dest_addr") or device.default_dest_addr)
    if spec.include_time_stamp:
        payload["time_stamp"] = int(values.get("time_stamp") or current_time_stamp())
    for field in spec.fields:
        if field.name == "dest_addr":
            continue
        if field.name == "time_stamp" and spec.include_time_stamp:
            continue
        value = values.get(field.name, field.default)
        if value in (None, "") and field.required:
            raise ValueError(f"Field '{field.label}' is required.")
        if value in (None, ""):
            continue
        payload[field.name] = _normalize_value(field, value)
    return payload


def _scene_definition_builder(device: DeviceProfile, values: dict[str, Any], spec: CommandSpec) -> dict[str, Any]:
    payload = _generic_builder(device, values, spec)
    scene_bytes = bytes(
        (
            int(values["scene_id"]),
            int(values["occupied_brightness"]),
            int(values["idle_brightness"]),
            int(values["auto_off_seconds"]),
            int(values["running_mode"]),
            int(values["switch_state"]),
            0,
            0,
            0,
            0,
            0,
            0,
        )
    )
    payload["value"] = bytes_to_hex(scene_bytes)
    return payload


def _network_management_builder(device: DeviceProfile, values: dict[str, Any], spec: CommandSpec) -> dict[str, Any]:
    payload = _generic_builder(device, values, spec)
    payload["group_addr"] = int(values["group_addr"])
    return payload


QUERY_STATUS_CHOICES = (
    (0, "Version"),
    (1, "Running Mode"),
    (3, "Auto Off Delay"),
    (4, "Remote Provisioning State"),
    (5, "Heartbeat Interval"),
    (6, "Intra Group Linkage"),
    (7, "Switch State"),
    (8, "MAC Address"),
    (9, "Linkage Mode"),
    (11, "Energy Report Interval"),
    (12, "Microwave Sensor State"),
    (255, "Reserved"),
)

QUERY_MESH_CHOICES = (
    (0, "Partition + Lane + Adjacent"),
    (1, "Other Group List"),
    (2, "Linkage Group List"),
    (3, "Preferred BC Reporter"),
    (4, "Smart Linkage Adjacent Devices"),
)

COMMAND_SPECS: tuple[CommandSpec, ...] = (
    CommandSpec(
        key="query_bc_info",
        label="Query BC Device Info (0x46)",
        opcode=0x46,
        response_opcode=0x47,
        description="Query BC device version, model, and device identifier.",
        fields=(
            _int_field("value", "Reserved Value", 0, 0, 255),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="query_mesh_info",
        label="Query BC Mesh Info (0x4E)",
        opcode=0x4E,
        response_opcode=0x4F,
        description="Query BC device mesh network information.",
        fields=(
            _int_field("value", "Reserved Value", 0, 0, 255),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="query_a_lights",
        label="Query A-Light List (0x50)",
        opcode=0x50,
        response_opcode=0x51,
        description="Query all A lights or only remote-selected A lights.",
        fields=(
            _choice_field("value", "Query Mode", 0, (0, "All A Lights"), (1, "Remote Selected A Lights")),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="reset_mesh_device",
        label="Reset Mesh Device (0x02)",
        opcode=0x02,
        response_opcode=0x02,
        description="Reset the target mesh device.",
        fields=(_int_field("value", "Reserved Value", 1, 1, 1),),
    ),
    CommandSpec(
        key="switch_control",
        label="Switch Control (0x05)",
        opcode=0x05,
        response_opcode=0x05,
        description="Turn off, turn on, or blink the selected device(s).",
        fields=(
            _choice_field("value", "Switch State", 1, (0, "Off"), (1, "On"), (2, "Blink")),
        ),
    ),
    CommandSpec(
        key="energy_report_interval",
        label="Set Energy Report Interval (0x0F)",
        opcode=0x0F,
        response_opcode=0x0F,
        description="Set the energy report interval in hours.",
        fields=(_int_field("value", "Hours", 1, 0, 120),),
    ),
    CommandSpec(
        key="notify_a_ota",
        label="Notify A-Light OTA (0x11)",
        opcode=0x11,
        response_opcode=0x11,
        description="Notify A lights to upgrade to the specified version.",
        fields=(_int_field("value", "Target Version", 0, 0, 255),),
    ),
    CommandSpec(
        key="auto_off_delay",
        label="Set Auto-Off Delay (0x15)",
        opcode=0x15,
        response_opcode=0x15,
        description="Set the no-person auto-off delay in seconds.",
        fields=(_int_field("value", "Delay Seconds", 10, 10, 255),),
    ),
    CommandSpec(
        key="running_mode",
        label="Set Running Mode (0x17)",
        opcode=0x17,
        response_opcode=0x17,
        description="Switch the device between engineering and sensing modes.",
        fields=(_choice_field("value", "Mode", 2, (2, "Engineering"), (3, "Sensing")),),
    ),
    CommandSpec(
        key="remote_network_enable",
        label="Remote Provisioning Enable (0x19)",
        opcode=0x19,
        response_opcode=0x19,
        description="Enable or disable remote provisioning changes.",
        fields=(_choice_field("value", "Enabled", 1, (0, "Disabled"), (1, "Enabled")),),
    ),
    CommandSpec(
        key="heartbeat_interval",
        label="Set Heartbeat Interval (0x1A)",
        opcode=0x1A,
        response_opcode=0x1A,
        description="Set heartbeat report period in seconds.",
        fields=(_int_field("value", "Interval Seconds", 30, 0, 180),),
    ),
    CommandSpec(
        key="query_device_status",
        label="Query Device Status (0x1C)",
        opcode=0x1C,
        response_opcode=0x1D,
        description="Query a specific status field from the target device.",
        fields=(_choice_field("value", "Query Code", 0, *QUERY_STATUS_CHOICES),),
    ),
    CommandSpec(
        key="query_mesh_management",
        label="Query Mesh Management (0x1E)",
        opcode=0x1E,
        response_opcode=0x1F,
        description="Query mesh management addresses and relationship lists.",
        fields=(_choice_field("value", "Query Code", 0, *QUERY_MESH_CHOICES),),
    ),
    CommandSpec(
        key="switch_scene",
        label="Switch Scene (0x20)",
        opcode=0x20,
        response_opcode=0x20,
        description="Switch to a scene index or emergency/fire scene.",
        fields=(_int_field("value", "Scene ID", 0, 0, 255),),
    ),
    CommandSpec(
        key="query_current_scene",
        label="Query Current Scene (0x21)",
        opcode=0x21,
        response_opcode=0x22,
        description="Query the currently running scene parameters.",
        fields=(_int_field("value", "Reserved Value", 0, 0, 0),),
    ),
    CommandSpec(
        key="upsert_scene",
        label="Create or Update Scene (0x23)",
        opcode=0x23,
        response_opcode=0x23,
        description="Define a scene payload using packed hex bytes.",
        fields=(
            _int_field("scene_id", "Scene ID", 1, 1, 20),
            _int_field("occupied_brightness", "Occupied Brightness", 100, 0, 255),
            _int_field("idle_brightness", "Idle Brightness", 0, 0, 255),
            _int_field("auto_off_seconds", "Auto-Off Seconds", 5, 0, 255),
            _choice_field("running_mode", "Running Mode", 3, (2, "Engineering"), (3, "Sensing")),
            _choice_field("switch_state", "Switch State", 1, (0, "Off"), (1, "On"), (2, "Blink")),
        ),
        payload_builder=_scene_definition_builder,
    ),
    CommandSpec(
        key="delete_scene",
        label="Delete Scene (0x24)",
        opcode=0x24,
        response_opcode=0x24,
        description="Delete a custom scene definition.",
        fields=(_int_field("value", "Scene ID", 1, 1, 20),),
    ),
    CommandSpec(
        key="query_scene",
        label="Query Scene Definitions (0x25)",
        opcode=0x25,
        response_opcode=0x26,
        description="Query one scene or all defined scene identifiers.",
        fields=(_int_field("value", "Scene ID / 255 for all", 255, 0, 255),),
    ),
    CommandSpec(
        key="group_linkage",
        label="Set Intra-Group Linkage (0x27)",
        opcode=0x27,
        response_opcode=0x27,
        description="Enable or disable same-group light-up linkage.",
        fields=(_choice_field("value", "Enabled", 1, (0, "Disabled"), (1, "Enabled")),),
    ),
    CommandSpec(
        key="energy_info",
        label="Query Energy Info (0x28)",
        opcode=0x28,
        response_opcode=0x29,
        description="Query energy information and optionally reset counters.",
        fields=(_choice_field("value", "Reset Counters", 0, (0, "No"), (1, "Yes")),),
    ),
    CommandSpec(
        key="linkage_group_switch",
        label="Set Linkage Group Switch (0x30)",
        opcode=0x30,
        response_opcode=0x30,
        description="Enable or disable linkage-group wakeup behavior.",
        fields=(_choice_field("value", "Enabled", 1, (0, "Disabled"), (1, "Enabled")),),
    ),
    CommandSpec(
        key="query_linkage_group",
        label="Query Linkage Group State (0x31)",
        opcode=0x31,
        response_opcode=0x32,
        description="Query whether a linkage group address is enabled.",
        fields=(_int_field("value", "Group Address", 0xFE00, 0x0001, 0xFFFF),),
    ),
    CommandSpec(
        key="network_management",
        label="Update Mesh Management Entry (0x33)",
        opcode=0x33,
        response_opcode=0x33,
        description="Add, modify, or delete group and reporting address mappings.",
        fields=(
            _choice_field(
                "value",
                "Operation",
                1,
                (0, "Delete Partition"),
                (1, "Add/Update Partition"),
                (2, "Delete Lane Group"),
                (3, "Add/Update Lane Group"),
                (4, "Delete Adjacent Group"),
                (5, "Add/Update Adjacent Group"),
                (6, "Delete Other Group"),
                (7, "Add Other Group"),
                (8, "Delete Linkage Group"),
                (9, "Add Linkage Group"),
                (10, "Delete Reporting BC Address"),
                (11, "Add/Update Reporting BC Address"),
                (12, "Delete Smart Linkage Adjacent Address"),
            ),
            _int_field("group_addr", "Group Address", 0xE000, 0x0000, 0xFFFF),
        ),
        payload_builder=_network_management_builder,
    ),
    CommandSpec(
        key="smart_linkage_mode",
        label="Set Smart Linkage Mode (0x34)",
        opcode=0x34,
        response_opcode=0x34,
        description="Set classic linkage mode or smart linkage hop count.",
        fields=(_int_field("value", "Mode / Hop Count", 0, 0, 10),),
    ),
    CommandSpec(
        key="microwave_sensor",
        label="Set Microwave Sensor (0x35)",
        opcode=0x35,
        response_opcode=0x35,
        description="Disable, enable with sensitivity, or enable without changing sensitivity.",
        fields=(_int_field("value", "Value", 255, 0, 255),),
    ),
    CommandSpec(
        key="set_network",
        label="Set BC Network Config (0x48)",
        opcode=0x48,
        response_opcode=0x49,
        description="Configure static network parameters for the BC device.",
        fields=(
            FieldSpec(name="ipaddr", label="IP Address", default="192.168.0.100"),
            FieldSpec(name="netmask", label="Netmask", default="255.255.255.0"),
            FieldSpec(name="gateway", label="Gateway", default="192.168.0.1"),
            _choice_field("dhcp", "DHCP", 0, (0, "Static"), (1, "Auto")),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="set_mqtt_service",
        label="Set BC MQTT Service (0x4A)",
        opcode=0x4A,
        response_opcode=0x4B,
        description="Configure MQTT server settings on the BC device.",
        fields=(
            FieldSpec(name="mqtt_addr", label="MQTT Host", default="192.168.0.200"),
            FieldSpec(name="mqtt_port", label="MQTT Port", field_type="int", default=1883, minimum=1, maximum=65535),
            FieldSpec(name="mqtt_username", label="MQTT Username", default=""),
            FieldSpec(name="mqtt_password", label="MQTT Password", default=""),
            FieldSpec(name="mqtt_client_id", label="MQTT Client ID", default="device-client"),
            FieldSpec(name="mqtt_subscribe_topic", label="MQTT Subscribe Topic", default="/application/AP-C-BM/device/DEVICE/down"),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="set_dns",
        label="Set DNS (0x52)",
        opcode=0x52,
        response_opcode=0x53,
        description="Configure primary and secondary DNS servers.",
        fields=(
            FieldSpec(name="dns1", label="Primary DNS", default="114.114.114.114"),
            FieldSpec(name="dns2", label="Secondary DNS", default="8.8.8.8"),
            _int_field("time_stamp", "Time Stamp", current_time_stamp(), 0, 0xFFFFFFFF),
        ),
        include_dest_addr=False,
        include_mesh_dev_type=False,
        include_time_stamp=False,
    ),
    CommandSpec(
        key="speaker_volume",
        label="Speaker Switch / Volume (0x62)",
        opcode=0x62,
        response_opcode=0x63,
        description="Turn the speaker off or set volume 1 to 10.",
        fields=(_int_field("value", "Volume (0=Off)", 5, 0, 10),),
        include_dest_addr=False,
        include_mesh_dev_type=False,
    ),
)

COMMAND_MAP = {spec.key: spec for spec in COMMAND_SPECS}


def build_command_payload(spec: CommandSpec, device: DeviceProfile, values: dict[str, Any]) -> dict[str, Any]:
    builder = spec.payload_builder or _generic_builder
    return builder(device, values, spec)


def expected_response_opcode(opcode: int) -> int:
    for spec in COMMAND_SPECS:
        if spec.opcode == opcode:
            return spec.response_opcode
    return opcode


def parse_opcode(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    return int(text, 16) if text.lower().startswith("0x") else int(text)


def summarize_payload(payload: dict[str, Any]) -> str:
    opcode = payload.get("opcode")
    value = payload.get("value")
    if opcode == 0x1B and isinstance(value, str):
        data = hex_to_bytes(value)
        if len(data) >= 4:
            return f"heartbeat switch={data[0]} mode={data[1]} version=0x{data[2]:02X} type={data[3]}"
    if opcode == 0x1D and isinstance(value, str):
        data = hex_to_bytes(value)
        if len(data) >= 2:
            return f"status query={data[0]} value=0x{data[1]:02X}"
    if opcode in {0x47, 0x49, 0x4B, 0x4F, 0x53, 0x63}:
        return str(value)
    if isinstance(value, list):
        return f"value_array[{len(value)}]"
    if isinstance(value, str) and len(value) > 48:
        return value[:48] + "..."
    return json.dumps(payload, ensure_ascii=False)[:96]


def build_transfer_packets(kind: TransferKind, data: bytes, *, version: int, voice_name: str) -> list[tuple[dict[str, Any], int]]:
    if not data:
        raise ValueError("Transfer data cannot be empty.")
    return [
        build_transfer_packet(kind, data, packet_index, version=version, voice_name=voice_name)
        for packet_index in range(transfer_total_packets(kind, data))
    ]


def transfer_total_packets(kind: TransferKind, data: bytes, chunk_size: int = 400) -> int:
    chunk_count = transfer_chunk_count(data, chunk_size)
    if kind in {TransferKind.BC_OTA, TransferKind.A_OTA}:
        return 1 + chunk_count
    if kind in {TransferKind.VOICE_FILE, TransferKind.REALTIME_VOICE}:
        return 2 + chunk_count
    raise ValueError(f"Unsupported transfer type: {kind}")


def build_transfer_packet(
    kind: TransferKind,
    data: bytes,
    packet_index: int,
    *,
    version: int,
    voice_name: str,
    chunk_size: int = 400,
) -> tuple[dict[str, Any], int]:
    if not data:
        raise ValueError("Transfer data cannot be empty.")
    chunk_count = transfer_chunk_count(data, chunk_size)

    if kind == TransferKind.BC_OTA:
        if packet_index == 0:
            return (
                {
                    "opcode": 0x40,
                    "version": int(version),
                    "ota_pack_count": chunk_count,
                    "ota_total_size": len(data),
                },
                0,
            )
        chunk_id = packet_index - 1
        chunk = data[chunk_id * chunk_size : (chunk_id + 1) * chunk_size]
        return (
            {
                "opcode": 0x42,
                "version": int(version),
                "ota_data_index": chunk_id,
                "ota_byte_size": len(chunk),
                "value": bytes_to_hex(chunk),
            },
            len(chunk),
        )

    if kind == TransferKind.A_OTA:
        if packet_index == 0:
            return (
                {
                    "opcode": 0x43,
                    "version": int(version),
                    "ota_pack_count": chunk_count,
                    "ota_total_size": len(data),
                },
                0,
            )
        chunk_id = packet_index - 1
        chunk = data[chunk_id * chunk_size : (chunk_id + 1) * chunk_size]
        return (
            {
                "opcode": 0x45,
                "version": int(version),
                "ota_data_index": chunk_id,
                "ota_byte_size": len(chunk),
                "value": bytes_to_hex(chunk),
            },
            len(chunk),
        )

    if kind == TransferKind.VOICE_FILE:
        start_opcode, chunk_opcode, end_opcode = 0x54, 0x56, 0x58
    elif kind == TransferKind.REALTIME_VOICE:
        start_opcode, chunk_opcode, end_opcode = 0x5C, 0x5E, 0x60
    else:
        raise ValueError(f"Unsupported transfer type: {kind}")

    if packet_index == 0:
        return (
            {
                "opcode": start_opcode,
                "voice_name": voice_name,
                "totle_len": len(data),
                "value": 1,
                "time_stamp": current_time_stamp(),
            },
            0,
        )
    if packet_index == chunk_count + 1:
        return (
            {
                "opcode": end_opcode,
                "voice_name": voice_name,
                "totle_len": len(data),
                "check_sum": crc16_xmodem(data),
                "time_stamp": current_time_stamp(),
            },
            0,
        )
    chunk_id = packet_index - 1
    chunk = data[chunk_id * chunk_size : (chunk_id + 1) * chunk_size]
    return (
        {
            "opcode": chunk_opcode,
            "voice_name": voice_name,
            "packet_index": chunk_id,
            "packet_len": len(chunk),
            "value": bytes_to_hex(chunk),
            "time_stamp": current_time_stamp(),
        },
        len(chunk),
    )


def format_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


SENSITIVE_KEYS = {"password", "mqtt_password"}


def redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("***" if key in SENSITIVE_KEYS and item not in (None, "") else redact_secrets(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    return value


def redact_json_text(payload_text: str) -> str:
    try:
        parsed = json.loads(payload_text)
    except json.JSONDecodeError:
        return payload_text
    if isinstance(parsed, dict):
        return format_json(redact_secrets(parsed))
    return payload_text
