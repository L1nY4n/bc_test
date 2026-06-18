use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::models::{
    BytesTransferFrame, DeviceProfile, OtaTransferFormat, TransferKind, TransferPacket,
    TransferPayload,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    Integer,
    Choice(&'static [Choice]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Choice {
    pub value: &'static str,
    pub label: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldSpec {
    pub key: &'static str,
    pub label: &'static str,
    pub kind: FieldKind,
    pub default: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    Generic,
    ScenePacked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandSpec {
    pub key: &'static str,
    pub label: &'static str,
    pub opcode: u32,
    pub response_opcode: u32,
    pub include_dest_addr: bool,
    pub include_mesh_dev_type: bool,
    pub fields: &'static [FieldSpec],
    pub build_mode: BuildMode,
}

const SWITCH_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "关灯",
    },
    Choice {
        value: "1",
        label: "开灯",
    },
    Choice {
        value: "2",
        label: "闪烁",
    },
];

const STATUS_QUERY_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "版本",
    },
    Choice {
        value: "1",
        label: "运行模式",
    },
    Choice {
        value: "3",
        label: "无人关灯延时",
    },
    Choice {
        value: "4",
        label: "遥控组网状态",
    },
    Choice {
        value: "5",
        label: "心跳周期",
    },
    Choice {
        value: "6",
        label: "组内联动",
    },
    Choice {
        value: "7",
        label: "开关状态",
    },
    Choice {
        value: "8",
        label: "MAC地址",
    },
    Choice {
        value: "9",
        label: "联动模式",
    },
    Choice {
        value: "11",
        label: "能耗上报周期",
    },
    Choice {
        value: "12",
        label: "人体微波状态",
    },
];

const RUN_MODE_CHOICES: &[Choice] = &[
    Choice {
        value: "2",
        label: "工程模式",
    },
    Choice {
        value: "3",
        label: "感应模式",
    },
];

const DHCP_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "静态",
    },
    Choice {
        value: "1",
        label: "自动",
    },
];

const START_STOP_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "停止",
    },
    Choice {
        value: "1",
        label: "开始",
    },
];

const BINARY_ENABLE_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "禁用",
    },
    Choice {
        value: "1",
        label: "启用",
    },
];

const QUERY_MESH_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "分区/车道组/相邻组",
    },
    Choice {
        value: "1",
        label: "其他组列表",
    },
    Choice {
        value: "2",
        label: "联动组列表",
    },
    Choice {
        value: "3",
        label: "上报BC灯地址",
    },
    Choice {
        value: "4",
        label: "智能联动相邻灯",
    },
];

const QUERY_A_LIGHT_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "全部A灯",
    },
    Choice {
        value: "1",
        label: "遥控选中A灯",
    },
];

const RESET_COUNTER_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "不重置",
    },
    Choice {
        value: "1",
        label: "重置",
    },
];

const NETWORK_MGMT_OPERATION_CHOICES: &[Choice] = &[
    Choice {
        value: "0",
        label: "删除分区",
    },
    Choice {
        value: "1",
        label: "新增/修改分区",
    },
    Choice {
        value: "2",
        label: "删除车道组",
    },
    Choice {
        value: "3",
        label: "新增/修改车道组",
    },
    Choice {
        value: "4",
        label: "删除相邻组",
    },
    Choice {
        value: "5",
        label: "新增/修改相邻组",
    },
    Choice {
        value: "6",
        label: "删除其他组",
    },
    Choice {
        value: "7",
        label: "新增其他组",
    },
    Choice {
        value: "8",
        label: "删除联动组",
    },
    Choice {
        value: "9",
        label: "新增联动组",
    },
    Choice {
        value: "10",
        label: "删除上报BC灯地址",
    },
    Choice {
        value: "11",
        label: "新增/修改上报BC灯地址",
    },
    Choice {
        value: "12",
        label: "删除智能联动相邻灯",
    },
];

pub const COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        key: "query_bc_info",
        label: "查询BC设备信息 (0x46)",
        opcode: 0x46,
        response_opcode: 0x47,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "value",
                label: "保留值",
                kind: FieldKind::Integer,
                default: "0",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_mesh_info",
        label: "查询BC Mesh组网信息 (0x4E)",
        opcode: 0x4E,
        response_opcode: 0x4F,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "value",
                label: "保留值",
                kind: FieldKind::Integer,
                default: "0",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_a_lights",
        label: "查询A灯列表 (0x50)",
        opcode: 0x50,
        response_opcode: 0x51,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "value",
                label: "查询模式",
                kind: FieldKind::Choice(QUERY_A_LIGHT_CHOICES),
                default: "0",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "reset_mesh_device",
        label: "复位蓝牙Mesh设备 (0x02)",
        opcode: 0x02,
        response_opcode: 0x02,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "保留值",
            kind: FieldKind::Integer,
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "switch_control",
        label: "开关灯控制 (0x05)",
        opcode: 0x05,
        response_opcode: 0x05,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "开关状态",
            kind: FieldKind::Choice(SWITCH_CHOICES),
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "energy_report_interval",
        label: "设置能耗上报周期 (0x0F)",
        opcode: 0x0F,
        response_opcode: 0x0F,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "周期(小时)",
            kind: FieldKind::Integer,
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "notify_a_ota",
        label: "通知A灯 OTA升级 (0x11)",
        opcode: 0x11,
        response_opcode: 0x11,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "目标版本",
            kind: FieldKind::Integer,
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "auto_off_delay",
        label: "设置无人关灯延时 (0x15)",
        opcode: 0x15,
        response_opcode: 0x15,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "延时秒数",
            kind: FieldKind::Integer,
            default: "10",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "running_mode",
        label: "设置运行模式 (0x17)",
        opcode: 0x17,
        response_opcode: 0x17,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "运行模式",
            kind: FieldKind::Choice(RUN_MODE_CHOICES),
            default: "3",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "remote_network_enable",
        label: "设置遥控组网使能 (0x19)",
        opcode: 0x19,
        response_opcode: 0x19,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "使能状态",
            kind: FieldKind::Choice(BINARY_ENABLE_CHOICES),
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "heartbeat_interval",
        label: "设置心跳周期 (0x1A)",
        opcode: 0x1A,
        response_opcode: 0x1A,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "周期秒数",
            kind: FieldKind::Integer,
            default: "30",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_device_status",
        label: "查询设备状态 (0x1C)",
        opcode: 0x1C,
        response_opcode: 0x1D,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "查询码",
            kind: FieldKind::Choice(STATUS_QUERY_CHOICES),
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_mesh_management",
        label: "查询组网管理信息 (0x1E)",
        opcode: 0x1E,
        response_opcode: 0x1F,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "查询码",
            kind: FieldKind::Choice(QUERY_MESH_CHOICES),
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "switch_scene",
        label: "切换场景 (0x20)",
        opcode: 0x20,
        response_opcode: 0x20,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "场景编号",
            kind: FieldKind::Integer,
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_current_scene",
        label: "查询当前运行场景 (0x21)",
        opcode: 0x21,
        response_opcode: 0x22,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "保留值",
            kind: FieldKind::Integer,
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "upsert_scene",
        label: "新增或修改场景 (0x23)",
        opcode: 0x23,
        response_opcode: 0x23,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[
            FieldSpec {
                key: "scene_id",
                label: "场景编号",
                kind: FieldKind::Integer,
                default: "1",
            },
            FieldSpec {
                key: "occupied_brightness",
                label: "有人亮度",
                kind: FieldKind::Integer,
                default: "100",
            },
            FieldSpec {
                key: "idle_brightness",
                label: "无人亮度",
                kind: FieldKind::Integer,
                default: "0",
            },
            FieldSpec {
                key: "auto_off_seconds",
                label: "无人关灯延时",
                kind: FieldKind::Integer,
                default: "5",
            },
            FieldSpec {
                key: "running_mode",
                label: "运行模式",
                kind: FieldKind::Choice(RUN_MODE_CHOICES),
                default: "3",
            },
            FieldSpec {
                key: "switch_state",
                label: "开关状态",
                kind: FieldKind::Choice(SWITCH_CHOICES),
                default: "1",
            },
        ],
        build_mode: BuildMode::ScenePacked,
    },
    CommandSpec {
        key: "delete_scene",
        label: "删除场景定义 (0x24)",
        opcode: 0x24,
        response_opcode: 0x24,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "场景编号",
            kind: FieldKind::Integer,
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_scene",
        label: "查询场景定义 (0x25)",
        opcode: 0x25,
        response_opcode: 0x26,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "场景编号/255全部",
            kind: FieldKind::Integer,
            default: "255",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "group_linkage",
        label: "设置组内联动开关 (0x27)",
        opcode: 0x27,
        response_opcode: 0x27,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "联动状态",
            kind: FieldKind::Choice(BINARY_ENABLE_CHOICES),
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "energy_info",
        label: "查询能耗信息 (0x28)",
        opcode: 0x28,
        response_opcode: 0x29,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "是否重置计数",
            kind: FieldKind::Choice(RESET_COUNTER_CHOICES),
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "linkage_group_switch",
        label: "设置联动组开关 (0x30)",
        opcode: 0x30,
        response_opcode: 0x30,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "联动状态",
            kind: FieldKind::Choice(BINARY_ENABLE_CHOICES),
            default: "1",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "query_linkage_group",
        label: "查询联动组状态 (0x31)",
        opcode: 0x31,
        response_opcode: 0x32,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "联动组地址",
            kind: FieldKind::Integer,
            default: "65024",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "network_management",
        label: "修改组网管理信息 (0x33)",
        opcode: 0x33,
        response_opcode: 0x33,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[
            FieldSpec {
                key: "value",
                label: "操作类型",
                kind: FieldKind::Choice(NETWORK_MGMT_OPERATION_CHOICES),
                default: "1",
            },
            FieldSpec {
                key: "group_addr",
                label: "组地址",
                kind: FieldKind::Integer,
                default: "57344",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "smart_linkage_mode",
        label: "设置智能联动模式 (0x34)",
        opcode: 0x34,
        response_opcode: 0x34,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "模式/联动跳数",
            kind: FieldKind::Integer,
            default: "0",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "microwave_sensor",
        label: "设置人体微波开关/灵敏度 (0x35)",
        opcode: 0x35,
        response_opcode: 0x35,
        include_dest_addr: true,
        include_mesh_dev_type: true,
        fields: &[FieldSpec {
            key: "value",
            label: "值",
            kind: FieldKind::Integer,
            default: "255",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "set_network",
        label: "设置BC网络参数 (0x48)",
        opcode: 0x48,
        response_opcode: 0x49,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "ipaddr",
                label: "IP地址",
                kind: FieldKind::Text,
                default: "192.168.0.100",
            },
            FieldSpec {
                key: "netmask",
                label: "子网掩码",
                kind: FieldKind::Text,
                default: "255.255.255.0",
            },
            FieldSpec {
                key: "gateway",
                label: "网关",
                kind: FieldKind::Text,
                default: "192.168.0.1",
            },
            FieldSpec {
                key: "dhcp",
                label: "DHCP",
                kind: FieldKind::Choice(DHCP_CHOICES),
                default: "0",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "set_mqtt_service",
        label: "设置BC MQTT服务 (0x4A)",
        opcode: 0x4A,
        response_opcode: 0x4B,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "mqtt_addr",
                label: "MQTT地址",
                kind: FieldKind::Text,
                default: "192.168.0.200",
            },
            FieldSpec {
                key: "mqtt_port",
                label: "MQTT端口",
                kind: FieldKind::Integer,
                default: "1883",
            },
            FieldSpec {
                key: "mqtt_username",
                label: "MQTT用户名",
                kind: FieldKind::Text,
                default: "",
            },
            FieldSpec {
                key: "mqtt_password",
                label: "MQTT密码",
                kind: FieldKind::Text,
                default: "",
            },
            FieldSpec {
                key: "mqtt_client_id",
                label: "MQTT客户端ID",
                kind: FieldKind::Text,
                default: "device-client",
            },
            FieldSpec {
                key: "mqtt_subscribe_topic",
                label: "MQTT订阅主题",
                kind: FieldKind::Text,
                default: "/application/AP-C-BM/device/DEVICE/down",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "set_dns",
        label: "设置DNS (0x52)",
        opcode: 0x52,
        response_opcode: 0x53,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "dns1",
                label: "首选DNS",
                kind: FieldKind::Text,
                default: "114.114.114.114",
            },
            FieldSpec {
                key: "dns2",
                label: "备用DNS",
                kind: FieldKind::Text,
                default: "8.8.8.8",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "speaker_volume",
        label: "喇叭开关/音量 (0x62)",
        opcode: 0x62,
        response_opcode: 0x63,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[FieldSpec {
            key: "value",
            label: "音量(0=关闭)",
            kind: FieldKind::Integer,
            default: "5",
        }],
        build_mode: BuildMode::Generic,
    },
    CommandSpec {
        key: "play_voice_file",
        label: "播放声音文件 (0x5A)",
        opcode: 0x5A,
        response_opcode: 0x5B,
        include_dest_addr: false,
        include_mesh_dev_type: false,
        fields: &[
            FieldSpec {
                key: "voice_name",
                label: "声音文件名",
                kind: FieldKind::Text,
                default: "voice.adpcm",
            },
            FieldSpec {
                key: "value",
                label: "播放控制",
                kind: FieldKind::Choice(START_STOP_CHOICES),
                default: "1",
            },
            FieldSpec {
                key: "time_stamp",
                label: "时间戳",
                kind: FieldKind::Integer,
                default: "",
            },
        ],
        build_mode: BuildMode::Generic,
    },
];

pub fn command_by_key(key: &str) -> Option<&'static CommandSpec> {
    COMMANDS.iter().find(|spec| spec.key == key)
}

pub fn command_by_opcode(opcode: u32) -> Option<&'static CommandSpec> {
    COMMANDS.iter().find(|spec| spec.opcode == opcode)
}

pub fn expected_response_opcode(opcode: u32) -> Option<u32> {
    command_by_opcode(opcode).map(|spec| spec.response_opcode)
}

pub fn response_can_omit_timestamp(opcode: u32) -> bool {
    matches!(opcode, 0x41 | 0x44)
}

pub fn current_time_stamp() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn current_time_millis() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

pub fn bytes_to_hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02X}")).collect()
}

fn padded_ota_chunk_bytes(chunk: &[u8]) -> Vec<u8> {
    let mut padded = chunk.to_vec();
    padded.resize(OTA_TRANSFER_CHUNK_SIZE, 0);
    padded
}

pub fn hex_to_bytes(value: &str) -> Result<Vec<u8>, String> {
    let cleaned: String = value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_uppercase();
    if !cleaned.len().is_multiple_of(2) {
        return Err("十六进制字符串长度必须为偶数".into());
    }
    (0..cleaned.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&cleaned[index..index + 2], 16).map_err(|err| err.to_string())
        })
        .collect()
}

pub fn crc16_xmodem(data: &[u8]) -> u16 {
    crc16_xmodem_with_initial(data, 0x0000)
}

fn crc16_xmodem_with_initial(data: &[u8], initial: u16) -> u16 {
    let mut crc = initial;
    for byte in data {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn bytes_ota_app_checksum(data: &[u8]) -> u16 {
    crc16_xmodem(data)
}

pub fn crc16_modbus(data: &[u8]) -> u16 {
    let mut crc = 0xFFFFu16;
    for byte in data {
        crc ^= u16::from(*byte);
        for _ in 0..8 {
            if crc & 0x0001 != 0 {
                crc = (crc >> 1) ^ 0xA001;
            } else {
                crc >>= 1;
            }
        }
    }
    crc
}

pub fn parse_opcode(text: &str) -> Result<u32, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("opcode 不能为空".into());
    }
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u32::from_str_radix(hex, 16).map_err(|err| err.to_string())
    } else {
        trimmed.parse::<u32>().map_err(|err| err.to_string())
    }
}

pub fn redact_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sanitized = serde_json::Map::new();
            for (key, item) in map {
                let value = if matches!(key.as_str(), "password" | "mqtt_password") {
                    Value::String("***".into())
                } else {
                    redact_json(item)
                };
                sanitized.insert(key.clone(), value);
            }
            Value::Object(sanitized)
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_json).collect()),
        _ => value.clone(),
    }
}

pub fn build_command_payload(
    spec: &CommandSpec,
    device: &DeviceProfile,
    form: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let payload = match spec.build_mode {
        BuildMode::Generic => build_generic_payload(spec, device, form),
        BuildMode::ScenePacked => build_scene_payload(spec, device, form),
    }?;
    validate_payload_semantics(spec.opcode, &payload)?;
    Ok(payload)
}

fn build_generic_payload(
    spec: &CommandSpec,
    device: &DeviceProfile,
    form: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let mut payload = serde_json::Map::new();
    payload.insert("opcode".into(), json!(spec.opcode));
    if spec.include_mesh_dev_type {
        payload.insert("mesh_dev_type".into(), json!(device.mesh_dev_type));
    }
    if spec.include_dest_addr {
        let dest_addr = form
            .get("dest_addr")
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(device.default_dest_addr);
        payload.insert("dest_addr".into(), json!(dest_addr));
    }
    for field in spec.fields {
        let raw = form
            .get(field.key)
            .map(|s| s.trim())
            .unwrap_or(field.default);
        if raw.is_empty() && field.key == "time_stamp" {
            payload.insert("time_stamp".into(), json!(current_time_stamp()));
            continue;
        }
        if raw.is_empty() {
            continue;
        }
        match field.kind {
            FieldKind::Integer | FieldKind::Choice(_) => {
                let value = if raw.starts_with("0x") || raw.starts_with("0X") {
                    u64::from_str_radix(raw.trim_start_matches("0x").trim_start_matches("0X"), 16)
                        .map_err(|err| err.to_string())?
                } else {
                    raw.parse::<u64>().map_err(|err| err.to_string())?
                };
                payload.insert(field.key.into(), json!(value));
            }
            FieldKind::Text => {
                payload.insert(field.key.into(), json!(raw));
            }
        }
    }
    if !payload.contains_key("time_stamp") {
        payload.insert("time_stamp".into(), json!(current_time_stamp()));
    }
    Ok(Value::Object(payload))
}

fn build_scene_payload(
    spec: &CommandSpec,
    device: &DeviceProfile,
    form: &BTreeMap<String, String>,
) -> Result<Value, String> {
    let mut payload = build_generic_payload(spec, device, form)?;
    let scene_bytes = [
        parse_u8(form, "scene_id", 1)?,
        parse_u8(form, "occupied_brightness", 100)?,
        parse_u8(form, "idle_brightness", 0)?,
        parse_u8(form, "auto_off_seconds", 5)?,
        parse_u8(form, "running_mode", 3)?,
        parse_u8(form, "switch_state", 1)?,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
    if let Value::Object(ref mut object) = payload {
        object.insert("value".into(), json!(bytes_to_hex(&scene_bytes)));
        object.remove("scene_id");
        object.remove("occupied_brightness");
        object.remove("idle_brightness");
        object.remove("auto_off_seconds");
        object.remove("running_mode");
        object.remove("switch_state");
    }
    Ok(payload)
}

fn parse_u8(form: &BTreeMap<String, String>, key: &str, default: u8) -> Result<u8, String> {
    match form
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        Some(value) => value.parse::<u8>().map_err(|err| err.to_string()),
        None => Ok(default),
    }
}

fn payload_u64(payload: &Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(Value::as_u64)
}

fn validate_payload_semantics(opcode: u32, payload: &Value) -> Result<(), String> {
    let value = payload_u64(payload, "value");
    match opcode {
        0x02 if value != Some(1) => {
            return Err("复位命令 value 必须为 1".into());
        }
        0x0F if !matches!(value, Some(0..=120)) => {
            return Err("能耗上报周期必须在 0..120 小时之间".into());
        }
        0x11 if value.is_none() => {
            return Err("A灯 OTA 通知必须包含目标版本".into());
        }
        0x15 if !matches!(value, Some(10..=255)) => {
            return Err("无人关灯延时必须在 10..255 秒之间".into());
        }
        0x17 if !matches!(value, Some(2 | 3)) => {
            return Err("运行模式只允许 2(工程模式) 或 3(感应模式)".into());
        }
        0x19 | 0x27 | 0x28 | 0x30 if !matches!(value, Some(0 | 1)) => {
            return Err("该开关类命令只允许 0 或 1".into());
        }
        0x1A if !matches!(value, Some(0) | Some(30..=180)) => {
            return Err("心跳周期只允许 0 或 30..180 秒".into());
        }
        0x20 if !matches!(value, Some(0..=20) | Some(254 | 255)) => {
            return Err("场景切换值只允许 0..20、254 或 255".into());
        }
        0x24 if !matches!(value, Some(1..=20)) => {
            return Err("删除场景编号只允许 1..20".into());
        }
        0x25 if !matches!(value, Some(0..=20) | Some(255)) => {
            return Err("查询场景值只允许 0..20 或 255".into());
        }
        0x31 if !matches!(value, Some(1..=0xFFFF)) => {
            return Err("联动组地址必须在 0x0001..0xFFFF".into());
        }
        0x34 if !matches!(value, Some(0..=10)) => {
            return Err("智能联动模式参数只允许 0..10".into());
        }
        0x35 if !matches!(value, Some(0) | Some(1..=5) | Some(255)) => {
            return Err("人体微波设置只允许 0、1..5 或 255".into());
        }
        0x4E if !matches!(value, Some(0..=255)) => {
            return Err("查询BC Mesh组网信息的保留值必须在 0..255".into());
        }
        0x50 if !matches!(value, Some(0 | 1)) => {
            return Err("查询A灯列表只允许 0(全部) 或 1(遥控选中)".into());
        }
        0x62 if !matches!(value, Some(0..=10)) => {
            return Err("喇叭音量只允许 0..10".into());
        }
        _ => {}
    }
    Ok(())
}

pub fn summarize_payload(payload: &Value) -> String {
    if let Some(opcode) = payload.get("opcode").and_then(Value::as_u64) {
        if opcode == 0x1B
            && let Some(value) = payload.get("value").and_then(Value::as_str)
            && let Ok(bytes) = hex_to_bytes(value)
            && bytes.len() >= 4
        {
            return format!(
                "心跳 开关={} 模式={} 版本=0x{:02X} 类型={}",
                bytes[0], bytes[1], bytes[2], bytes[3]
            );
        }
        if opcode == 0x47 {
            let version = payload
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("--");
            let model = payload
                .get("dev_model")
                .and_then(Value::as_str)
                .unwrap_or("--");
            let dev_id = payload
                .get("dev_id")
                .and_then(Value::as_str)
                .unwrap_or("--");
            return format!("BC信息 版本={} 型号={} 设备ID={}", version, model, dev_id);
        }
        if opcode == 0x4F {
            let value = payload.get("value").and_then(Value::as_str).unwrap_or("--");
            return format!("Mesh地址={value}");
        }
        if opcode == 0x51 {
            let total = payload
                .get("array_total_size")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let from = payload
                .get("index_from")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let to = payload.get("index_to").and_then(Value::as_u64).unwrap_or(0);
            return format!("A灯列表 分页 {}..{} / 总数 {}", from, to, total);
        }
        if opcode == 0x1D
            && let Some(value) = payload.get("value").and_then(Value::as_str)
            && let Ok(bytes) = hex_to_bytes(value)
            && bytes.len() >= 2
        {
            return format!("状态查询 查询码={} 返回=0x{:02X}", bytes[0], bytes[1]);
        }
        if opcode == 0x1F
            && let Some(value) = payload.get("value").and_then(Value::as_str)
        {
            return format!("组网信息 {}", value.chars().take(24).collect::<String>());
        }
        if (opcode == 0x22 || opcode == 0x26)
            && let Some(value) = payload.get("value").and_then(Value::as_str)
        {
            return format!("场景数据 {}", value.chars().take(24).collect::<String>());
        }
        if opcode == 0x29 {
            let power = payload.get("power").and_then(Value::as_u64).unwrap_or(0);
            let energy = payload
                .get("energy_consumption")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            return format!("能耗 功率={}W 电量={}kWh", power, energy);
        }
        if opcode == 0x32 {
            let value = payload.get("value").and_then(Value::as_str).unwrap_or("--");
            return format!("联动组状态={value}");
        }
        if opcode == 0x10 {
            let value = payload.get("value").and_then(Value::as_str).unwrap_or("--");
            return format!("人体微波事件={value}");
        }
        return format!("操作码=0x{opcode:02X}");
    }
    serde_json::to_string(payload).unwrap_or_else(|_| "{}".into())
}

pub fn decode_payload_details(payload: &Value) -> Vec<(String, String)> {
    let mut details = Vec::new();
    let Some(opcode) = payload
        .get("opcode")
        .and_then(Value::as_u64)
        .map(|value| value as u32)
    else {
        return details;
    };
    details.push(("操作码".into(), format!("0x{opcode:02X}")));

    match opcode {
        0x10 => {
            let value = payload.get("value").and_then(Value::as_str).unwrap_or("--");
            let event = match value {
                "FF" => "检测到有人",
                "00" => "检测到离开",
                _ => "未知事件",
            };
            details.push(("人体微波事件".into(), event.into()));
        }
        0x1B => {
            if let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = hex_to_bytes(value)
                && bytes.len() >= 4
            {
                details.push(("开关状态".into(), bytes[0].to_string()));
                details.push(("运行模式".into(), bytes[1].to_string()));
                details.push(("固件版本".into(), format!("0x{:02X}", bytes[2])));
                details.push(("设备类型".into(), bytes[3].to_string()));
            }
        }
        0x1D => {
            if let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = hex_to_bytes(value)
                && bytes.len() >= 2
            {
                let query_code = bytes[0];
                details.push(("查询码".into(), query_code.to_string()));
                match query_code {
                    0 | 1 | 3 | 4 | 5 | 6 | 7 | 9 | 11 | 12 => {
                        details.push(("返回值".into(), bytes[1].to_string()));
                    }
                    8 | 255 if bytes.len() >= 7 => {
                        details.push(("MAC地址".into(), format_mac(&bytes[1..7])));
                    }
                    _ => {}
                }
            }
        }
        0x1F => {
            if let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = hex_to_bytes(value)
                && !bytes.is_empty()
            {
                let query_code = bytes[0];
                details.push(("查询码".into(), query_code.to_string()));
                let addresses = decode_be_u16_addresses(&bytes[1..]);
                if !addresses.is_empty() {
                    details.push(("地址列表".into(), addresses.join(", ")));
                }
            }
        }
        0x22 | 0x26 => {
            if let Some(value) = payload.get("value").and_then(Value::as_str)
                && let Ok(bytes) = hex_to_bytes(value)
                && bytes.len() >= 6
            {
                details.push(("场景编号".into(), bytes[0].to_string()));
                details.push(("有人亮度".into(), bytes[1].to_string()));
                details.push(("无人亮度".into(), bytes[2].to_string()));
                details.push(("无人关灯延时".into(), bytes[3].to_string()));
                details.push(("运行模式".into(), bytes[4].to_string()));
                details.push(("开关状态".into(), bytes[5].to_string()));
            }
        }
        0x29 => {
            for (key, label) in [
                ("running_duration", "通电时长(秒)"),
                ("light_on_duration", "亮灯时长(秒)"),
                ("power", "最大功率(W)"),
                ("energy_consumption", "能耗(kWh)"),
            ] {
                if let Some(value) = payload.get(key) {
                    details.push((label.into(), json_value_to_string(value)));
                }
            }
        }
        0x47 => {
            for (key, label) in [
                ("version", "设备版本"),
                ("dev_model", "产品型号"),
                ("dev_id", "设备ID"),
            ] {
                if let Some(value) = payload.get(key) {
                    details.push((label.into(), json_value_to_string(value)));
                }
            }
        }
        0x4F => {
            if let Some(value) = payload.get("value") {
                details.push(("Mesh地址".into(), json_value_to_string(value)));
            }
        }
        0x51 => {
            for (key, label) in [
                ("value", "列表类型"),
                ("array_total_size", "总数量"),
                ("index_from", "起始索引"),
                ("index_to", "结束索引"),
            ] {
                if let Some(value) = payload.get(key) {
                    details.push((label.into(), json_value_to_string(value)));
                }
            }
            if let Some(values) = payload.get("value_array").and_then(Value::as_array) {
                let mut rendered = Vec::new();
                for item in values.iter().take(5) {
                    if let Some(text) = item.as_str() {
                        rendered.push(decode_a_light_entry(text));
                    }
                }
                if !rendered.is_empty() {
                    details.push(("前5项".into(), rendered.join(" | ")));
                }
            }
        }
        _ => {}
    }

    details
}

pub fn classify_execution_result(payload: &Value) -> Option<(&'static str, String)> {
    let value = payload.get("value").and_then(Value::as_str)?;
    let normalized = value.trim().to_lowercase();

    if normalized.contains("execute error") || normalized == "error" || normalized.contains("失败")
    {
        return Some(("错误", value.to_string()));
    }
    if normalized.contains("forward ok and execute ok") {
        return Some(("成功", "转发成功，执行成功".into()));
    }
    if normalized.contains("forward ok and execute error") {
        return Some(("错误", "转发成功，执行失败".into()));
    }
    if normalized.contains("execute ok") {
        return Some(("成功", "执行成功".into()));
    }
    if normalized.contains("forward ok") {
        return Some(("成功", "转发成功".into()));
    }
    None
}

fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn format_mac(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn decode_be_u16_addresses(bytes: &[u8]) -> Vec<String> {
    let mut addresses = Vec::new();
    for chunk in bytes.chunks(2) {
        if chunk.len() < 2 {
            continue;
        }
        let address = u16::from_be_bytes([chunk[0], chunk[1]]);
        if address == 0 || address == 0xFFFF {
            continue;
        }
        addresses.push(format!("0x{address:04X}"));
    }
    addresses
}

fn decode_a_light_entry(text: &str) -> String {
    match hex_to_bytes(text) {
        Ok(bytes) if bytes.len() >= 8 => {
            let address = u16::from_be_bytes([bytes[0], bytes[1]]);
            let mac = format_mac(&bytes[2..8]);
            format!("0x{address:04X}/{mac}")
        }
        _ => text.chars().take(24).collect(),
    }
}

pub fn decode_a_light_entry_public(text: &str) -> String {
    decode_a_light_entry(text)
}

pub fn chunk_count(data: &[u8], chunk_size: usize) -> usize {
    if data.is_empty() {
        0
    } else {
        data.len().div_ceil(chunk_size)
    }
}

const OTA_TRANSFER_CHUNK_SIZE: usize = 200;
const MEDIA_TRANSFER_CHUNK_SIZE: usize = 400;
pub const DEFAULT_BYTES_OTA_CHUNK_SIZE: usize = 1024;
pub const BYTES_OTA_QUICK_CHUNK_SIZES: [usize; 3] = [1024, 512, 256];
const BYTES_OTA_MAX_INDEXED_CHUNKS: usize = (u16::MAX as usize) + 1;
const BYTES_OTA_MAX_CHUNK_SIZE: usize = 1024;
const BYTES_FRAME_HEADER_SIZE: usize = 26;
const BYTES_FRAME_PREFIX: [u8; 2] = [0xFE, 0xFE];
const BYTES_FRAME_DEVICE_TYPE: u8 = 0x01;
const BYTES_FRAME_MODULE_TYPE: u8 = 0x00;
const BYTES_FRAME_MODULE_INDEX: u8 = 0x00;
const BYTES_FRAME_COMMAND_TYPE: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedBytesTransferFrame {
    pub command: u8,
    pub message_id: u64,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferFormatOptions {
    pub bytes_chunk_size: usize,
    pub bytes_app_crc_override: Option<u16>,
    pub bytes_a_app_version: Option<u16>,
}

impl Default for TransferFormatOptions {
    fn default() -> Self {
        Self {
            bytes_chunk_size: DEFAULT_BYTES_OTA_CHUNK_SIZE,
            bytes_app_crc_override: None,
            bytes_a_app_version: None,
        }
    }
}

pub fn build_transfer_packets_for_format(
    kind: TransferKind,
    format: OtaTransferFormat,
    data: &[u8],
    version: u8,
    voice_name: &str,
    options: TransferFormatOptions,
) -> Result<Vec<TransferPacket>, String> {
    match format {
        OtaTransferFormat::Json => build_transfer_packets(kind, data, version, voice_name)
            .map(|packets| packets.into_iter().map(TransferPacket::json).collect()),
        OtaTransferFormat::Bytes => build_bytes_ota_transfer_packets(
            kind,
            data,
            version,
            options.bytes_chunk_size,
            options.bytes_app_crc_override,
            options.bytes_a_app_version,
        ),
    }
}

pub fn transfer_preview_for_format(
    kind: TransferKind,
    format: OtaTransferFormat,
    data: &[u8],
    version: u8,
    voice_name: &str,
    options: TransferFormatOptions,
) -> Value {
    match format {
        OtaTransferFormat::Json => transfer_preview(kind, data, version, voice_name),
        OtaTransferFormat::Bytes => bytes_ota_transfer_preview(
            kind,
            data,
            version,
            options.bytes_chunk_size,
            options.bytes_app_crc_override,
            options.bytes_a_app_version,
        ),
    }
}

pub fn build_bytes_ota_transfer_packets(
    kind: TransferKind,
    data: &[u8],
    protocol_version: u8,
    chunk_size: usize,
    app_crc_override: Option<u16>,
    a_app_version: Option<u16>,
) -> Result<Vec<TransferPacket>, String> {
    if data.is_empty() {
        return Err("传输数据不能为空".into());
    }
    validate_bytes_ota_chunk_size(chunk_size)?;
    let Some((start_opcode, data_opcode, end_opcode)) = bytes_ota_opcodes(kind) else {
        return Err("Bytes OTA 仅支持 A灯/BC灯 APP升级".into());
    };
    let chunk_total = chunk_count(data, chunk_size);
    if chunk_total > BYTES_OTA_MAX_INDEXED_CHUNKS {
        return Err(format!(
            "Bytes OTA 数据块数 {} 超出分包索引上限 {}，请增大包长或减小固件大小。",
            chunk_total, BYTES_OTA_MAX_INDEXED_CHUNKS
        ));
    }

    let base_message_id = current_time_millis();
    let padded_data = padded_bytes_ota_data(data, chunk_size);
    let mut packets = Vec::with_capacity(chunk_total + 2);
    packets.push(TransferPacket::bytes_frame(BytesTransferFrame {
        command: start_opcode,
        message_id: base_message_id,
        protocol_version,
        body: vec![1],
    }));

    for chunk_id in 0..chunk_total {
        let chunk = &data[chunk_id * chunk_size..data.len().min((chunk_id + 1) * chunk_size)];
        let mut body = Vec::with_capacity(2 + chunk_size);
        body.extend_from_slice(&(chunk_id as u16).to_be_bytes());
        body.extend_from_slice(&padded_bytes_ota_chunk(chunk, chunk_size));
        packets.push(TransferPacket::bytes_frame(BytesTransferFrame {
            command: data_opcode,
            message_id: base_message_id.wrapping_add(chunk_id as u64 + 1),
            protocol_version,
            body,
        }));
    }

    let mut end_body = Vec::with_capacity(if kind == TransferKind::AOta { 8 } else { 6 });
    end_body.extend_from_slice(&(padded_data.len() as u32).to_be_bytes());
    end_body.extend_from_slice(
        &app_crc_override
            .unwrap_or_else(|| bytes_ota_app_checksum(&padded_data))
            .to_be_bytes(),
    );
    if kind == TransferKind::AOta {
        end_body.extend_from_slice(&a_app_version.unwrap_or_default().to_be_bytes());
    }
    packets.push(TransferPacket::bytes_frame(BytesTransferFrame {
        command: end_opcode,
        message_id: base_message_id.wrapping_add(chunk_total as u64 + 1),
        protocol_version,
        body: end_body,
    }));
    Ok(packets)
}

pub fn bytes_ota_transfer_preview(
    kind: TransferKind,
    data: &[u8],
    protocol_version: u8,
    chunk_size: usize,
    app_crc_override: Option<u16>,
    a_app_version: Option<u16>,
) -> Value {
    let chunk_size = if validate_bytes_ota_chunk_size(chunk_size).is_ok() {
        chunk_size
    } else {
        DEFAULT_BYTES_OTA_CHUNK_SIZE
    };
    let chunk_total = data.len().div_ceil(chunk_size);
    let padded_total_size = chunk_total * chunk_size;
    let computed_check_sum = bytes_ota_app_checksum(&padded_bytes_ota_data(data, chunk_size));
    let check_sum = app_crc_override.unwrap_or(computed_check_sum);
    let (start_opcode, data_opcode, end_opcode) = bytes_ota_opcodes(kind).unwrap_or((0, 0, 0));
    json!({
        "format": "bytes",
        "opcode": start_opcode,
        "data_opcode": data_opcode,
        "end_opcode": end_opcode,
        "protocol_version": protocol_version,
        "ota_pack_count": chunk_total,
        "ota_total_size": data.len(),
        "ota_padded_total_size": padded_total_size,
        "chunk_size": chunk_size,
        "check_sum": check_sum,
        "computed_check_sum": computed_check_sum,
        "check_sum_override": app_crc_override,
        "a_app_version": if kind == TransferKind::AOta { a_app_version } else { None },
    })
}

pub fn parse_bytes_ota_app_crc_override(value: &str) -> Result<Option<u16>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() != 4 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("Bytes OTA APP CRC覆盖值必须是2字节CRC十六进制值，例如2DF3。".into());
    }
    u16::from_str_radix(value, 16)
        .map(Some)
        .map_err(|err| err.to_string())
}

pub fn parse_bytes_a_ota_app_version(value: &str) -> Result<Option<u16>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if !matches!(value.len(), 2 | 4) || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("A灯 Bytes OTA 版本必须是十六进制值，例如02或0102。".into());
    }
    u16::from_str_radix(value, 16)
        .map(Some)
        .map_err(|err| err.to_string())
}

pub fn render_transfer_packet_payload(
    packet: &TransferPacket,
    device_id: &str,
) -> Result<Vec<u8>, String> {
    match &packet.payload {
        TransferPayload::Json(payload) => Ok(payload.to_string().into_bytes()),
        TransferPayload::BytesFrame(frame) => render_bytes_transfer_frame(frame, device_id),
    }
}

pub fn render_bytes_transfer_frame(
    frame: &BytesTransferFrame,
    device_id: &str,
) -> Result<Vec<u8>, String> {
    let frame_len = BYTES_FRAME_HEADER_SIZE + frame.body.len();
    if frame_len > u16::MAX as usize {
        return Err("Bytes MQTT帧过长，无法编码长度字段".into());
    }

    let mut payload = Vec::with_capacity(frame_len + 2);
    payload.extend_from_slice(&BYTES_FRAME_PREFIX);
    payload.extend_from_slice(&(frame_len as u16).to_be_bytes());
    payload.push(frame.protocol_version);
    payload.push(BYTES_FRAME_DEVICE_TYPE);
    payload.extend_from_slice(&device_id_frame_bytes(device_id)?);
    payload.push(BYTES_FRAME_MODULE_TYPE);
    payload.push(BYTES_FRAME_MODULE_INDEX);
    payload.push(BYTES_FRAME_COMMAND_TYPE);
    payload.push(frame.command);
    payload.extend_from_slice(&frame.message_id.to_be_bytes());
    payload.extend_from_slice(&frame.body);
    let checksum = crc16_modbus(&payload);
    payload.extend_from_slice(&checksum.to_be_bytes());
    Ok(payload)
}

pub fn decode_bytes_transfer_frame(payload: &[u8]) -> Option<DecodedBytesTransferFrame> {
    if payload.len() < BYTES_FRAME_HEADER_SIZE + 2 {
        return None;
    }
    if payload.get(0..2)? != BYTES_FRAME_PREFIX {
        return None;
    }
    let frame_len = u16::from_be_bytes([payload[2], payload[3]]) as usize;
    if frame_len < BYTES_FRAME_HEADER_SIZE || payload.len() != frame_len + 2 {
        return None;
    }
    let expected_crc = u16::from_be_bytes([payload[frame_len], payload[frame_len + 1]]);
    if crc16_modbus(&payload[..frame_len]) != expected_crc {
        return None;
    }
    let message_id = u64::from_be_bytes(payload[18..26].try_into().ok()?);
    Some(DecodedBytesTransferFrame {
        command: payload[17],
        message_id,
        body: payload[BYTES_FRAME_HEADER_SIZE..frame_len].to_vec(),
    })
}

pub fn decode_bytes_transfer_ack_payload(payload: &[u8]) -> Option<Value> {
    let frame = decode_bytes_transfer_frame(payload)?;
    let return_code = frame.body.first().copied().unwrap_or(0xFF);
    let value = if return_code == 0 {
        "Execute OK"
    } else {
        "Execute Error"
    };
    let mut payload = json!({
        "opcode": frame.command,
        "value": value,
        "return_code": return_code,
        "time_stamp": frame.message_id,
    });
    if matches!(frame.command, 0xC3 | 0xC9) && frame.body.len() >= 3 {
        payload["ota_data_index"] = json!(u16::from_be_bytes([frame.body[1], frame.body[2]]));
    }
    Some(payload)
}

fn bytes_ota_opcodes(kind: TransferKind) -> Option<(u8, u8, u8)> {
    match kind {
        TransferKind::BcOta => Some((0xC0, 0xC2, 0xC4)),
        TransferKind::AOta => Some((0xC6, 0xC8, 0xCA)),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => None,
    }
}

pub fn build_bytes_control_packet(
    command: u8,
    body: Vec<u8>,
    protocol_version: u8,
) -> TransferPacket {
    TransferPacket::bytes_frame(BytesTransferFrame {
        command,
        message_id: current_time_millis(),
        protocol_version,
        body,
    })
}

pub fn validate_bytes_ota_chunk_size(chunk_size: usize) -> Result<(), String> {
    if chunk_size == 0 {
        return Err("Bytes OTA 分包长度必须大于 0。".into());
    }
    if chunk_size > BYTES_OTA_MAX_CHUNK_SIZE {
        return Err(format!(
            "Bytes OTA 分包长度不能超过 {} 字节。",
            BYTES_OTA_MAX_CHUNK_SIZE
        ));
    }
    Ok(())
}

fn padded_bytes_ota_chunk(chunk: &[u8], chunk_size: usize) -> Vec<u8> {
    let mut padded = chunk.to_vec();
    padded.resize(chunk_size, 0xFF);
    padded
}

fn padded_bytes_ota_data(data: &[u8], chunk_size: usize) -> Vec<u8> {
    let mut padded = data.to_vec();
    let padded_len = chunk_count(data, chunk_size) * chunk_size;
    padded.resize(padded_len, 0xFF);
    padded
}

fn device_id_frame_bytes(device_id: &str) -> Result<[u8; 8], String> {
    let trimmed = device_id.trim();
    if trimmed.is_empty() {
        return Err("Bytes OTA 设备ID不能为空，需填写十六进制设备ID。".into());
    }
    if !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("Bytes OTA 设备ID必须只包含十六进制字符。".into());
    }
    if !trimmed.len().is_multiple_of(2) {
        return Err("Bytes OTA 设备ID十六进制长度必须为偶数。".into());
    }
    let bytes = hex_to_bytes(trimmed)?;
    if bytes.len() > 8 {
        return Err("Bytes OTA 设备ID最多只能编码为8字节。".into());
    }
    Ok(right_aligned_8(&bytes))
}

fn right_aligned_8(bytes: &[u8]) -> [u8; 8] {
    let mut output = [0; 8];
    let copy_len = bytes.len().min(8);
    output[8 - copy_len..].copy_from_slice(&bytes[bytes.len() - copy_len..]);
    output
}

pub fn build_transfer_packets(
    kind: TransferKind,
    data: &[u8],
    version: u8,
    voice_name: &str,
) -> Result<Vec<Value>, String> {
    if data.is_empty() {
        return Err("传输数据不能为空".into());
    }
    let total_packets = match kind {
        TransferKind::BcOta | TransferKind::AOta => 1 + chunk_count(data, OTA_TRANSFER_CHUNK_SIZE),
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => {
            2 + chunk_count(data, MEDIA_TRANSFER_CHUNK_SIZE)
        }
    };
    let mut packets = Vec::with_capacity(total_packets);
    for packet_index in 0..total_packets {
        packets.push(build_transfer_packet(
            kind,
            data,
            packet_index,
            version,
            voice_name,
        )?);
    }
    Ok(packets)
}

pub fn build_transfer_packet(
    kind: TransferKind,
    data: &[u8],
    packet_index: usize,
    version: u8,
    voice_name: &str,
) -> Result<Value, String> {
    if data.is_empty() {
        return Err("传输数据不能为空".into());
    }

    match kind {
        TransferKind::BcOta => {
            let chunk_total = chunk_count(data, OTA_TRANSFER_CHUNK_SIZE);
            if packet_index == 0 {
                return Ok(json!({
                    "opcode": 0x40,
                    "version": version,
                    "ota_pack_count": chunk_total,
                    "ota_total_size": data.len(),
                }));
            }
            let chunk_id = packet_index - 1;
            let chunk = &data[chunk_id * OTA_TRANSFER_CHUNK_SIZE
                ..data.len().min((chunk_id + 1) * OTA_TRANSFER_CHUNK_SIZE)];
            let padded_chunk = padded_ota_chunk_bytes(chunk);
            Ok(json!({
                "opcode": 0x42,
                "version": version,
                "ota_data_index": chunk_id,
                "ota_byte_size": chunk.len(),
                "value": bytes_to_hex(&padded_chunk),
            }))
        }
        TransferKind::AOta => {
            let chunk_total = chunk_count(data, OTA_TRANSFER_CHUNK_SIZE);
            if packet_index == 0 {
                return Ok(json!({
                    "opcode": 0x43,
                    "version": version,
                    "ota_pack_count": chunk_total,
                    "ota_total_size": data.len(),
                }));
            }
            let chunk_id = packet_index - 1;
            let chunk = &data[chunk_id * OTA_TRANSFER_CHUNK_SIZE
                ..data.len().min((chunk_id + 1) * OTA_TRANSFER_CHUNK_SIZE)];
            let padded_chunk = padded_ota_chunk_bytes(chunk);
            Ok(json!({
                "opcode": 0x45,
                "version": version,
                "ota_data_index": chunk_id,
                "ota_byte_size": chunk.len(),
                "value": bytes_to_hex(&padded_chunk),
            }))
        }
        TransferKind::VoiceFile | TransferKind::RealtimeVoice => {
            let chunk_total = chunk_count(data, MEDIA_TRANSFER_CHUNK_SIZE);
            let (start_opcode, chunk_opcode, end_opcode) = match kind {
                TransferKind::VoiceFile => (0x54, 0x56, 0x58),
                TransferKind::RealtimeVoice => (0x5C, 0x5E, 0x60),
                _ => unreachable!(),
            };

            if packet_index == 0 {
                return Ok(json!({
                    "opcode": start_opcode,
                    "voice_name": voice_name,
                    "totle_len": data.len(),
                    "value": 1,
                    "time_stamp": current_time_stamp(),
                }));
            }
            if packet_index == chunk_total + 1 {
                return Ok(json!({
                    "opcode": end_opcode,
                    "voice_name": voice_name,
                    "totle_len": data.len(),
                    "check_sum": crc16_xmodem(data),
                    "time_stamp": current_time_stamp(),
                }));
            }
            let chunk_id = packet_index - 1;
            let chunk = &data[chunk_id * MEDIA_TRANSFER_CHUNK_SIZE
                ..data.len().min((chunk_id + 1) * MEDIA_TRANSFER_CHUNK_SIZE)];
            Ok(json!({
                "opcode": chunk_opcode,
                "voice_name": voice_name,
                "packet_index": chunk_id,
                "packet_len": chunk.len(),
                "value": bytes_to_hex(chunk),
                "time_stamp": current_time_stamp(),
            }))
        }
    }
}

pub fn transfer_preview(kind: TransferKind, data: &[u8], version: u8, voice_name: &str) -> Value {
    match kind {
        TransferKind::BcOta => json!({
            "opcode": 0x40,
            "version": version,
            "ota_pack_count": data.len().div_ceil(OTA_TRANSFER_CHUNK_SIZE),
            "ota_total_size": data.len(),
        }),
        TransferKind::AOta => json!({
            "opcode": 0x43,
            "version": version,
            "ota_pack_count": data.len().div_ceil(OTA_TRANSFER_CHUNK_SIZE),
            "ota_total_size": data.len(),
        }),
        TransferKind::VoiceFile => json!({
            "opcode": 0x54,
            "voice_name": voice_name,
            "totle_len": data.len(),
            "value": 1,
        }),
        TransferKind::RealtimeVoice => json!({
            "opcode": 0x5C,
            "voice_name": voice_name,
            "totle_len": data.len(),
            "value": 1,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_device() -> DeviceProfile {
        DeviceProfile {
            local_id: 1,
            name: "Lane 1".into(),
            device_id: "11112345".into(),
            up_topic: "/application/AP-C-BM/device/11112345/up".into(),
            down_topic: "/application/AP-C-BM/device/11112345/down".into(),
            mesh_dev_type: 1,
            default_dest_addr: 0x1001,
            subscribe_enabled: true,
        }
    }

    #[test]
    fn crc_vector() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
        assert_eq!(crc16_xmodem_with_initial(b"123456789", 0xFFFF), 0x29B1);
        assert_eq!(crc16_modbus(b"123456789"), 0x4B37);
    }

    #[test]
    fn bytes_frame_outer_crc_uses_modbus_high_byte_first() {
        let frame = BytesTransferFrame {
            command: 0xC0,
            message_id: 0x0000_019E_15A6_3018,
            protocol_version: 1,
            body: vec![1],
        };
        let payload = render_bytes_transfer_frame(&frame, "34B7DA848802").unwrap();

        assert_eq!(
            bytes_to_hex(&payload),
            "FEFE001B0101000034B7DA848802000002C00000019E15A63018012989"
        );
        assert_eq!(crc16_modbus(&payload[..payload.len() - 2]), 0x2989);
        assert_eq!(&payload[payload.len() - 2..], &[0x29, 0x89]);
        assert!(decode_bytes_transfer_frame(&payload).is_some());
    }

    #[test]
    fn hex_roundtrip() {
        let original = b"\x05\x3F\x04\x11";
        let encoded = bytes_to_hex(original);
        assert_eq!(encoded, "053F0411");
        assert_eq!(hex_to_bytes(&encoded).unwrap(), original);
    }

    #[test]
    fn parse_opcode_variants() {
        assert_eq!(parse_opcode("70").unwrap(), 70);
        assert_eq!(parse_opcode("0x46").unwrap(), 0x46);
        assert!(parse_opcode("").is_err());
    }

    #[test]
    fn command_catalog_contains_core_protocol_commands() {
        let keys = COMMANDS.iter().map(|spec| spec.key).collect::<Vec<_>>();
        for required in [
            "reset_mesh_device",
            "energy_report_interval",
            "notify_a_ota",
            "auto_off_delay",
            "running_mode",
            "remote_network_enable",
            "query_mesh_management",
            "switch_scene",
            "delete_scene",
            "query_scene",
            "group_linkage",
            "energy_info",
            "linkage_group_switch",
            "query_linkage_group",
            "network_management",
            "smart_linkage_mode",
            "microwave_sensor",
            "query_mesh_info",
            "query_a_lights",
        ] {
            assert!(keys.contains(&required), "missing {required}");
        }
    }

    #[test]
    fn build_network_management_payload() {
        let spec = command_by_key("network_management").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("dest_addr".into(), "4097".into());
        form.insert("value".into(), "11".into());
        form.insert("group_addr".into(), "57344".into());
        let payload = build_command_payload(spec, &device, &form).unwrap();
        assert_eq!(payload["opcode"], 0x33);
        assert_eq!(payload["dest_addr"], 4097);
        assert_eq!(payload["mesh_dev_type"], 1);
        assert_eq!(payload["group_addr"], 57344);
    }

    #[test]
    fn generic_commands_auto_include_time_stamp() {
        let spec = command_by_key("switch_control").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("dest_addr".into(), "4097".into());
        form.insert("value".into(), "1".into());
        let payload = build_command_payload(spec, &device, &form).unwrap();
        assert!(payload.get("time_stamp").and_then(Value::as_u64).is_some());
    }

    #[test]
    fn auto_off_delay_rejects_out_of_range_value() {
        let spec = command_by_key("auto_off_delay").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("dest_addr".into(), "4097".into());
        form.insert("value".into(), "5".into());
        let error = build_command_payload(spec, &device, &form).unwrap_err();
        assert!(error.contains("10..255"));
    }

    #[test]
    fn microwave_sensor_accepts_special_value_255() {
        let spec = command_by_key("microwave_sensor").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("dest_addr".into(), "4097".into());
        form.insert("value".into(), "255".into());
        let payload = build_command_payload(spec, &device, &form).unwrap();
        assert_eq!(payload["value"], 255);
    }

    #[test]
    fn heartbeat_interval_rejects_invalid_short_period() {
        let spec = command_by_key("heartbeat_interval").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("dest_addr".into(), "4097".into());
        form.insert("value".into(), "10".into());
        let error = build_command_payload(spec, &device, &form).unwrap_err();
        assert!(error.contains("30..180"));
    }

    #[test]
    fn build_bc_ota_packets() {
        let packets =
            build_transfer_packets(TransferKind::BcOta, b"0123456789ABCDEF", 7, "").unwrap();
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0]["opcode"], 0x40);
        assert_eq!(packets[1]["opcode"], 0x42);
        assert_eq!(packets[1]["ota_data_index"], 0);
    }

    #[test]
    fn build_bc_ota_packets_split_at_200_bytes() {
        let data = vec![0x5A; 201];
        let packets = build_transfer_packets(TransferKind::BcOta, &data, 7, "").unwrap();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0]["ota_pack_count"], 2);
        assert_eq!(packets[1]["ota_byte_size"], 200);
        assert_eq!(packets[2]["ota_byte_size"], 1);
        let value = packets[2]["value"].as_str().unwrap();
        assert_eq!(value.len(), OTA_TRANSFER_CHUNK_SIZE * 2);
        assert!(value.starts_with("5A"));
        assert!(value[2..].chars().all(|ch| ch == '0'));
    }

    #[test]
    fn build_a_ota_packets_pad_last_chunk_but_keep_real_size() {
        let data = vec![0x6B; 201];
        let packets = build_transfer_packets(TransferKind::AOta, &data, 7, "").unwrap();
        assert_eq!(packets.len(), 3);
        assert_eq!(packets[0]["ota_pack_count"], 2);
        assert_eq!(packets[2]["opcode"], 0x45);
        assert_eq!(packets[2]["ota_byte_size"], 1);
        let value = packets[2]["value"].as_str().unwrap();
        assert_eq!(value.len(), OTA_TRANSFER_CHUNK_SIZE * 2);
        assert!(value.starts_with("6B"));
        assert!(value[2..].chars().all(|ch| ch == '0'));
    }

    #[test]
    fn build_bytes_bc_ota_frames_follow_document_layout() {
        let data = vec![0x5A; DEFAULT_BYTES_OTA_CHUNK_SIZE + 1];
        let packets = build_transfer_packets_for_format(
            TransferKind::BcOta,
            OtaTransferFormat::Bytes,
            &data,
            1,
            "",
            TransferFormatOptions::default(),
        )
        .unwrap();
        assert_eq!(packets.len(), 4);
        assert_eq!(packets[0].opcode, 0xC0);
        assert_eq!(packets[1].opcode, 0xC2);
        assert_eq!(packets[3].opcode, 0xC4);

        let start = render_transfer_packet_payload(&packets[0], "34B7DA848802").unwrap();
        assert_eq!(&start[0..2], &[0xFE, 0xFE]);
        assert_eq!(u16::from_be_bytes([start[2], start[3]]), 27);
        assert_eq!(start[4], 1);
        assert_eq!(&start[6..14], &[0, 0, 0x34, 0xB7, 0xDA, 0x84, 0x88, 0x02]);
        assert_eq!(start[16], 0x02);
        assert_eq!(start[17], 0xC0);
        assert_eq!(start[26], 1);
        let start_crc = u16::from_be_bytes([start[start.len() - 2], start[start.len() - 1]]);
        assert_eq!(start_crc, crc16_modbus(&start[..start.len() - 2]));

        let data_frame = render_transfer_packet_payload(&packets[2], "34B7DA848802").unwrap();
        assert_eq!(data_frame[17], 0xC2);
        assert_eq!(u16::from_be_bytes([data_frame[26], data_frame[27]]), 1);
        assert_eq!(data_frame[28], 0x5A);
        assert!(
            data_frame[29..26 + 2 + DEFAULT_BYTES_OTA_CHUNK_SIZE]
                .iter()
                .all(|byte| *byte == 0xFF)
        );

        let end = render_transfer_packet_payload(&packets[3], "34B7DA848802").unwrap();
        assert_eq!(end[17], 0xC4);
        assert_eq!(u16::from_be_bytes([end[2], end[3]]), 32);
        assert_eq!(
            u32::from_be_bytes([end[26], end[27], end[28], end[29]]),
            (DEFAULT_BYTES_OTA_CHUNK_SIZE * 2) as u32
        );
        let mut padded = data.clone();
        padded.resize(DEFAULT_BYTES_OTA_CHUNK_SIZE * 2, 0xFF);
        assert_eq!(
            u16::from_be_bytes([end[30], end[31]]),
            bytes_ota_app_checksum(&padded)
        );
    }

    #[test]
    fn build_bytes_a_ota_uses_a_light_opcodes() {
        let packets = build_transfer_packets_for_format(
            TransferKind::AOta,
            OtaTransferFormat::Bytes,
            b"abc",
            1,
            "",
            TransferFormatOptions {
                bytes_a_app_version: Some(0x0102),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            packets
                .iter()
                .map(|packet| packet.opcode)
                .collect::<Vec<_>>(),
            vec![0xC6, 0xC8, 0xCA]
        );
        let end = render_transfer_packet_payload(&packets[2], "34B7DA848802").unwrap();
        assert_eq!(end[17], 0xCA);
        assert_eq!(u16::from_be_bytes([end[2], end[3]]), 34);
        assert_eq!(u16::from_be_bytes([end[32], end[33]]), 0x0102);
    }

    #[test]
    fn build_bytes_ota_uses_manual_app_crc_override() {
        let packets = build_transfer_packets_for_format(
            TransferKind::BcOta,
            OtaTransferFormat::Bytes,
            b"abc",
            1,
            "",
            TransferFormatOptions {
                bytes_app_crc_override: Some(0x2DF3),
                ..Default::default()
            },
        )
        .unwrap();

        let end = render_transfer_packet_payload(&packets[2], "34B7DA848802").unwrap();
        assert_eq!(end[17], 0xC4);
        assert_eq!(u16::from_be_bytes([end[30], end[31]]), 0x2DF3);
    }

    #[test]
    fn parse_bytes_ota_app_crc_override_requires_four_hex_digits() {
        assert_eq!(parse_bytes_ota_app_crc_override(""), Ok(None));
        assert_eq!(parse_bytes_ota_app_crc_override("2DF3"), Ok(Some(0x2DF3)));
        assert!(parse_bytes_ota_app_crc_override("2DF").is_err());
        assert!(parse_bytes_ota_app_crc_override("2DFX").is_err());
    }

    #[test]
    fn parse_bytes_a_ota_app_version_accepts_short_or_full_hex() {
        assert_eq!(parse_bytes_a_ota_app_version(""), Ok(None));
        assert_eq!(parse_bytes_a_ota_app_version("02"), Ok(Some(0x0002)));
        assert_eq!(parse_bytes_a_ota_app_version("0102"), Ok(Some(0x0102)));
        assert!(parse_bytes_a_ota_app_version("102").is_err());
        assert!(parse_bytes_a_ota_app_version("01XZ").is_err());
    }

    #[test]
    fn build_bytes_ota_honors_custom_chunk_size() {
        let data = vec![0xA5; 513];
        let packets = build_transfer_packets_for_format(
            TransferKind::BcOta,
            OtaTransferFormat::Bytes,
            &data,
            1,
            "",
            TransferFormatOptions {
                bytes_chunk_size: 256,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(packets.len(), 5);

        let first_data = render_transfer_packet_payload(&packets[1], "34B7DA848802").unwrap();
        assert_eq!(first_data[17], 0xC2);
        assert_eq!(first_data.len(), 26 + 2 + 256 + 2);

        let end = render_transfer_packet_payload(&packets[4], "34B7DA848802").unwrap();
        assert_eq!(
            u32::from_be_bytes([end[26], end[27], end[28], end[29]]),
            768
        );
    }

    #[test]
    fn build_bytes_ota_allows_more_than_legacy_chunk_limit() {
        let data = vec![0xA5; 3402 * 512];
        let packets = build_transfer_packets_for_format(
            TransferKind::BcOta,
            OtaTransferFormat::Bytes,
            &data,
            1,
            "",
            TransferFormatOptions {
                bytes_chunk_size: 512,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(packets.len(), 3404);
        let last_data = render_transfer_packet_payload(&packets[3402], "34B7DA848802").unwrap();
        assert_eq!(last_data[17], 0xC2);
        assert_eq!(u16::from_be_bytes([last_data[26], last_data[27]]), 3401);
        let end = render_transfer_packet_payload(&packets[3403], "34B7DA848802").unwrap();
        assert_eq!(
            u32::from_be_bytes([end[26], end[27], end[28], end[29]]),
            (3402 * 512) as u32
        );
    }

    #[test]
    fn build_bytes_control_packet_builds_reset_command() {
        let packet = build_bytes_control_packet(0x4C, Vec::new(), 1);
        let payload = render_transfer_packet_payload(&packet, "34B7DA848802").unwrap();

        assert_eq!(packet.opcode, 0x4C);
        assert_eq!(payload[17], 0x4C);
        assert_eq!(u16::from_be_bytes([payload[2], payload[3]]), 26);
    }

    #[test]
    fn decode_bytes_transfer_ack_payload_maps_status_and_index() {
        let frame = BytesTransferFrame {
            command: 0xC3,
            message_id: 42,
            protocol_version: 1,
            body: vec![0, 0, 7],
        };
        let payload = render_bytes_transfer_frame(&frame, "34B7DA848802").unwrap();
        let decoded = decode_bytes_transfer_ack_payload(&payload).unwrap();

        assert_eq!(decoded["opcode"], 0xC3);
        assert_eq!(decoded["value"], "Execute OK");
        assert_eq!(decoded["return_code"], 0);
        assert_eq!(decoded["time_stamp"], 42);
        assert_eq!(decoded["ota_data_index"], 7);
    }

    #[test]
    fn render_bytes_transfer_frame_rejects_non_hex_device_id() {
        let frame = BytesTransferFrame {
            command: 0xC0,
            message_id: 42,
            protocol_version: 1,
            body: vec![1],
        };
        let error = render_bytes_transfer_frame(&frame, "dev-a").unwrap_err();
        assert!(error.contains("十六进制"));
    }

    #[test]
    fn build_voice_packets_have_crc_on_end_packet() {
        let data = b"\x01\x02\x03\x04";
        let packets =
            build_transfer_packets(TransferKind::VoiceFile, data, 1, "demo.adpcm").unwrap();
        assert_eq!(packets[0]["opcode"], 0x54);
        assert_eq!(packets[1]["opcode"], 0x56);
        assert_eq!(packets[2]["opcode"], 0x58);
        assert_eq!(packets[2]["check_sum"], crc16_xmodem(data));
    }

    #[test]
    fn decode_bc_info_reply_details() {
        let payload = json!({
            "opcode": 0x47,
            "version": "10",
            "dev_model": "BC-01",
            "dev_id": "11112345"
        });
        let details = decode_payload_details(&payload);
        assert!(details.iter().any(|(k, v)| k == "产品型号" && v == "BC-01"));
        assert!(
            details
                .iter()
                .any(|(k, v)| k == "设备ID" && v == "11112345")
        );
    }

    #[test]
    fn decode_status_query_mac_details() {
        let payload = json!({
            "opcode": 0x1D,
            "value": "08112233445566"
        });
        let details = decode_payload_details(&payload);
        assert!(details.iter().any(|(k, v)| k == "查询码" && v == "8"));
        assert!(
            details
                .iter()
                .any(|(k, v)| k == "MAC地址" && v == "11:22:33:44:55:66")
        );
    }

    #[test]
    fn classify_execute_ok_result() {
        let payload = json!({
            "opcode": 0x49,
            "value": "Execute OK"
        });
        let classified = classify_execution_result(&payload).unwrap();
        assert_eq!(classified.0, "成功");
    }

    #[test]
    fn classify_forward_ok_execute_error_result() {
        let payload = json!({
            "opcode": 0x49,
            "value": "Forward OK and Execute Error"
        });
        let classified = classify_execution_result(&payload).unwrap();
        assert_eq!(classified.0, "错误");
    }

    #[test]
    fn build_play_voice_file_payload() {
        let spec = command_by_key("play_voice_file").unwrap();
        let device = sample_device();
        let mut form = BTreeMap::new();
        form.insert("voice_name".into(), "demo.adpcm".into());
        form.insert("value".into(), "1".into());
        let payload = build_command_payload(spec, &device, &form).unwrap();
        assert_eq!(payload["opcode"], 0x5A);
        assert_eq!(payload["voice_name"], "demo.adpcm");
        assert_eq!(payload["value"], 1);
        assert!(payload.get("time_stamp").and_then(Value::as_u64).is_some());
    }
}
