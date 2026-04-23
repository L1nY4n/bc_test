from __future__ import annotations

import unittest

from mesh_bc_tester.models import DeviceProfile, TransferKind
from mesh_bc_tester.protocol import (
    COMMAND_MAP,
    build_command_payload,
    build_transfer_packets,
    bytes_to_hex,
    crc16_xmodem,
    crc16_xmodem_bytes_little_endian,
    hex_to_bytes,
    parse_opcode,
)


class ProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.device = DeviceProfile(
            name="Lane 1",
            device_id="11112345",
            up_topic="/application/AP-C-BM/device/11112345/up",
            down_topic="/application/AP-C-BM/device/11112345/down",
            mesh_dev_type=1,
            default_dest_addr=0x1001,
        )

    def test_crc16_xmodem_known_vector(self) -> None:
        self.assertEqual(crc16_xmodem(b"123456789"), 0x31C3)
        self.assertEqual(crc16_xmodem_bytes_little_endian(b"123456789"), bytes([0xC3, 0x31]))

    def test_hex_roundtrip(self) -> None:
        data = b"\x05\x3F\x04\x11"
        encoded = bytes_to_hex(data)
        self.assertEqual(encoded, "053F0411")
        self.assertEqual(hex_to_bytes(encoded), data)

    def test_build_query_bc_info(self) -> None:
        payload = build_command_payload(
            COMMAND_MAP["query_bc_info"],
            self.device,
            {"value": 0, "time_stamp": 123456},
        )
        self.assertEqual(payload["opcode"], 0x46)
        self.assertEqual(payload["value"], 0)
        self.assertEqual(payload["time_stamp"], 123456)
        self.assertNotIn("dest_addr", payload)

    def test_build_scene_payload(self) -> None:
        payload = build_command_payload(
            COMMAND_MAP["upsert_scene"],
            self.device,
            {
                "dest_addr": 0x1001,
                "scene_id": 2,
                "occupied_brightness": 100,
                "idle_brightness": 10,
                "auto_off_seconds": 30,
                "running_mode": 3,
                "switch_state": 1,
            },
        )
        self.assertEqual(payload["opcode"], 0x23)
        self.assertEqual(payload["dest_addr"], 0x1001)
        self.assertEqual(payload["value"], "02640A1E0301000000000000")

    def test_voice_transfer_packets(self) -> None:
        packets = build_transfer_packets(
            TransferKind.VOICE_FILE,
            b"\x01\x02\x03\x04",
            version=1,
            voice_name="demo.adpcm",
        )
        self.assertEqual(packets[0][0]["opcode"], 0x54)
        self.assertEqual(packets[1][0]["opcode"], 0x56)
        self.assertEqual(packets[-1][0]["opcode"], 0x58)
        self.assertEqual(packets[1][0]["value"], "01020304")
        self.assertEqual(packets[-1][0]["check_sum"], crc16_xmodem(b"\x01\x02\x03\x04"))

    def test_bc_ota_transfer_packets(self) -> None:
        data = bytes(range(16))
        packets = build_transfer_packets(TransferKind.BC_OTA, data, version=7, voice_name="")
        self.assertEqual(packets[0][0]["opcode"], 0x40)
        self.assertEqual(packets[1][0]["opcode"], 0x42)
        self.assertEqual(packets[1][0]["ota_data_index"], 0)
        self.assertEqual(packets[1][0]["ota_byte_size"], 16)

    def test_parse_opcode(self) -> None:
        self.assertEqual(parse_opcode(0x46), 0x46)
        self.assertEqual(parse_opcode("70"), 70)
        self.assertEqual(parse_opcode("0x46"), 0x46)
        self.assertIsNone(parse_opcode(""))
        with self.assertRaises(ValueError):
            parse_opcode("not-an-opcode")

    def test_empty_transfer_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_transfer_packets(TransferKind.BC_OTA, b"", version=1, voice_name="")


if __name__ == "__main__":
    unittest.main()
