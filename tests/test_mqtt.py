from __future__ import annotations

import unittest

from mesh_bc_tester.models import BrokerProfile
from mesh_bc_tester.mqtt import MqttController


class _FakeClient:
    def __init__(self) -> None:
        self.subscribed: list[tuple[str, int]] = []
        self.unsubscribed: list[str] = []

    def subscribe(self, topic: str, qos: int = 0) -> None:
        self.subscribed.append((topic, qos))

    def unsubscribe(self, topic: str) -> None:
        self.unsubscribed.append(topic)


class MqttControllerTests(unittest.TestCase):
    def test_sync_subscriptions_adds_and_removes_topics(self) -> None:
        controller = MqttController()
        client = _FakeClient()
        controller._client = client  # type: ignore[attr-defined]
        controller._subscriptions = {"keep/topic", "old/topic"}  # type: ignore[attr-defined]

        controller.sync_subscriptions(["keep/topic", "new/topic"])

        self.assertEqual(client.subscribed, [("new/topic", 0)])
        self.assertEqual(client.unsubscribed, ["old/topic"])
        self.assertEqual(controller._subscriptions, {"keep/topic", "new/topic"})  # type: ignore[attr-defined]

    def test_broker_password_not_serialized_by_default(self) -> None:
        broker = BrokerProfile(password="secret")
        self.assertEqual(broker.to_dict()["password"], "")
        self.assertEqual(broker.to_dict(include_password=True)["password"], "secret")


if __name__ == "__main__":
    unittest.main()
