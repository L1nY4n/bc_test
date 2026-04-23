from __future__ import annotations

import json
from typing import Iterable

from PySide6.QtCore import QObject, Signal
import paho.mqtt.client as mqtt

from .models import BrokerProfile


class MqttController(QObject):
    connection_changed = Signal(bool, str)
    message_received = Signal(str, str, object)
    publish_logged = Signal(str, str)
    system_message = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._client: mqtt.Client | None = None
        self._connected = False
        self._subscriptions: set[str] = set()
        self._profile: BrokerProfile | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    def connect_profile(self, profile: BrokerProfile) -> None:
        self.disconnect_client()
        self._profile = profile
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=profile.client_id,
            protocol=mqtt.MQTTv311,
            clean_session=True,
        )
        if profile.username:
            client.username_pw_set(profile.username, profile.password)
        if profile.use_tls:
            client.tls_set()
            client.tls_insecure_set(False)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        self._client = client
        try:
            client.connect_async(profile.host, profile.port, profile.keepalive)
            client.loop_start()
            self.system_message.emit(f"Connecting to {profile.host}:{profile.port}...")
        except OSError as exc:
            self.connection_changed.emit(False, f"Connect failed: {exc}")

    def disconnect_client(self) -> None:
        if self._client is None:
            return
        try:
            self._client.disconnect()
            self._client.loop_stop()
        except OSError:
            pass
        self._client = None
        self._connected = False
        self.connection_changed.emit(False, "Disconnected")

    def sync_subscriptions(self, topics: Iterable[str]) -> None:
        desired_topics = {topic for topic in topics if topic}
        if self._client is None:
            self._subscriptions = desired_topics
            return
        removed_topics = sorted(self._subscriptions - desired_topics)
        new_topics = sorted(desired_topics - self._subscriptions)
        for topic in removed_topics:
            self._client.unsubscribe(topic)
        for topic in new_topics:
            self._client.subscribe(topic, qos=0)
        self._subscriptions = desired_topics

    def reset_subscriptions(self) -> None:
        self._subscriptions.clear()

    def publish_json(self, topic: str, payload: dict) -> bool:
        if not self._client or not self._connected:
            self.system_message.emit("Publish skipped because the MQTT client is disconnected.")
            return False
        encoded = json.dumps(payload, ensure_ascii=False)
        result = self._client.publish(topic, encoded, qos=0)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            self.system_message.emit(f"Publish failed for topic '{topic}' with code {result.rc}.")
            return False
        self.publish_logged.emit(topic, encoded)
        return True

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: object,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        self._connected = int(reason_code) == 0
        message = "Connected" if self._connected else f"Connect failed: {reason_code}"
        self.connection_changed.emit(self._connected, message)
        if self._connected and self._subscriptions:
            for topic in sorted(self._subscriptions):
                client.subscribe(topic, qos=0)

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: object,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None = None,
    ) -> None:
        self._connected = False
        self.connection_changed.emit(False, f"Disconnected: {reason_code}")

    def _on_message(self, client: mqtt.Client, userdata: object, message: mqtt.MQTTMessage) -> None:
        payload_text = message.payload.decode("utf-8", errors="replace")
        parsed: object
        try:
            parsed = json.loads(payload_text)
        except json.JSONDecodeError:
            parsed = payload_text
        self.message_received.emit(message.topic, payload_text, parsed)
