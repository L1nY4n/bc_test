use std::{
    collections::BTreeSet,
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, JoinHandle},
    time::Duration,
};

use rumqttc::{Client, Connection, Event, Incoming, MqttOptions, QoS, TlsConfiguration, Transport};

use crate::models::BrokerProfile;

#[derive(Debug)]
pub enum MqttEvent {
    Connection { message: String },
    Message { topic: String, payload: String },
}

pub struct MqttRuntime {
    client: Option<Client>,
    worker: Option<JoinHandle<()>>,
    events_tx: Sender<MqttEvent>,
    pub events_rx: Receiver<MqttEvent>,
    subscriptions: BTreeSet<String>,
}

const MAX_INBOUND_PAYLOAD_BYTES: usize = 64 * 1024;

impl Default for MqttRuntime {
    fn default() -> Self {
        let (events_tx, events_rx) = mpsc::channel();
        Self {
            client: None,
            worker: None,
            events_tx,
            events_rx,
            subscriptions: BTreeSet::new(),
        }
    }
}

impl MqttRuntime {
    pub fn connect(&mut self, broker: &BrokerProfile) {
        self.disconnect();

        let mut options =
            MqttOptions::new(broker.client_id.clone(), broker.host.clone(), broker.port);
        options.set_keep_alive(Duration::from_secs(u64::from(broker.keepalive_secs)));
        if !broker.username.is_empty() {
            options.set_credentials(broker.username.clone(), broker.password.clone());
        }
        if broker.use_tls {
            options.set_transport(Transport::Tls(TlsConfiguration::default()));
        }

        let (client, mut connection) = Client::new(options, 100);
        let sender = self.events_tx.clone();
        let worker = thread::spawn(move || {
            Self::pump_connection(&mut connection, sender);
        });

        self.client = Some(client);
        self.worker = Some(worker);
    }

    fn pump_connection(connection: &mut Connection, sender: Sender<MqttEvent>) {
        for notification in connection.iter() {
            match notification {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    let _ = sender.send(MqttEvent::Connection {
                        message: "已连接".into(),
                    });
                }
                Ok(Event::Incoming(Incoming::Publish(publish))) => {
                    let payload_bytes: &[u8] = if publish.payload.len() > MAX_INBOUND_PAYLOAD_BYTES
                    {
                        &publish.payload[..MAX_INBOUND_PAYLOAD_BYTES]
                    } else {
                        &publish.payload
                    };
                    let mut payload = String::from_utf8_lossy(payload_bytes).to_string();
                    if publish.payload.len() > MAX_INBOUND_PAYLOAD_BYTES {
                        payload.push_str("\n...[payload truncated]...");
                    }
                    let _ = sender.send(MqttEvent::Message {
                        topic: publish.topic,
                        payload,
                    });
                }
                Ok(_) => {}
                Err(err) => {
                    let _ = sender.send(MqttEvent::Connection {
                        message: format!("连接断开: {err}"),
                    });
                    break;
                }
            }
        }
    }

    pub fn disconnect(&mut self) {
        if let Some(client) = &self.client {
            let _ = client.disconnect();
        }
        self.client = None;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        let _ = self.events_tx.send(MqttEvent::Connection {
            message: "已断开".into(),
        });
    }

    pub fn sync_subscriptions(&mut self, topics: impl IntoIterator<Item = String>) {
        let desired: BTreeSet<String> = topics
            .into_iter()
            .filter(|topic| !topic.is_empty())
            .collect();
        if let Some(client) = &self.client {
            for removed in self.subscriptions.difference(&desired) {
                let _ = client.unsubscribe(removed);
            }
            for added in desired.difference(&self.subscriptions) {
                let _ = client.subscribe(added, QoS::AtMostOnce);
            }
        }
        self.subscriptions = desired;
    }

    pub fn publish_json(&mut self, topic: &str, payload: &str) -> Result<(), String> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| "MQTT 客户端未连接".to_string())?;
        client
            .publish(topic, QoS::AtMostOnce, false, payload.as_bytes())
            .map_err(|err| err.to_string())
    }
}
