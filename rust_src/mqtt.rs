use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rumqttc::{
    Client, Connection, Event, Incoming, MqttOptions, QoS, RecvTimeoutError, TlsConfiguration,
    Transport,
};

use crate::models::BrokerProfile;

#[derive(Debug, Clone)]
pub enum MqttEvent {
    Connection {
        generation: u64,
        connected: bool,
        reconnecting: bool,
        message: String,
    },
    Message {
        generation: u64,
        topic: String,
        payload: String,
    },
}

pub struct MqttRuntime {
    client: Option<Client>,
    worker: Option<JoinHandle<()>>,
    stop_signal: Option<Arc<AtomicBool>>,
    events_tx: Sender<MqttEvent>,
    mirror_tx: Option<Sender<MqttEvent>>,
    pub events_rx: Receiver<MqttEvent>,
    subscriptions: BTreeSet<String>,
    generation: u64,
}

const MAX_INBOUND_PAYLOAD_BYTES: usize = 64 * 1024;
const AUTO_RECONNECT_DELAY_SECS: u64 = 1;
const CONNECTION_RECV_TIMEOUT_MS: u64 = 250;

impl Default for MqttRuntime {
    fn default() -> Self {
        let (events_tx, events_rx) = mpsc::channel();
        Self {
            client: None,
            worker: None,
            stop_signal: None,
            events_tx,
            mirror_tx: None,
            events_rx,
            subscriptions: BTreeSet::new(),
            generation: 0,
        }
    }
}

impl MqttRuntime {
    pub fn connect(&mut self, broker: &BrokerProfile) {
        self.disconnect();
        self.subscriptions.clear();
        let generation = self.next_generation();

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
        let mirror_sender = self.mirror_tx.clone();
        let stop_signal = Arc::new(AtomicBool::new(false));
        let worker_stop_signal = Arc::clone(&stop_signal);
        let worker = thread::spawn(move || {
            Self::pump_connection(
                &mut connection,
                sender,
                mirror_sender,
                generation,
                worker_stop_signal,
            );
        });

        self.client = Some(client);
        self.worker = Some(worker);
        self.stop_signal = Some(stop_signal);
    }

    fn pump_connection(
        connection: &mut Connection,
        sender: Sender<MqttEvent>,
        mirror_sender: Option<Sender<MqttEvent>>,
        generation: u64,
        stop_signal: Arc<AtomicBool>,
    ) {
        let mut recovering_after_disconnect = false;
        let mut announced_loss = false;
        let mut had_connected_once = false;
        while !stop_signal.load(Ordering::Relaxed) {
            let notification =
                match connection.recv_timeout(Duration::from_millis(CONNECTION_RECV_TIMEOUT_MS)) {
                    Ok(notification) => notification,
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
            match notification {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    let message = if recovering_after_disconnect {
                        "已重连"
                    } else {
                        "已连接"
                    };
                    recovering_after_disconnect = false;
                    announced_loss = false;
                    had_connected_once = true;
                    Self::send_event(
                        &sender,
                        mirror_sender.as_ref(),
                        MqttEvent::Connection {
                            generation,
                            connected: true,
                            reconnecting: false,
                            message: message.into(),
                        },
                    );
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
                    Self::send_event(
                        &sender,
                        mirror_sender.as_ref(),
                        MqttEvent::Message {
                            generation,
                            topic: publish.topic,
                            payload,
                        },
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    if !announced_loss {
                        let message = if had_connected_once {
                            format!("连接断开，自动重连中: {err}")
                        } else {
                            format!("连接失败，自动重连中: {err}")
                        };
                        Self::send_event(
                            &sender,
                            mirror_sender.as_ref(),
                            MqttEvent::Connection {
                                generation,
                                connected: false,
                                reconnecting: true,
                                message,
                            },
                        );
                        announced_loss = true;
                    }
                    if had_connected_once {
                        recovering_after_disconnect = true;
                    }
                    thread::sleep(Duration::from_secs(AUTO_RECONNECT_DELAY_SECS));
                }
            }
        }
    }

    fn send_event(
        sender: &Sender<MqttEvent>,
        mirror_sender: Option<&Sender<MqttEvent>>,
        event: MqttEvent,
    ) {
        let _ = sender.send(event.clone());
        if let Some(mirror_sender) = mirror_sender {
            let _ = mirror_sender.send(event);
        }
    }

    pub fn disconnect(&mut self) {
        let had_connection = self.client.is_some() || self.worker.is_some();
        let generation = self.next_generation();
        if let Some(stop_signal) = self.stop_signal.take() {
            stop_signal.store(true, Ordering::Relaxed);
        }
        if let Some(client) = &self.client {
            let _ = client.disconnect();
        }
        self.client = None;
        if let Some(worker) = self.worker.take() {
            thread::spawn(move || {
                let _ = worker.join();
            });
        }
        if had_connection {
            Self::send_event(
                &self.events_tx,
                self.mirror_tx.as_ref(),
                MqttEvent::Connection {
                    generation,
                    connected: false,
                    reconnecting: false,
                    message: "已断开".into(),
                },
            );
        }
    }

    pub fn set_event_mirror(&mut self, mirror_tx: Option<Sender<MqttEvent>>) {
        self.mirror_tx = mirror_tx;
    }

    pub fn client_handle(&self) -> Option<Client> {
        self.client.clone()
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

    pub fn reapply_subscriptions(&mut self) {
        let Some(client) = &self.client else {
            return;
        };
        for topic in &self.subscriptions {
            let _ = client.subscribe(topic, QoS::AtMostOnce);
        }
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

    pub fn is_current_generation(&self, generation: u64) -> bool {
        self.generation == generation
    }

    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }
}
