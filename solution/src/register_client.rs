pub mod register_client {
    use crate::{
        serialize_register_command, Broadcast, RegisterClient, RegisterCommand,
        SystemRegisterCommand,
    };
    use async_channel::{ Sender};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::{Mutex, RwLock};
    use tokio::time::{interval, Duration};
    use uuid::Uuid;

    pub struct Stubborn {
        self_identifier: u8,
        tcp_streams: Vec<Mutex<Option<TcpStream>>>,
        tcp_locations: Vec<(String, u16)>,
        loopback: Sender<SystemRegisterCommand>,
        buffered_msgs: Mutex<HashMap<Uuid, SystemRegisterCommand>>,
        hmac_system_key: [u8; 64],
        broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    }

    impl Stubborn {
        pub fn new(
            ident: u8,
            tcp_locations: Vec<(String, u16)>,
            sender: Sender<SystemRegisterCommand>,
            hmac_system_key: [u8; 64],
            broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
        ) -> Self {
            let mut tcp_streams = Vec::with_capacity(tcp_locations.len());
            for _ in 0..tcp_locations.len() {
                tcp_streams.push(Mutex::new(None));
            }
            Stubborn {
                self_identifier: ident,
                tcp_streams,
                tcp_locations,
                loopback: sender,
                buffered_msgs: Mutex::new(HashMap::new()),
                hmac_system_key,
                broadcast_pool,
            }
        }

        async fn send_cmd(&self, proc_ident: u8, cmd: SystemRegisterCommand) {
            if proc_ident == self.self_identifier {
                log::debug!("sending cmd {:?} through loopback", cmd.header);
                if let Err(e) = self.loopback.send(cmd).await {
                    log::error!("reg_client:{}, loopback error {}", self.self_identifier, e);
                }
            } else {
                log::debug!("sending cmd {:?} through tcp", cmd.header);
                let mut data = Vec::new();
                match serialize_register_command(
                    &RegisterCommand::System(cmd),
                    &mut data,
                    &self.hmac_system_key,
                )
                .await
                {
                    Ok(_) => self.send_serialized_cmd_over_tcp(proc_ident, data).await,
                    Err(e) => log::error!(
                        "reg_client:{}, serialize_register_command error {}",
                        self.self_identifier,
                        e
                    ),
                }
            }
        }

        async fn send_serialized_cmd_over_tcp(&self, proc_ident: u8, data: Vec<u8>) {
            let mut interval = interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let mut stream_guard = self.tcp_streams[proc_ident as usize - 1].lock().await;
                if let Some(stream) = stream_guard.as_mut() {
                    if let Err(e) = stream.write_all(&data).await {
                        log::error!(
                            "reg_client:{}, stream.write_all error {}",
                            self.self_identifier,
                            e
                        )
                    }
                } else {
                    let (host, port) = &self.tcp_locations[proc_ident as usize - 1];
                    match tokio::net::TcpStream::connect((host.as_str(), *port)).await {
                        Ok(s) => {
                            *stream_guard = Some(s);
                            break;
                        }
                        Err(e) => {
                            log::warn!("cannot reconnect with: ({}, {}) with error: {:?}", host, port, e);
                            //continue;
                        }
                    }
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl RegisterClient for Stubborn {
        async fn send(&self, msg: crate::Send) {
            self.send_cmd(msg.target as u8, msg.cmd.as_ref().clone())
                .await;
        }

        async fn broadcast(&self, msg: Broadcast) {
            let k = msg.cmd.header.msg_ident;
            log::debug!("wanting to broadcast waiting on lock");
            let map = self.broadcast_pool.read().await;
            log::debug!("broadcasting msg.cmd {:?} map {:?}", msg.cmd.header, *map);
            if let Some(set) = map.get(&k) {
                for &proc_ident in set.iter() {
                    self.send_cmd(proc_ident, msg.cmd.as_ref().clone()).await;
                }
            }
        }
    }
}
