pub mod register_client {
    use crate::{
        serialize_register_command, Broadcast, RegisterClient, RegisterCommand,
        SystemRegisterCommand,
    };
    use async_channel::Sender;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::{Mutex, RwLock};
    use tokio::time::{interval, Duration};
    use uuid::Uuid;

    pub struct Stubborn {
        self_identifier: u8,
        tcp_streams: Arc<Mutex<Vec<Option<TcpStream>>>>,
        tcp_locations: Vec<(String, u16)>,
        loopback: Sender<SystemRegisterCommand>,
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
                tcp_streams.push(None);
            }
            Stubborn {
                self_identifier: ident,
                tcp_streams: Arc::new(Mutex::new(tcp_streams)),
                tcp_locations,
                loopback: sender,
                hmac_system_key,
                broadcast_pool,
            }
        }

        async fn send_cmd(&self, proc_ident: u8, cmd: SystemRegisterCommand) {
            if proc_ident == self.self_identifier {
                log::debug!("\nSEND cmd to myself {} through loopback {:?} ", self.self_identifier, cmd);
                if let Err(e) = self.loopback.send(cmd).await {
                    log::error!("reg_client:{}, loopback error {}", self.self_identifier, e);
                }
            } else {
                log::debug!("\nSEND cmd from {} to {} through tcp {:?}", self.self_identifier, proc_ident, cmd);
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
                let mut vec_guard = self.tcp_streams.lock().await;
                if let Some(stream) = &mut vec_guard[proc_ident as usize - 1] {
                    if let Err(e) = stream.write_all(&data).await {

                            log::error!(
                                "reg_client:{}, stream.write_all error {}",
                                self.self_identifier,
                                e
                            );
                    }
                    break;
                } else {
                    let (host, port) = &self.tcp_locations[proc_ident as usize - 1];
                    match tokio::net::TcpStream::connect((host.as_str(), *port)).await {
                        Ok(s) => {
                            vec_guard[proc_ident as usize - 1] = Some(s);
                        }
                        Err(e) => {
                            log::warn!(
                                "cannot reconnect with: ({}, {}) with error: {:?}",
                                host,
                                port,
                                e
                            );
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
            let broadcast_pool = self.broadcast_pool.clone();
            let self_identifier = self.self_identifier;
            let loopback = self.loopback.clone();
            let tcp_streams = self.tcp_streams.clone();
            let tcp_locations = self.tcp_locations.clone();
            let cmd = msg.cmd.as_ref().clone();
            let mut data = Vec::new();
            if let Err(e) = serialize_register_command(
                &RegisterCommand::System(cmd.clone()),
                &mut data,
                &self.hmac_system_key,
            )
            .await
            {
                log::error!(
                    "reg_client:{}, serialize_register_command error {}",
                    self.self_identifier,
                    e
                )
            }
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_millis(100));
                let mut i: u32 = 0;
                loop {
                    interval.tick().await;
                    i+=1;
                    let k = msg.cmd.header.msg_ident;
                    let map = broadcast_pool.read().await;
                    if i <= 3 {
                        log::debug!("\nB i:{}. map: {:?}", i, *map);
                    }
                    if let Some(set) = map.get(&k) {
                        if set.is_empty() {
                            break;
                        } else {
                            let s = set.clone();
                            drop(map);
                            for &proc_ident in s.iter() {
                                //self.send_cmd(proc_ident, msg.cmd.as_ref().clone()).await;
                                if proc_ident == self_identifier {
                                    if i <= 3 {
                                        log::debug!("\nB sending cmd through loopback to myself {} {:?}", proc_ident, cmd);
                                    }
                                    if let Err(e) = loopback.send(cmd.clone()).await {
                                        log::error!(
                                        "reg_client:{}, loopback error {}",
                                        self_identifier,
                                        e
                                    );
                                    }
                                } else {
                                    if i <= 3 {
                                        log::debug!("\nB sending cmd from {} to {} through tcp {:?}", self_identifier ,proc_ident ,cmd);
                                    }
                                    //self.send_serialized_cmd_over_tcp(proc_ident, data.clone()).await
                                    let mut vec_guard = tcp_streams.lock().await;
                                    if let Some(stream) = &mut vec_guard[proc_ident as usize - 1] {
                                        if i <= 3 {
                                            log::debug!("\nB me {} Some(stream) to send to process {} cmd {:?}", self_identifier ,proc_ident ,cmd);
                                        }
                                        if let Err(e) = stream.write_all(&data).await {
                                            log::error!(
                                                "reg_client:{}, stream.write_all error {}",
                                                self_identifier,
                                                e
                                            )
                                        }
                                    } else {
                                        let (host, port) = &tcp_locations[proc_ident as usize - 1];
                                        match tokio::net::TcpStream::connect((host.as_str(), *port))
                                            .await
                                        {
                                            Ok(s) => {
                                                if i <= 3 {
                                                    log::debug!("\nB me {} None stream but reconnected to send to process {} cmd {:?}", self_identifier ,proc_ident ,cmd);
                                                }
                                                vec_guard[proc_ident as usize - 1] = Some(s);
                                            }
                                            Err(e) => {
                                                log::warn!(
                                                "cannot reconnect with: ({}, {}) with error: {:?}",
                                                host,
                                                port,
                                                e
                                            );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            });
        }
    }
}
