pub mod register_client {
    use crate::{Broadcast, RegisterClient, RegisterCommand, serialize_register_command, SystemRegisterCommand};
    use std::collections::HashMap;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc::UnboundedSender;
    use uuid::Uuid;
    use std::sync::{Arc};
    use tokio::sync::Mutex;
    use tokio::io::AsyncWriteExt;

    pub struct Sbeb {
        self_identifier: u8,
        tcp_streams: Vec<Arc<Mutex<Option<TcpStream>>>>, // TODO bez Arc jesli mutex tokio
        tcp_locations: Vec<(String, u16)>,
        loopback: UnboundedSender<SystemRegisterCommand>,
        buffered_msgs: Mutex<HashMap<Uuid, SystemRegisterCommand>>,
        hmac_system_key: [u8; 64],
    }

    impl Sbeb {
        pub fn new(
            ident: u8,
            tcp_locations: Vec<(String, u16)>,
            sender: UnboundedSender<SystemRegisterCommand>,
            hmac_system_key: [u8; 64],
        ) -> Self {
            let mut tcp_streams = Vec::with_capacity(tcp_locations.len());
            for _ in 0..tcp_locations.len() {
                tcp_streams.push(Arc::new(Mutex::new(None)));
            }
            Sbeb {
                self_identifier: ident,
                tcp_streams,
                tcp_locations,
                loopback: sender,
                buffered_msgs: Mutex::new(HashMap::new()),
                hmac_system_key,
            }
        }
    }

    #[async_trait::async_trait]
    impl RegisterClient for Sbeb {
        async fn send(&self, msg: crate::Send) {
            let header = msg.cmd.header.clone();
            let content = msg.cmd.content.clone();
            let target = msg.target;
            if target == (self.self_identifier as usize) {
                if let Err(e) = self.loopback.send(SystemRegisterCommand{header, content}) {
                    log::error!("reg_client:{}, loopback error {}", self.self_identifier, e);
                }
            } else {
                // sync
                //let mut stream_guard = self.tcp_streams[target-1].lock().unwrap();
                //if let Some(stream) = *stream_guard {}
                // tokio::sync::Mutex
                let mut stream_guard = self.tcp_streams[target-1].lock().await;
                if let Some(stream) = stream_guard.as_mut() {
                    let mut data = Vec::new();
                    let cmd = RegisterCommand::System(SystemRegisterCommand{header, content});
                    match serialize_register_command(&cmd, &mut data, &self.hmac_system_key).await {
                        Ok(_) => {
                            if let Err(e) = stream.write_all(&data).await {
                                log::error!("reg_client:{}, stream.write_all error {}", self.self_identifier, e)
                            }
                        },
                        Err(e) => log::error!("reg_client:{}, serialize_register_command error {}", self.self_identifier, e)
                    }
                } else { // cannot send right now => add to buffer and it will be sent eventually
                    // std::sync::Mutex
                    //let buffered_msgs_guard = self.buffered_msgs.lock().unwrap();
                    //(*buffered_msgs_guard).insert(header.msg_ident, SystemRegisterCommand{header, content}).unwrap();
                    // tokio::sync::Mutex
                    let mut buffered_msgs_guard = self.buffered_msgs.lock().await;
                    buffered_msgs_guard.insert(header.msg_ident, SystemRegisterCommand{header, content}).unwrap();
                }

            }
        }

        async fn broadcast(&self, msg: Broadcast) {
            for target_m1 in 0..self.tcp_locations.len() {
               self.send(crate::Send{
                    cmd: msg.cmd.clone(),
                    target: target_m1+1,
                }).await;
            }
        }
    }
/*
    use futures::future::join_all;

    async fn foo(i: u32) -> u32 { i }

    let futures = vec![foo(1), foo(2), foo(3)];

    assert_eq!(join_all(futures).await, [1, 2, 3]);*/
}
