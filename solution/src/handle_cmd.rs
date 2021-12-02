
pub mod cmd {
    use std::io::Error;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use crate::{deserialize_register_command, RegisterCommand};

    pub async fn get_cmd(mut read_stream: OwnedReadHalf, write_stream: OwnedWriteHalf,
                      hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], max_sector: u64) -> RegisterCommand {

            let mut _buff: [u8; 1] = [0];
            match read_stream.peek(&mut *_buff).await {
                Err(e) => log::error!("Error while peeking OwnedReadHalf of Tcp stream: {:?}", e);
                Ok(0) => {} // no bytes to read     // if in loop add break;
                Ok(_) => {
                    match deserialize_register_command(&mut read_stream,
                                                       &hmac_system_key, &hmac_client_key).await {
                        Ok((cmd, bool)) => {

                        }
                        Err(_) => {}
                    }
                }
            }
    }

    pub async fn handle_cmd(cmd: RegisterCommand) {

    }
}

