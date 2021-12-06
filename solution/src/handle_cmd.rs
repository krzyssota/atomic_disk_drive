#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

pub mod cmd {
    use std::io::Error;
    use hmac::{Hmac, Mac, NewMac};
    use sha2::Sha256;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use crate::{deserialize_register_command, RegisterCommand};

    pub async fn get_cmd(mut read_stream: OwnedReadHalf, write_stream: OwnedWriteHalf,
                      hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], max_sector: u64) -> RegisterCommand {
// todo add loop here
            let mut _buff: [u8; 1] = [0];
        /*    match read_stream.peek(&mut _buff).await {
                Err(e) => {
                    log::error!("Error while peeking OwnedReadHalf of Tcp stream: {:?}", e);
                },
                Ok(0) => {}, // no bytes to read     // if in loop add break;
                Ok(_) => {
                    match deserialize_register_command(&mut read_stream,
                                                       &hmac_system_key, &hmac_client_key).await {
                        Ok((cmd, bool)) => {

                        }
                        Err(_) => {}
                    }
                }
            }*/
        match deserialize_register_command(&mut read_stream,
                                           &hmac_system_key, &hmac_client_key).await {
            Ok((cmd, bool)) => { cmd }
            Err(_) => { todo!() }
        }
    }

    pub async fn handle_cmd(cmd: RegisterCommand) {

    }

    type HmacSha256 = Hmac<Sha256>;
    pub(crate) fn verify_hmac_tag(tag: &[u8], message: &[u8], hmac_key: &[u8]) -> bool {
        // Initialize a new MAC instance from the secret key:
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
        // Calculate MAC for the data (one can provide it in multiple portions):
        mac.update(message);
        // Verify the tag:
        mac.verify(tag).is_ok()
    }

    pub(crate) fn calculate_hmac_tag(message: &[u8], hmac_key: &[u8]) -> [u8; 32] {
        // Initialize a new MAC instance from the secret key:
        let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();

        // Calculate MAC for the data (one can provide it in multiple portions):
        mac.update(message);

        // Finalize the computations of MAC and obtain the resulting tag:
        let tag = mac.finalize().into_bytes();

        tag.into()
    }
}

