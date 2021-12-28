pub mod transfer {
    use crate::RegisterCommand::{System};
    use crate::{ClientCommandHeader, ReadReturn, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, MAGIC_NUMBER, ACK_MSG_T, READ_MSG_T, READ_PROC_MSG_T, SECTOR_SIZE, VALUE_MSG_T, HMAC_TAG_SIZE, WRITE_MSG_T, WRITE_PROC_MSG_T, OperationComplete, OperationReturn};
    use std::convert::{TryInto};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use uuid::Uuid;
    use crate::utils::utils;
    use std::io::{Error};


    pub async fn serialize_client_response(op: OperationComplete,  writer: &mut (dyn AsyncWrite + Send + Unpin),
                                           hmac_key: [u8; 32], fail: bool) {
        let mut msg: Vec<u8> = Vec::with_capacity(2 * 8 + HMAC_TAG_SIZE);
        msg.append(&mut MAGIC_NUMBER.to_vec());
        msg.append(&mut vec![0; 2]); // padding
        msg.push(op.status_code as u8);
        match op.op_return {
            OperationReturn::Read(_) => msg.push(READ_MSG_T + 0x40),
            OperationReturn::Write => msg.push(WRITE_MSG_T + 0x40)
        }
        let mut req_id = op.request_identifier.to_be_bytes().to_vec();
        msg.append(&mut req_id);
        match op.op_return {
            OperationReturn::Read(ReadReturn{read_data}) => {
                if !fail {
                    if let Some(SectorVec(mut data)) = read_data {
                        msg.append(&mut data)
                    } else {
                        log::error!("serialize_client_response OperationComplete: {} came with None ReadReturn", op.request_identifier);
                    }
                }
            }
            OperationReturn::Write => {}
        }
        let tag = utils::calculate_hmac_tag(&msg, &hmac_key);
        msg.append(&mut tag.to_vec());
        writer.write_all(&msg).await.unwrap();
    }

    pub async fn serialize_client_command(
        cmd: &ClientRegisterCommand,
        msg: &mut Vec<u8>,
    ) {
        //log::debug!("serializing client command");
        msg.append(&mut vec![0; 3]); // padding
        let mut req_id = cmd.header.request_identifier.to_be_bytes().to_vec();
        let mut sec_idx = cmd.header.sector_idx.to_be_bytes().to_vec();
        match cmd.clone().content {
            ClientRegisterCommandContent::Read => {
                msg.push(READ_MSG_T);
                msg.append(&mut req_id);
                msg.append(&mut sec_idx);
            }
            ClientRegisterCommandContent::Write {
                data: SectorVec(mut vec),
            } => {
                msg.push(WRITE_MSG_T);
                msg.append(&mut req_id);
                msg.append(&mut sec_idx);
                msg.append(&mut vec);
            }
        };
        //log::debug!("msg {:?}", msg);
    }

    pub async fn serialize_system_command(
        cmd: &SystemRegisterCommand,
        msg: &mut Vec<u8>,
    ) {
        //log::debug!("serializing system command");
        msg.append(&mut [0_u8; 2].to_vec()); // padding
        log::debug!("SER padding {}", 2);

        let mut rank = cmd.header.process_identifier.to_be_bytes().to_vec();
        log::debug!("SER rank {}", rank.len());
        msg.append(&mut rank);

       /* let SystemRegisterCommand{header: _, content} = cmd;
        let msg_type =  match content {
                SystemRegisterCommandContent::Ack => {
                    ACK_MSG_T
                }
                SystemRegisterCommandContent::Value{..} => {
                    VALUE_MSG_T
                }
                SystemRegisterCommandContent::WriteProc{ .. } => {
                    WRITE_PROC_MSG_T
                }
                SystemRegisterCommandContent::ReadProc => {
                    READ_PROC_MSG_T
                }
            };
        msg.push(msg_type);
        log::debug!("SER msg_type {}", 1);*/

        let mut uuid = cmd.header.msg_ident.as_bytes().to_vec();
        log::debug!("SER uuid {}", uuid.len());

        let mut rid = cmd.header.read_ident.to_be_bytes().to_vec();
        log::debug!("SER rid {}", rid.len());

        let mut sec_idx = cmd.header.sector_idx.to_be_bytes().to_vec();
        log::debug!("SER idx {}", sec_idx.len());

        match cmd.clone().content {
            SystemRegisterCommandContent::ReadProc => {
                msg.push(READ_PROC_MSG_T);
                msg.append(&mut uuid);
                msg.append(&mut rid);
                msg.append(&mut sec_idx);
            }
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data: SectorVec(mut data),
            } => {
                msg.push(VALUE_MSG_T);
                msg.append(&mut uuid);
                msg.append(&mut rid);
                msg.append(&mut sec_idx);
                msg.append(&mut timestamp.to_be_bytes().to_vec());
                log::debug!("SER timestamp {}", timestamp.to_be_bytes().to_vec().len());
                msg.append(&mut [0; 7].to_vec()); // padding
                msg.push(write_rank);
                log::debug!("SER data {}", data.len());
                let d = data.clone();
                log::info!("SER V {:?}", &d[SECTOR_SIZE-10..]);
                msg.append(&mut data);
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write: SectorVec(mut data),
            } => {
                msg.push(WRITE_PROC_MSG_T);
                msg.append(&mut uuid);
                msg.append(&mut rid);
                msg.append(&mut sec_idx);
                msg.append(&mut timestamp.to_be_bytes().to_vec());
                msg.append(&mut [0; 7].to_vec()); // padding
                msg.push(write_rank);
                msg.append(&mut data);
            }
            SystemRegisterCommandContent::Ack => {
                msg.push(ACK_MSG_T);
                msg.append(&mut uuid);
                msg.append(&mut rid);
                msg.append(&mut sec_idx);
            }
        };
        //log::debug!("msg {:?}", msg);
    }

    pub async fn deserialize_client_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        msg_type: u8,
        msg: &mut Vec<u8>,
    ) -> Result<Option<RegisterCommand>, Error> {
        let mut err = false;
        let mut buffer: [u8; 16] = [0; 16];
        if let Err(e) = data.read_exact(&mut buffer).await {
            log::error!("deserialize_system_command read_exact buffer error {:?}", e);
            return Err(e);
        }
        msg.append(&mut buffer.to_vec());
        //let req_no: u64 = u64::from_be_bytes(buffer[..8].try_into().unwrap());
        //let sec_idx: u64 = u64::from_be_bytes(buffer[8..].try_into().unwrap());
        let req_no = match buffer[..8].try_into() {
            Ok(bytes) => u64::from_be_bytes(bytes),
            Err(e) => {
                err = true;
                log::error!("Cannot get request number {:?}", e);
                Default::default()
            }
        };
        let sec_idx = match buffer[8..].try_into() {
            Ok(bytes) => u64::from_be_bytes(bytes),
            Err(e) => {
                err = true;
                log::error!("Cannot get sector idx {:?}", e);
                Default::default()
            }
        };

        let header = ClientCommandHeader {
            request_identifier: req_no,
            sector_idx: sec_idx,
        };
        let content = match msg_type {
            0x01 => ClientRegisterCommandContent::Read,
            0x02 => {
                let mut content_buffer: [u8; SECTOR_SIZE] = [0; SECTOR_SIZE];
                if let Err(e) = data.read_exact(&mut content_buffer).await {
                    log::error!("deserialize_system_command read_exact content_buffer error {:?}", e);
                    return Err(e);
                }
                msg.append(&mut content_buffer.to_vec());
                ClientRegisterCommandContent::Write {
                    data: SectorVec(Vec::from(content_buffer)),
                }
            }
            t => {
                log::error!("Bug in deserialize_client_command. unknown msg_type {} should already be handled in deserialize_register_command", t);
                return Ok(None);
            }
        };
        if err {
            Ok(None) // consumed appropriate number of bytes
        } else {
            Ok(Some(RegisterCommand::Client(ClientRegisterCommand { header, content })))
        }

    }

    pub async fn deserialize_system_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        rank: u8,
        msg_type: u8,
        msg: &mut Vec<u8>,
    ) -> Result<Option<RegisterCommand>, Error> {
        let mut buffer: [u8; 32] = [0; 32];
        data.read_exact(&mut buffer).await.unwrap();
        log::info!("BOTTOM read buffer uuid,rid,sec_idx {:?}", &buffer);
        msg.append(&mut buffer.to_vec());
        let mut err = false;

        //let uuid = Uuid::from_bytes(buffer[..16].try_into());
        let uuid = match buffer[..16].try_into() {
            Ok(bytes) => Uuid::from_bytes(bytes),
            Err(e) => {
                err = true;
                log::error!("Cannot get uuid {:?}", e);
                Default::default()
            }
        };
        //let rid_rop = u64::from_be_bytes(buffer[16..24].try_into().unwrap());
        let rid = match buffer[16..24].try_into() {
            Ok(bytes) => u64::from_be_bytes(bytes),
            Err(e) => {
                err = true;
                log::error!("Cannot get read identifier {:?}", e);
                Default::default()
            }
        };
        //let sec_idx = u64::from_be_bytes(buffer[24..].try_into().unwrap());
        let sec_idx = match buffer[24..].try_into() {
            Ok(bytes) => u64::from_be_bytes(bytes),
            Err(e) => {
                err = true;
                log::error!("Cannot get sector index {:?}", e);
                Default::default()
            }
        };
        let header = SystemCommandHeader {
            process_identifier: rank,
            msg_ident: uuid,
            read_ident: rid,
            sector_idx: sec_idx,
        };
        let content = match msg_type {
            0x03 => SystemRegisterCommandContent::ReadProc,
            0x04 => {
                let mut buffer: [u8; 16 + SECTOR_SIZE] = [0; 16 + SECTOR_SIZE];
                if let Err(e) = data.read_exact(&mut buffer).await {
                    log::error!("deserialize_system_command read_exact buffer error {:?}", e);
                    return Err(e);
                }
                log::info!("\nDESER V \n[..20] {:?} \n[-16] {:?}", &buffer[..20], &buffer[SECTOR_SIZE..]);
                msg.append(&mut buffer.to_vec());
                //let timestamp = u64::from_be_bytes(buffer[..8].try_into().unwrap());
                let timestamp = match buffer[..8].try_into() {
                    Ok(bytes) => u64::from_be_bytes(bytes),
                    Err(e) => {
                        err = true;
                        log::error!("Cannot get timestamp {:?}", e);
                        Default::default()
                    }
                };
                // padding = buffer[8..15]
                let write_rank = buffer[15];
                let sec_data = SectorVec(buffer[16..].to_vec());
                //let SectorVec(d) = sec_data.clone();
                //log::info!("DESER V {:?}", &d[SECTOR_SIZE-10..]);
                SystemRegisterCommandContent::Value {
                    timestamp,
                    write_rank,
                    sector_data: sec_data,
                }
            }
            0x05 => {
                let mut buffer: [u8; 16 + SECTOR_SIZE] = [0; 16 + SECTOR_SIZE];
                if let Err(e) = data.read_exact(&mut buffer).await {
                    log::error!("deserialize_system_command read_exact buffer error {:?}", e);
                    return Err(e);
                }
                msg.append(&mut buffer.to_vec());
                //let timestamp = u64::from_be_bytes(buffer[..8].try_into().unwrap());
                let timestamp = match buffer[..8].try_into() {
                    Ok(bytes) => u64::from_be_bytes(bytes),
                    Err(e) => {
                        err = true;
                        log::error!("Cannot get timestamp {:?}", e);
                        Default::default()
                    }
                };
                // padding = buffer[8..15]
                let write_rank = buffer[15];
                let sec_data = SectorVec(buffer[16..].to_vec());
                let SectorVec(d) = sec_data.clone();
                log::info!("DESER WP {:?}", &d[SECTOR_SIZE-10..]);
                SystemRegisterCommandContent::WriteProc {
                    timestamp,
                    write_rank,
                    data_to_write: sec_data,
                }
            }
            0x06 => SystemRegisterCommandContent::Ack,
            _ => {
                log::error!("BUG in deserialize_client_command. unknown msg_type should already be handled in deserialize_register_command");
                return Ok(None);
            }
        };
        if err {
            Ok(None) // consumed appropriate number of bytes
        } else {
            Ok(Some(System(SystemRegisterCommand { header, content })))
        }
    }
}