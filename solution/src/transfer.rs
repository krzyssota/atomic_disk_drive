pub mod transfer {
    use crate::RegisterCommand::{System};
    use crate::{
        ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand,
        SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
        ACK_MSG_T, READ_MSG_T, READ_PROC_MSG_T, SECTOR_SIZE,
        VALUE_MSG_T, WRITE_MSG_T, WRITE_PROC_MSG_T,
    };
    use std::convert::TryInto;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
    use uuid::Uuid;


    pub async fn serialize_client_command(
        cmd: &ClientRegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        msg: &mut Vec<u8>,
    ) {
        log::debug!("serializing client command");
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
        log::debug!("msg {:?}", msg);
    }

    pub async fn serialize_system_command(
        cmd: &SystemRegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        msg: &mut Vec<u8>,
    ) {
        log::debug!("serializing system command");
        msg.append(&mut [0_u8; 2].to_vec()); // padding
        let mut rank = cmd.header.process_identifier.to_be_bytes().to_vec();
        msg.append(&mut rank);
        let mut uuid = cmd.header.msg_ident.as_bytes().to_vec();
        let mut rid = cmd.header.read_ident.to_be_bytes().to_vec();
        let mut sec_idx = cmd.header.sector_idx.to_be_bytes().to_vec();
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
                msg.append(&mut [0, 7].to_vec()); // padding
                msg.push(write_rank);
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
        log::debug!("msg {:?}", msg);
    }

    pub async fn deserialize_client_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        msg_type: u8,
        hmac_key: &[u8; 32],
        msg: &mut Vec<u8>,
    ) -> RegisterCommand {
        let mut buffer: [u8; 16] = [0; 16];
        data.read_exact(&mut buffer).await.unwrap();
        msg.append(&mut buffer.to_vec());
        let req_no: u64 = u64::from_be_bytes(buffer[..8].try_into().unwrap());
        let sec_idx: u64 = u64::from_be_bytes(buffer[8..].try_into().unwrap());

        let header = ClientCommandHeader {
            request_identifier: req_no,
            sector_idx: sec_idx,
        };
        let content = match msg_type {
            0x01 => ClientRegisterCommandContent::Read,
            0x02 => {
                let mut content_buffer: [u8; SECTOR_SIZE] = [0; SECTOR_SIZE];
                data.read_exact(&mut content_buffer).await.unwrap();
                msg.append(&mut content_buffer.to_vec());
                ClientRegisterCommandContent::Write {
                    data: SectorVec(Vec::from(content_buffer)),
                }
            }
            _ => {
                log::error!("Bug in deserialize_client_command. unknown msg_type should already be handled in deserialize_register_command");
                panic!();
            }
        };
        RegisterCommand::Client(ClientRegisterCommand { header, content })
    }

    pub async fn deserialize_system_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        rank: u8,
        msg_type: u8,
        hmac_system_key: &[u8; 64],
        msg: &mut Vec<u8>,
    ) -> RegisterCommand {
        let mut buffer: [u8; 32] = [0; 32];
        data.read_exact(&mut buffer).await.unwrap();
        msg.append(&mut buffer.to_vec());
        let uuid = Uuid::from_bytes(buffer[..16].try_into().unwrap());
        let rid_rop = u64::from_be_bytes(buffer[16..24].try_into().unwrap());
        let sec_idx = u64::from_be_bytes(buffer[24..].try_into().unwrap());
        let header = SystemCommandHeader {
            process_identifier: rank,
            msg_ident: uuid,
            read_ident: rid_rop,
            sector_idx: sec_idx,
        };
        let content = match msg_type {
            0x03 => SystemRegisterCommandContent::ReadProc,
            0x04 => {
                let mut buffer: [u8; 16 + SECTOR_SIZE] = [0; 16 + SECTOR_SIZE];
                data.read_exact(&mut buffer).await.unwrap();
                msg.append(&mut buffer.to_vec());
                let timestamp = u64::from_be_bytes(buffer[..8].try_into().unwrap());
                // padding = buffer[8..15]
                let write_rank = buffer[15];
                let sec_data = SectorVec(buffer[16..].to_vec());
                SystemRegisterCommandContent::Value {
                    timestamp,
                    write_rank,
                    sector_data: sec_data,
                }
            }
            0x05 => {
                let mut buffer: [u8; 16 + SECTOR_SIZE] = [0; 16 + SECTOR_SIZE];
                data.read_exact(&mut buffer).await.unwrap();
                msg.append(&mut buffer.to_vec());
                let timestamp = u64::from_be_bytes(buffer[..8].try_into().unwrap());
                // padding = buffer[8..15]
                let write_rank = buffer[15];
                let sec_data = SectorVec(buffer[16..].to_vec());
                SystemRegisterCommandContent::WriteProc {
                    timestamp,
                    write_rank,
                    data_to_write: sec_data,
                }
            }
            0x06 => SystemRegisterCommandContent::Ack,
            _ => {
                log::error!("Bug in deserialize_client_command. unknown msg_type should already be handled in deserialize_register_command");
                panic!();
            }
        };
        System(SystemRegisterCommand { header, content })
    }
}