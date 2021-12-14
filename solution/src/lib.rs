use tokio::io::AsyncReadExt;
pub use crate::domain::*;
mod domain;

mod utils;
mod atomic_register;
mod register_client;
mod sectors_manager;
mod transfer;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use tokio::net::TcpListener;
pub use transfer_public::*;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

const REGISTER_COUNT: u32 = 512;


pub async fn run_register_process(config: Configuration) {
    println!("{} {}", usize::MAX, u8::MAX);
    let ident: u8 = config.public.self_rank;
    let (host, port) = &config.public.tcp_locations[(ident as usize) - 1];
    let tcp_listener: TcpListener = TcpListener::bind((host.as_str(), *port)).await.unwrap();

    let (tx, rx) = unbounded_channel();
    let rc = build_register_client(
      ident, config.public.tcp_locations.clone(), tx, config.hmac_system_key).await;
    let sm = build_sectors_manager(config.public.storage_dir.clone());

    let txs = vec![];
    let workers = vec![];
    for i in 0..REGISTER_COUNT {
        let path = config.public.storage_dir.clone().join(format!("worker_{}", i));
        let ss = stable_storage_public::build_stable_storage(path).await;
        let ar = build_atomic_register(
            i+1,
            ident,
            ss,
            rc.clone(),
            sm.clone(),
            config.public.tcp_locations.len()
        ).await;
        let (tx, rx): (UnboundedSender<RegisterCommand>, UnboundedReceiver<RegisterCommand>) = unbounded_channel();
        txs.push(tx);
        // worker reads from channel and invokes system_cmd/client_cmd of NNAR he owns
        let worker = tokio::spawn(async move {
            loop {
                let cmd = rx.recv().await;
                match cmd {
                    Some(SystemRegisterCommand { header, content }) => {
                        ar.system_command(SystemRegisterCommand { header, content });
                    }
                    Some(ClientRegisterCommand { header, content }) => {
                        ar.client_command(ClientRegisterCommand { header, content });
                    }
                }
            }
        });
        workers.push(worker);
    }
    // construct READER worker and WRITER worker

    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, _)) => {
                let (mut read_stream, mut write_stream) = tcp_stream.into_split();
                loop {
                    let cmd = get_cmd(
                        &mut read_stream,
                        &mut write_stream, // todo maybe Arc
                        config.hmac_system_key.clone(),
                        config.hmac_client_key.clone(),
                        config.public.max_sector,
                    )
                    .await;
                    let _res = handle_cmd(cmd).await;
                }
            }
            Err(e) => {
                log::error!("Error while accepting connection on TcpListener: {:?}", e);
            }
        }
    }
}

pub async fn get_cmd(read_stream: &mut OwnedReadHalf, write_stream: &mut OwnedWriteHalf,
                     hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], max_sector: u64) -> RegisterCommand {
    let mut _buff: [u8; 1] = [0];
    match read_stream.peek(&mut _buff).await {
        Err(e) => {
            log::error!("Error while peeking OwnedReadHalf of Tcp stream: {:?}", e);
            panic!();
        },
        Ok(0) => {todo!()}, // no bytes to read     // if in loop add break;
        Ok(_) => {
            match deserialize_register_command( read_stream,
                                                &hmac_system_key, &hmac_client_key).await {
                Ok((cmd, bool)) => { return cmd; }
                Err(_) => { todo!(); }
            }
        }
    }
}

pub async fn handle_cmd(cmd: RegisterCommand) {

}

pub mod atomic_register_public {
    use crate::atomic_register::atomic_register::Nnar;
    use crate::{
        ClientRegisterCommand, OperationComplete, RegisterClient, SectorsManager, StableStorage,
        SystemRegisterCommand,
    };

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Send client command to the register. After it is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            operation_complete: Box<
                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Send system command to the register.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Storage for atomic register algorithm data is separated into StableStorage.
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    pub async fn build_atomic_register(
        self_identifier: u32,
        process_identifier: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,
    ) -> Box<dyn AtomicRegister> {
        Nnar::new(
            self_identifier,
            process_identifier,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        )
        .await
    }
}

pub mod sectors_manager_public {

    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;
    use crate::sectors_manager::sectors_manager::MySectorsManager;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        MySectorsManager::new(path)
    }
}

pub mod transfer_public {
    use crate::transfer::transfer::{serialize_client_command, serialize_system_command,
                                    deserialize_client_command, deserialize_system_command};

    use crate::utils::utils;
    use crate::ClientRegisterCommandContent::Write;
    use crate::RegisterCommand::{Client, System};
    use crate::{
        RegisterCommand,

        ACK_MSG_T, HMAC_TAG_SIZE, MAGIC_NUMBER, READ_MSG_T, READ_PROC_MSG_T,
        VALUE_MSG_T, WRITE_PROC_MSG_T,
    };
    use std::io::{Error, ErrorKind};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        let mut msg: Vec<u8> = Vec::with_capacity(3 * 8 + HMAC_TAG_SIZE);
        msg.append(&mut MAGIC_NUMBER.to_vec());
        match cmd {
            Client(crc) => serialize_client_command(crc, writer, &mut msg).await,
            System(src) => serialize_system_command(src, writer, &mut msg).await,
        };
        let tag = utils::calculate_hmac_tag(&msg, hmac_key);
        msg.append(&mut tag.to_vec());
        writer.write_all(&msg).await.unwrap();
        log::debug!("serialized msg length {:?}", msg.len());
        Ok(())
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        log::debug!("deserializing register command");

        // slide over bytes in the stream until it detects a valid magic number
        let mut magic_buffer: [u8; 4] = [0; 4];
        data.read_exact(&mut magic_buffer).await.unwrap();
        while magic_buffer != MAGIC_NUMBER {
            magic_buffer[0] = magic_buffer[1];
            magic_buffer[1] = magic_buffer[2];
            magic_buffer[2] = magic_buffer[3];
            data.read_exact(&mut magic_buffer[3..4]).await.unwrap();
        }

        let mut msg: Vec<u8> = MAGIC_NUMBER.to_vec();
        let mut buffer: [u8; 4] = [0; 4];
        data.read_exact(&mut buffer).await.unwrap();
        msg.append(&mut buffer.to_vec());
        let msg_type = buffer[3];
        let (rank, msg_type, key): (Option<u8>, u8, &[u8]) = match msg_type {
            0x01 => (None, READ_MSG_T, hmac_client_key),
            0x02 => (None, WRITE_PROC_MSG_T, hmac_client_key),
            0x03 => (Some(buffer[2]), READ_PROC_MSG_T, hmac_system_key),
            0x04 => (Some(buffer[2]), VALUE_MSG_T, hmac_system_key),
            0x05 => (Some(buffer[2]), WRITE_PROC_MSG_T, hmac_system_key),
            0x06 => (Some(buffer[2]), ACK_MSG_T, hmac_system_key),
            t => {
                // TODO / If a message type is invalid, the solution shall discard the magic number
                // TODO / and the following 4 bytes (8 bytes in total).
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("msg_type not handled: {}", t),
                ));
            }
        };
        // TODO / In case of every other error, the solution shall consume the same number of bytes
        // TODO / as if a message of this type was processed successfully.
        let cmd = if let Some(rank) = rank {
            deserialize_system_command(data, rank, msg_type, hmac_system_key, &mut msg).await
        } else {
            deserialize_client_command(data, msg_type, hmac_client_key, &mut msg).await
        };
        let mut tag_buffer: [u8; HMAC_TAG_SIZE] = [0; HMAC_TAG_SIZE];
        data.read_exact(&mut tag_buffer).await.unwrap();
        let verified = utils::verify_hmac_tag(&tag_buffer, &msg, key);
        Ok((cmd, verified))
    }


}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;
    use crate::register_client::register_client::Sbeb;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::sync::Mutex;
    use uuid::Uuid;
    use std::collections::HashMap;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: usize,
    }

    pub async fn build_register_client(ident: u8,
                                       tcp_locations: Vec<(String, u16)>,
                                       sender: UnboundedSender<SystemRegisterCommand>,
                                       hmac_system_key: [u8; 64],) -> Arc<dyn RegisterClient> {
        Arc::new(Sbeb::new(ident, tcp_locations, sender, hmac_system_key))
    }
}

pub mod stable_storage_public {
    use sha2::{Digest, Sha256};
    use std::path::{Path, PathBuf};
    use std::str;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[async_trait::async_trait]
    /// A helper trait for small amount of durable metadata needed by the register algorithm
    /// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
    /// is durable, as one could expect.
    pub trait StableStorage: Send + Sync {
        async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

        async fn get(&self, key: &str) -> Option<Vec<u8>>;
    }

    struct Storage {
        dir: PathBuf,
        key_max_len: usize,
        value_max_len: usize,
    }
    #[async_trait::async_trait]
    impl StableStorage for Storage {
        async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
            if key.len() > self.key_max_len {
                return Err(String::from("Key too long (>255)."));
            }
            if value.len() > self.value_max_len {
                return Err(String::from("Value too long (>65535)."));
            }

            let hashed_key = Storage::hash(key);
            let mut tmp_name = String::from("tmp_");
            tmp_name.push_str(&hashed_key);

            let mut tmp_path = self.dir.clone();
            Storage::append_path(&mut tmp_path, &tmp_name);
            let mut path = self.dir.clone();
            Storage::append_path(&mut path, &hashed_key);

            /* Write data to a temporary file dstdir/tmpfile. */
            let mut f = File::create(tmp_path.clone()).await.unwrap();
            f.write_all(value).await.unwrap();
            /* Call the POSIX fsyncdata method on dstdir/tmpfile to ensure
            the data is actually transferred to a disk device */
            f.sync_data().await.unwrap();
            /* Rename dstdir/tmpfile to dstdir/dstfile. */
            tokio::fs::rename(tmp_path, path).await.unwrap();
            /* Call the POSIX fsyncdata method on dstdir to transfer the data
            of the modified directory to the disk device */
            f.sync_data().await.unwrap();
            Ok(())
        }

        async fn get(&self, key: &str) -> Option<Vec<u8>> {
            let hashed_key = Storage::hash(key);
            let mut path = self.dir.clone();
            Storage::append_path(&mut path, &hashed_key);

            match tokio::fs::read(path).await {
                Ok(value) => Some(value),
                Err(_) => None,
            }
        }
    }

    impl Storage {
        fn hash(key: &str) -> String {
            let mut hasher = Sha256::new();
            hasher.update(key);
            format!("{:X}", hasher.finalize())
        }

        fn append_path(path: &mut PathBuf, suffix: impl AsRef<Path>) {
            path.push(suffix);
        }
    }

    /// Creates a new instance of stable storage.
    pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
        Box::new(Storage {
            dir: root_storage_dir,
            key_max_len: 255,
            value_max_len: 65535,
        })
    }
}
