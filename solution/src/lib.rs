pub use crate::domain::*;
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
mod domain;

mod atomic_register;
mod register_client;
mod sectors_manager;
mod transfer;
mod utils;
use crate::RegisterCommand::{Client, System};
use crate::SystemRegisterCommandContent::{Ack, Value};
use async_channel::{unbounded, Receiver, Sender};
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use std::sync::{Arc, RwLock};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
pub use transfer_public::*;
use uuid::Uuid;

const REGISTER_COUNT: u8 = u8::MAX;

pub async fn run_register_process(config: Configuration) {
    let ident: u8 = config.public.self_rank;
    let processes_count = config.public.tcp_locations.len();
    let (host, port) = &config.public.tcp_locations[(ident as usize) - 1];
    let tcp_listener: TcpListener = TcpListener::bind((host.as_str(), *port)).await.unwrap();

    let (loopback_sender, loopback_receiver) = unbounded();
    let (op_comp_sender, op_comp_receiver) = unbounded();

    let broadcast_pool = Arc::new(RwLock::new(HashMap::new()));
    let rc = build_register_client(
        ident,
        config.public.tcp_locations.clone(),
        loopback_sender,
        config.hmac_system_key,
        broadcast_pool.clone(),
    )
    .await;
    let sm = build_sectors_manager(config.public.storage_dir.clone());

    let mut cmd_senders = vec![];

    for i in 0..REGISTER_COUNT {
        let path = config
            .public
            .storage_dir
            .clone()
            .join(format!("worker_{}", i));
        let ss = stable_storage_public::build_stable_storage(path).await;
        let mut ar =
            build_atomic_register(i + 1, ss, rc.clone(), sm.clone(), processes_count).await;
        let (cmd_sender, cmd_receiver): (Sender<RegisterCommand>, Receiver<RegisterCommand>) =
            unbounded();

        cmd_senders.push(cmd_sender);

        let op_comp_sender_clone = op_comp_sender.clone();
        tokio::spawn(worker_loop(op_comp_sender_clone, cmd_receiver, ar, i));
    }

    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, _)) => {
                let (mut read_stream, write_stream) = tcp_stream.into_split();
                tokio::spawn(send_response(op_comp_receiver.clone(), write_stream));
                // receive self cmd
                tokio::spawn(read_from_loopback(
                    loopback_receiver.clone(),
                    cmd_senders.clone(),
                    broadcast_pool.clone(),
                    processes_count as u8,
                ));
                // receive cmd from client or another process
                tokio::spawn(read_from_tcp(read_stream,
                                           config.hmac_system_key.clone(),config.hmac_client_key.clone(),
                                           cmd_senders.clone(),broadcast_pool.clone(),
                processes_count as u8));
            }
            Err(e) => {
                log::error!("Error while accepting connection on TcpListener: {:?}", e);
            }
        }
    }
}

async fn read_from_tcp(mut read_stream: OwnedReadHalf, system_key:  [u8; 64], client_key: [u8; 32],
    cmd_senders: Vec<Sender<RegisterCommand>>, broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
                       processes_count: u8,)  {
    loop {
        match deserialize_register_command(
            &mut read_stream,
            &system_key,
            &client_key,
        )
        .await
        {
            Ok((cmd, correct)) => {
                if correct {
                    delegate_cmd(
                        &cmd_senders,
                        cmd,
                        broadcast_pool.clone(),
                        processes_count as u8,
                    )
                    .await;
                } else {
                    todo!(); // TODO handle msg with incorrect type
                }
            }
            Err(e) => {
                log::error!("reader worker, deserialize error: {:?}", e);
            }
        }
    }
}
async fn read_from_loopback(
    loopback_receiver: Receiver<SystemRegisterCommand>,
    cmd_senders: Vec<Sender<RegisterCommand>>,
    broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    processes_count: u8,
) {
    loop {
        match loopback_receiver.recv().await {
            Ok(cmd) => {
                delegate_cmd(
                    &cmd_senders,
                    RegisterCommand::System(cmd),
                    broadcast_pool.clone(),
                    processes_count,
                )
                .await;
            }
            Err(e) => {
                log::error!("writer worker, recvError: {:?}", e);
            }
        }
    }
}

pub async fn delegate_cmd(
    cmd_senders: &Vec<Sender<RegisterCommand>>,
    cmd: RegisterCommand,
    broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    processes_count: u8,
) {
    let mut sec_idx = 1;
    // adjust broadcasting_pool
    match cmd.clone() {
        Client(ClientRegisterCommand { header, content }) => {
            let mut map = broadcast_pool.write().unwrap();
            //HashSet::from([1..processes_count]),
            let set = (1..processes_count).collect();
            map.insert(Uuid::from_u128(header.request_identifier as u128), set);
            sec_idx = header.sector_idx;
        }
        System(SystemRegisterCommand { header, content }) => {
            match content {
                (SystemRegisterCommandContent::Value { .. })
                | SystemRegisterCommandContent::Ack => {
                    let mut map = broadcast_pool.write().unwrap();
                    let set: &mut HashSet<u8> = map.get_mut(&header.msg_ident).unwrap(); // TODO handle instead of unwrap
                    set.remove(&header.process_identifier);
                    if set.is_empty() {
                        (*map).remove(&header.msg_ident).unwrap();
                    }
                }
                _ => {}
            }
            sec_idx = header.sector_idx;
        }
    };
    let sender = &cmd_senders[(sec_idx % REGISTER_COUNT as u64) as usize];
    sender.send(cmd);
}

async fn worker_loop(
    op_comp_sender: Sender<OperationComplete>,
    cmd_receiver: Receiver<RegisterCommand>,
    mut ar: Box<dyn AtomicRegister>,
    i: u8,
) {
    loop {
        match cmd_receiver.recv().await {
            Ok(System(src)) => {
                ar.as_mut().system_command(src);
            }
            Ok(Client(crc)) => {
                let op_comp_sender_clone = op_comp_sender.clone();
                let callback: Box<
                    dyn FnOnce(
                            OperationComplete,
                        ) -> core::pin::Pin<
                            Box<dyn core::future::Future<Output = ()> + core::marker::Send>,
                        > + core::marker::Send
                        + Sync,
                > = Box::new(move |op_c| {
                    tokio::spawn(async move {
                        op_comp_sender_clone.send(op_c).await;
                    });
                    todo!(); // ^ this should be enough but types dont match
                });
                ar.as_mut().client_command(crc, callback);
            }
            Err(e) => {
                log::error!("worker: {:?}, recvError: {:?}", i + 1, e);
            }
        }
    }
}

async fn send_response(
    op_comp_receiver: Receiver<OperationComplete>,
    write_stream: OwnedWriteHalf,
) {
    match op_comp_receiver.recv().await {
        Ok(op_complete) => {
            let mut data: &[u8] = todo!(); // TODO write response on write_stream
            write_stream.write_all(data);
            todo!();
        }
        Err(e) => {
            log::error!("response worker, recvError: {:?}", e);
        }
    }
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
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,
    ) -> Box<dyn AtomicRegister> {
        Nnar::new(
            self_ident,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        )
        .await
    }
    // TODO sprawdzić zgonośc sygnatur z oryginałem
}

pub mod sectors_manager_public {

    use crate::sectors_manager::sectors_manager::MySectorsManager;
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

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
    use crate::transfer::transfer::{
        deserialize_client_command, deserialize_system_command, serialize_client_command,
        serialize_system_command,
    };

    use crate::utils::utils;
    use crate::ClientRegisterCommandContent::Write;
    use crate::RegisterCommand::{Client, System};
    use crate::{
        RegisterCommand, ACK_MSG_T, HMAC_TAG_SIZE, MAGIC_NUMBER, READ_MSG_T, READ_PROC_MSG_T,
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
    use crate::register_client::register_client::Stubborn;
    use crate::SystemRegisterCommand;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, RwLock};
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::sync::Mutex;
    use uuid::Uuid;

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

    pub async fn build_register_client(
        ident: u8,
        tcp_locations: Vec<(String, u16)>,
        sender: async_channel::Sender<SystemRegisterCommand>,
        hmac_system_key: [u8; 64],
        broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    ) -> Arc<dyn RegisterClient> {
        Arc::new(Stubborn::new(
            ident,
            tcp_locations,
            sender,
            hmac_system_key,
            broadcast_pool,
        ))
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
