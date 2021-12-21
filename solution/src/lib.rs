pub use crate::domain::*;
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncWriteExt;
mod domain;

mod atomic_register;
mod register_client;
mod sectors_manager;
mod transfer;
mod utils;
use crate::RegisterCommand::{Client, System};
use async_channel::{unbounded, Receiver, Sender};
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, Mutex, RwLock};
pub use transfer::transfer::serialize_client_response;
pub use transfer_public::*;
use uuid::Uuid;

//const REGISTER_COUNT: u8 = u8::MAX;
const REGISTER_COUNT: u8 = 64;

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
        if let Err(e) = tokio::fs::create_dir_all(&path).await {
            log::error!("Could not create stable storage directory for worker {}", i);
            panic!();
        }
        let ss = stable_storage_public::build_stable_storage(path).await;
        let ar = build_atomic_register(config.public.self_rank, ss, rc.clone(), sm.clone(), processes_count).await;
        let (src_sender, src_receiver) = unbounded();
        let (crc_sender, crc_receiver) = unbounded();

        cmd_senders.push((src_sender, crc_sender));

        let op_comp_sender_clone = op_comp_sender.clone();
        tokio::spawn(worker_loop(
            op_comp_sender_clone,
            src_receiver,
            crc_receiver,
            ar,
            i,
        ));
    }

    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, _)) => {
                log::debug!("got connection {:?}", tcp_stream);
                let (read_stream, write_stream) = tcp_stream.into_split();
                let writer = Arc::new(Mutex::new(write_stream));
                let key = config.hmac_client_key.clone();
                tokio::spawn(recv_and_send_response_to_client(
                    op_comp_receiver.clone(),
                    writer.clone(),
                    key,
                ));
                // receive self cmd
                tokio::spawn(read_from_loopback(
                    loopback_receiver.clone(),
                    cmd_senders.clone(),
                    broadcast_pool.clone(),
                    processes_count as u8,
                ));
                // receive cmd from client or another process
                tokio::spawn(read_from_tcp(
                    read_stream,
                    writer.clone(),
                    config.hmac_system_key.clone(),
                    config.hmac_client_key.clone(),
                    config.public.max_sector.clone(),
                    cmd_senders.clone(),
                    op_comp_sender.clone(),
                    broadcast_pool.clone(),
                    processes_count as u8,
                ));
            }
            Err(e) => {
                log::error!("Error while accepting connection on TcpListener: {:?}", e);
            }
        }
    }
}

async fn read_from_tcp(
    mut read_stream: OwnedReadHalf,
    write_stream: Arc<Mutex<OwnedWriteHalf>>,
    system_key: [u8; 64],
    client_key: [u8; 32],
    max_sector: u64,
    cmd_senders: Vec<(Sender<SystemRegisterCommand>, Sender<ClientRegisterCommand>)>,
    op_comp_sender: Sender<OperationComplete>,
    broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    processes_count: u8,
) {
    log::debug!("reading from tcp");
    loop {
        match deserialize_register_command(&mut read_stream, &system_key, &client_key).await {
            Ok((cmd, correct)) => {
                log::debug!("received cmd {:?} {}", cmd, correct);
                if let Client(ClientRegisterCommand { header, content }) = cmd.clone() {
                    if header.sector_idx < max_sector && correct {
                        delegate_cmd(
                            &cmd_senders,
                            cmd,
                            broadcast_pool.clone(),
                            processes_count as u8,
                        )
                            .await;
                        continue;
                    } else {
                        let mut status_code = StatusCode::AuthFailure;
                        if header.sector_idx >= max_sector {
                            status_code = StatusCode::InvalidSectorIndex
                        }
                        log::debug!("bad crc {:?} responding with {:?}", cmd, status_code);
                        let op_return = match content {
                            ClientRegisterCommandContent::Read => OperationReturn::Read(ReadReturn { read_data: None }),
                            ClientRegisterCommandContent::Write { .. } => OperationReturn::Write,
                        };
                        let op_complete = OperationComplete {
                            status_code,
                            request_identifier: header.request_identifier,
                            op_return
                        };
                        let mut data = vec![];
                        serialize_client_response(op_complete, &mut data, client_key).await;
                        let mut write = write_stream.lock().await;
                        if let Err(e) = write.write_all(&data).await {
                            log::error!("cannot write InvalidSectorIndex response to stream {:?}", e);
                        }
                    }
                } else {
                    delegate_cmd(
                        &cmd_senders,
                        cmd,
                        broadcast_pool.clone(),
                        processes_count as u8,
                    )
                    .await;
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
    cmd_senders: Vec<(Sender<SystemRegisterCommand>, Sender<ClientRegisterCommand>)>,
    broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    processes_count: u8,
) {
    log::debug!("reading from loopback");
    loop {
        log::debug!("trying to recv loopback");
        match loopback_receiver.recv().await {
            Ok(cmd) => {
                log::debug!("Received from loopback {:?}", cmd);
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
    cmd_senders: &Vec<(Sender<SystemRegisterCommand>, Sender<ClientRegisterCommand>)>,
    cmd: RegisterCommand,
    broadcast_pool: Arc<RwLock<HashMap<Uuid, HashSet<u8>>>>,
    processes_count: u8,
) {
    log::debug!("delegating cmd {:?}", cmd);
    let sec_idx;
    // adjust broadcasting_pool
    match cmd.clone() {
        Client(ClientRegisterCommand { header, content }) => {
            let mut map = broadcast_pool.write().await;
            let set = (1..processes_count+1).collect();
            log::debug!("proc_count {} set {:?}", processes_count,set);
            map.insert(Uuid::from_u128(header.request_identifier as u128), set);
            log::debug!("it was crc {:?} map {:?}", header, map);
            sec_idx = header.sector_idx;
            let (_, client_sender) = &cmd_senders[(sec_idx % REGISTER_COUNT as u64) as usize];
            if let Err(e) = client_sender
                .send(ClientRegisterCommand { header, content })
                .await
            {
                log::error!(
                    "cannot delegate crc (sector: {}) to worker: {:?}",
                    sec_idx,
                    e
                );
            }
        }
        System(SystemRegisterCommand { header, content }) => {
            match content {
               /* SystemRegisterCommandContent::WriteProc {..}/* | SystemRegisterCommandContent::ReadProc*/ => {
                    let mut map = broadcast_pool.write().await;
                    log::debug!("delegating writeproc {:?} map {:?}", header, map);
                    let set = (1..processes_count+1).collect();
                    log::debug!("proc_count {} set {:?} msg_ident {}", processes_count, set, header.msg_ident);
                    map.insert(header.msg_ident, set);
                    log::debug!("it was writeproc {:?} map {:?}", header, map);
                },*/
                /*SystemRegisterCommandContent::Value { .. }*/ | SystemRegisterCommandContent::Ack => {
                    let mut map = broadcast_pool.write().await;
                    log::debug!("delegating value/ack  curr map {:?}", map);
                    if let Some(set) = map.get_mut(&header.msg_ident) {
                        log::debug!("delegating value/ack 2 curr set {:?} removing proc_ident {:?} from set", set, header.process_identifier);
                        set.remove(&header.process_identifier);
                        // TODO clear whole set if it is smaller than N/2 !!!!!
                    } else {
                        log::debug!("delegating value/ack 2 no set removing msg_ident {:?} from map", header.msg_ident);
                        let res = (*map).remove(&header.msg_ident);
                    }
                    log::debug!("it was Value|Ack {:?} with msg_ident {:?} map {:?}", cmd, header.msg_ident, map);
                }
                _ => {}
            }
            sec_idx = header.sector_idx;
            let (system_sender, _) = &cmd_senders[(sec_idx % REGISTER_COUNT as u64) as usize];
            if let Err(e) = system_sender
                .send(SystemRegisterCommand { header, content })
                .await
            {
                log::error!(
                    "cannot delegate src (sector: {}) to worker: {:?}",
                    sec_idx,
                    e
                );
            }
        }
    };
}
async fn worker_loop(
    op_comp_sender: Sender<OperationComplete>,
    src_receiver: Receiver<SystemRegisterCommand>,
    crc_receiver: Receiver<ClientRegisterCommand>,
    mut ar: Box<dyn AtomicRegister>,
    i: u8,
) {
    let accepting_client_command = Arc::new(AtomicBool::new(true));
    loop {
        if accepting_client_command.load(Ordering::Relaxed) {
            match crc_receiver.recv().await {
                Err(e) => log::error!("worker: {} cannot recv crc {:?}", i, e),
                Ok(crc) => {
                    log::debug!("Worker {} got crc {:?}", i, crc);
                    accepting_client_command.store(false, Ordering::Relaxed);
                    let op_comp_sender_clone = op_comp_sender.clone();
                    let accepting = accepting_client_command.clone();
                    let callback: Box<
                        dyn FnOnce(
                                OperationComplete,
                            ) -> core::pin::Pin<
                                Box<dyn core::future::Future<Output = ()> + core::marker::Send>,
                            > + core::marker::Send
                            + Sync,
                    > = Box::new(move |op_comp| {
                        Box::pin(async move {
                            log::debug!("Callback for {:?} is called", op_comp);
                            if let Err(e) = op_comp_sender_clone.send(op_comp).await {
                                log::error!(
                                    "worker: {} cannot send op_complete in callback {:?}",
                                    i,
                                    e
                                );
                            }
                            accepting.store(true, Ordering::Relaxed);
                        })
                    });
                    ar.as_mut().client_command(crc, callback).await;
                }
            }
        } else {
            log::debug!("Worker {} trying to recv src", i);
            match src_receiver.recv().await {
                Err(e) => log::error!("worker: {} cannot recv src {:?}", i, e),
                Ok(src) => {
                    log::debug!("Worker {} received src {:?}", i, src);
                    ar.as_mut().system_command(src).await
                },
            }
        }
    }
}

async fn recv_and_send_response_to_client(
    op_comp_receiver: Receiver<OperationComplete>,
    write_stream: Arc<Mutex<OwnedWriteHalf>>,
    hmac_key: [u8; 32],
) {
    match op_comp_receiver.recv().await {
        Ok(op_complete) => {
            log::debug!("received op_complete {:?}", op_complete);
            let mut data = vec![];
            serialize_client_response(op_complete, &mut data, hmac_key).await; // TODO write response on write_stream
            let mut write = write_stream.lock().await;
            write.write_all(&data).await.unwrap(); // TODO handle
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
    use crate::RegisterCommand::{Client, System};
    use crate::{
        RegisterCommand, ACK_MSG_T, HMAC_TAG_SIZE, MAGIC_NUMBER, READ_MSG_T, WRITE_MSG_T, READ_PROC_MSG_T,
        VALUE_MSG_T, WRITE_PROC_MSG_T,
    };
    use std::io::Error;
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
        //log::debug!("serialized msg length {:?}", msg.len());
        Ok(())
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        let mut start_over = true;
        while start_over {
            start_over = false;
            // slide over bytes in the stream until it detects a valid magic number
            let mut magic_buffer: [u8; 4] = [0; 4];
            if let Err(e) = data.read_exact(&mut magic_buffer).await {
                log::error!(
                    "deserialize_register_command read_exact magic_buffer error {:?}",
                    e
                );
                return Err(e);
            }
            while magic_buffer != MAGIC_NUMBER {
                magic_buffer[0] = magic_buffer[1];
                magic_buffer[1] = magic_buffer[2];
                magic_buffer[2] = magic_buffer[3];
                data.read_exact(&mut magic_buffer[3..4]).await.unwrap();
            }

            let mut msg = MAGIC_NUMBER.to_vec();
            let mut buffer: [u8; 4] = [0; 4];
            if let Err(e) = data.read_exact(&mut buffer).await {
                log::error!(
                    "deserialize_register_command read_exact buffer error {:?}",
                    e
                );
                return Err(e);
            }
            msg.append(&mut buffer.to_vec());
            let (rank, msg_type, key): (Option<u8>, u8, &[u8]) = match buffer[3] {
                0x01 => (None, READ_MSG_T, hmac_client_key),
                0x02 => (None, WRITE_MSG_T, hmac_client_key),
                0x03 => (Some(buffer[2]), READ_PROC_MSG_T, hmac_system_key),
                0x04 => (Some(buffer[2]), VALUE_MSG_T, hmac_system_key),
                0x05 => (Some(buffer[2]), WRITE_PROC_MSG_T, hmac_system_key),
                0x06 => (Some(buffer[2]), ACK_MSG_T, hmac_system_key),
                t => {
                    start_over = true; // discard bytes and start over
                    (Default::default(), Default::default(), (Default::default()))
                }
            };
            if !start_over {
                // everything correct until now
                let res = if let Some(rank) = rank {
                    deserialize_system_command(data, rank, msg_type, hmac_system_key, &mut msg)
                        .await
                } else {
                    deserialize_client_command(data, msg_type, hmac_client_key, &mut msg).await
                };
                match res {
                    Ok(Some(cmd)) => {
                        let mut tag_buffer: [u8; HMAC_TAG_SIZE] = [0; HMAC_TAG_SIZE];
                        if let Err(e) = data.read_exact(&mut tag_buffer).await {
                            log::error!(
                                "deserialize_register_command read_exact tag_buffer error {:?}",
                                e
                            );
                            return Err(e);
                        }
                        let verified = utils::verify_hmac_tag(&tag_buffer, &msg, key);
                        return Ok((cmd, verified));
                    }
                    Ok(None) => start_over = true, // consumed appropriate amount of bytes, starting over
                    Err(e) => return Err(e),
                }
            }
        }
        log::error!("Bug in deserialize_register_command. Shouldn't leave while loop.");
        panic!();
    }
}

pub mod register_client_public {
    use crate::register_client::register_client::Stubborn;
    use crate::SystemRegisterCommand;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::RwLock;
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
