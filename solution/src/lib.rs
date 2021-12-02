mod domain;
mod handle_cmd;

use std::process::id;
pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;
use tokio::net::{TcpListener, TcpStream};
use std::net::{SocketAddr};
use crate::handle_cmd::cmd::get_cmd;


pub async fn run_register_process(config: Configuration) {
    let ident: u8 = config.public.self_rank;
    let (host, port): (&String, &u16) = config.public.tcp_locations[ident-1];
    let addr: SocketAddr = (host, port).parse::<SocketAddr>().unwrap();
    let tcp_listener: TcpListener = TcpListener::bind(&addr).await.unwrap();
    loop {
        match tcp_listener.accept().await {
            Ok((tcp_stream, sock_addr)) => {
                let (read_stream, write_stream) = tcp_stream.into_split();
                loop {
                    let cmd = get_cmd(read_stream.clone(), write_stream.clone(), // todo maybe Arc
                                      config.hmac_system_key.clone(), config.hmac_client_key.clone(),
                                      config.public.max_sector).await;
                    let res = handle_cmd(cmd).await;
                }
            }
            Err(e) => {
                log::error!("Error while accepting connection on TcpListener: {:?}", e);
            }
        }
    }
}

pub mod atomic_register_public {
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
        unimplemented!()
    }
}

pub mod sectors_manager_public {
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
        unimplemented!()
    }
}

pub mod transfer_public {
    use std::fs::rename;
    use crate::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, MAGIC_NUMBER, READ, RegisterCommand, SectorVec, WRITE};
    use std::io::Error;
    use serde::Serialize;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        /*
        When a message which does not comply with the presented above formats is received, it shall be handled as follows:
!"The solution shall slide over bytes in the stream until it detects a valid magic number. This marks a beginning of a message.
!"If a message type is invalid, the solution shall discard the magic number and the following 4 bytes (8 bytes in total).
!"In case of every other error, the solution shall consume the same number of bytes as if a message of this type was processed successfully.
        */

        // slide over bytes in the stream until it detects a valid magic number
       let mut magic_buffer: [u8; 4] = [0; 4];
        data.read_exact(&mut *magic_buffer).await.unwrap();
        while magic_buffer != MAGIC_NUMBER {
            magic_buffer[0] = magic_buffer[1];
            magic_buffer[1] = magic_buffer[2];
            magic_buffer[2] = magic_buffer[3];
            data.read_exact(&mut magic_buffer[3..4]);
        }
        // If a message type is invalid, the solution shall discard the magic number
        // and the following 4 bytes (8 bytes in total).
        let mut pad_msg_type: [u8; 4] = [0; 4]; // 3 bytes padding, 1 byte msg type
        data.read_exact(&mut *pad_msg_type);
        let cmd = match pad_msg_type[3] {
            0x01 => {
                deserialize_client_command(data, READ, hmac_system_key, hmac_client_key).await
            },
            0x02 => {
                deserialize_client_command(data, WRITE, hmac_system_key, hmac_client_key).await
            }
            READ_PROC => {

            }
            _ => {}
        };



        Ok((RegisterCommand{}, true))

    }

    async fn deserialize_client_command(data: &mut (dyn AsyncRead + Send + Unpin), msg_type: u8,
                                         hmac_system_key: &[u8; 64],
                                         hmac_client_key: &[u8; 32]) -> RegisterCommand {
        let mut buffer: [u8; 8] = [0; 8];
        data.read_exact(&mut buffer);
        let req_no: u64 = u64::from_be_bytes(buffer);
        data.read_exact(&mut buffer);
        let sec_idx: u64 = u64::from_be_bytes(buffer);
        match msg_type {
            READ => {
                RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: req_no,
                        sector_idx: sec_idx,
                    },
                    content: ClientRegisterCommandContent::Read
                })
            }
            WRITE => {
                let mut content_buffer: Vec<u8> = Vec::with_capacity(4096);
                data.read_exact(&mut content_buffer);

                RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: req_no,
                        sector_idx: sec_idx
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(content_buffer)
                    }
                })
            }
            _ => {}
        }
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        unimplemented!()
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

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
}

pub mod stable_storage_public {
    #[async_trait::async_trait]
    /// A helper trait for small amount of durable metadata needed by the register algorithm
    /// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
    /// is durable, as one could expect.
    pub trait StableStorage: Send + Sync {
        async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

        async fn get(&self, key: &str) -> Option<Vec<u8>>;
    }
}

struct Storage {
    dir: PathBuf,
    key_max_len: usize,
    value_max_len: usize,
    //hashed_keys: HashMap<str, String>,
}
#[async_trait::async_trait]
impl StableStorage for Storage {

    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > self.key_max_len {
            return Err(String::from("Key too long (>255)."));
        }
        if value.len() > self.value_max_len {
            return Err(String::from("Value too long (>65535)."))
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
    Box::new(Storage{ dir: root_storage_dir, key_max_len: 255, value_max_len: 65535})
}

