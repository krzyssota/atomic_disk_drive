pub mod atomic_register {

    use crate::ClientRegisterCommandContent::{Read, Write};
    use crate::SystemRegisterCommandContent::{Ack, ReadProc, Value, WriteProc};
    use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, OperationComplete, OperationReturn, ReadReturn, RegisterClient, SectorVec, SectorsManager, StableStorage, StatusCode, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, SECTOR_SIZE};
    use std::collections::{HashMap, HashSet};
    use std::fs::read;
    use std::future::Future;
    use std::pin::Pin;
    use std::ptr::write;
    use std::sync::Arc;
    use uuid::Uuid;

    pub struct Nnar {
        self_identifier: u8,
        read_ident: u64,
        readlist: HashMap<u8, (u64, u8, SectorVec)>, // readlist[self] := (timestamp, write_rank, val);
        acklist: HashSet<u8>,                        // acklist[q] := Ack;
        reading: bool,
        writing: bool,
        writeval: Option<SectorVec>,
        readval: Option<SectorVec>,
        write_phase: bool,
        callback: Option<
            Box<
                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        >,
        stable_storage: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,
    }
    const RID_KEY: &str = "rid";

    impl Nnar {
        pub async fn new(
            self_identifier: u8,
            metadata: Box<dyn StableStorage>,
            register_client: Arc<dyn RegisterClient>,
            sectors_manager: Arc<dyn SectorsManager>,
            processes_count: usize,
        ) -> Box<dyn AtomicRegister> {
            let read_ident = match metadata.get(RID_KEY).await {
                Some(bytes) => bincode::deserialize(&bytes).unwrap(),
                None => 0
            };
            Box::new(Nnar {
                self_identifier,
                read_ident,
                readlist: HashMap::new(),
                acklist: HashSet::new(),
                reading: false,
                writing: false,
                writeval: None,
                readval: None,
                write_phase: false,
                callback: None,
                stable_storage: metadata,
                register_client: register_client,
                sectors_manager,
                processes_count,
            })
        }

        fn highest(list: HashMap<u8, (u64, u8, SectorVec)>) -> (u64, u8, SectorVec) {
            let mut high_ts = 0;
            let mut high_wr = 0;
            let mut key = 0;

            for  (i, (k, (ts, wr, _sector_vec))) in list.iter().enumerate() {
                if i == 0 || *ts > high_ts || (*ts == high_ts && *wr > high_wr) {
                    high_wr = *wr;
                    high_ts = *ts;
                    key = *k;
                }
            }
            match list.get(&key) {
                Some((t, w, d)) => {
                    (t.clone(), w.clone(), d.clone())
                },
                None => {
                    log::error!("BUGGGGG key:{:?} list:{:?}", key, list);
                    panic!();
                }
            }
        }

       /* fn highest(list: HashMap<u8, (u64, u8, SectorVec)>) -> (u64, u8, SectorVec) {
            let list_cloned = list.clone();
            let (mut high_ts, mut high_r, init_val) = list_cloned.values().next().unwrap(); //"highest should be called if thera are N/2 records in readlist and apparently there are none"
            let mut val: SectorVec = init_val.clone();
            for (_, (curr_ts, curr_r, curr_val)) in list {
                if curr_ts > high_ts || (curr_ts == high_ts && curr_r > high_r) {
                    high_ts = curr_ts;
                    high_r = curr_r;
                    val = curr_val;
                }
            }
            (high_ts, high_r, val)
        }*/
    }

    #[async_trait::async_trait]
    impl AtomicRegister for Nnar {
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            operation_complete: Box<
                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        ) {
            /*
            upon event < nnar, Read > do
                rid := rid + 1;
                store(rid);
                readlist := [ _ ] `of length` N;
                acklist := [ _ ] `of length` N;
                reading := TRUE;
                trigger < sbeb, Broadcast | [READ_PROC, rid] >;


            upon event < nnar, Write | v > do
                rid := rid + 1;
                writeval := v;
                acklist := [ _ ] `of length` N;
                readlist := [ _ ] `of length` N;
                writing := TRUE;
                store(rid);
                trigger < sbeb, Broadcast | [READ_PROC, rid] >;
                        */
            self.callback = Some(operation_complete);
            self.acklist.clear();
            self.readlist.clear();
            self.read_ident += 1;
            self.stable_storage
                .put(RID_KEY, &bincode::serialize(&self.read_ident).unwrap()).await.unwrap();
            match cmd.content {
                Read => {
                    self.reading = true;
                }
                Write { data } => {
                    self.writing = true;
                    self.writeval = Some(data);
                }
            };
            log::debug!("\nAR read handler me {} broadcasting readproc", self.self_identifier);
            let broadcast = Broadcast {
                cmd: Arc::new(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_identifier,
                        msg_ident: Uuid::from_u128(
                            cmd.header.request_identifier as u128
                        ),
                        read_ident: self.read_ident,
                        sector_idx: cmd.header.sector_idx,
                    },
                    content: SystemRegisterCommandContent::ReadProc,
                }),
            };
            self.register_client.broadcast(broadcast).await;
        }

        /// Send system command to the register.
        async fn system_command(&mut self, cmd: SystemRegisterCommand) {
            let header = SystemCommandHeader {
                process_identifier: self.self_identifier,
                //msg_ident: Uuid::from_u128(cmd.header.process_identifier as u128),
                msg_ident: cmd.header.msg_ident,
                read_ident: cmd.header.read_ident,
                sector_idx: cmd.header.sector_idx,
            };
            match cmd.content {
                /*
                 upon event < sbeb, Deliver | p [READ_PROC, r] > do      // od p-tego procesu READ_PROC z jego read_ident
                    trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;  // VALUE na prośbę p-tego z jego read_ident i z moimi ts,write_rank (rank tego kto zapisał,val
                */
                ReadProc => {
                    let sec_idx = cmd.header.sector_idx;
                    let (ts, wr) = self.sectors_manager.read_metadata(sec_idx).await;
                    let data = self.sectors_manager.read_data(sec_idx).await;
                    let SectorVec(d) = data.clone();
                    if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                        log::debug!("\nAR proc:{} reading from sector after ReadProc data[:-10] {:?}\ndata: {:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                    }
                    let content = Value {
                        timestamp: ts,
                        write_rank: wr,
                        sector_data: data,
                    };
                    let sender = cmd.header.process_identifier;
                    log::debug!("\nAR proc:{} readproc handler sending value to {}", self.self_identifier, sender);
                    self.register_client.send(crate::Send {
                        cmd: Arc::new(SystemRegisterCommand { header, content }),
                        target: sender as usize,
                    }).await;
                } /*
             upon event <sl, Deliver | q, [VALUE, r, ts', wr', v'] > such that r == rid and !write_phase do // od q otrzymalem jego timestamp, write_rank i wartość; wlaściwe parsowanie dla N/2 takiej wiadomosci. wczesniej tylko odnotowuje, póżniej ignoruje
                readlist[q] := (ts', wr', v');
                if #(readlist) > N / 2 and (reading or writing) then
                    readlist[self] := (ts, wr, val);
                    (maxts, rr, readval) := highest(retaadlist); // najwyższy (ts, rank) i powiązana z nim wartość
                    readlist := [ _ ] `of length` N;
                    acklist := [ _ ] `of length` N;
                    write_phase := TRUE;
                    if reading = TRUE then
                        trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts, rr, readval] >; // chce przekazać wszystkim to co odczytałem (najbardziej aktualny stan)
                    else
                        (ts, wr, val) := (maxts + 1, rank(self), writeval);
                        store(ts, wr, val);
                        trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts + 1, rank(self), writeval] >; // chce zapisać nową wartośc z lepszym timestampem niż najwyzszy obecnie

                */
                Value {
                    timestamp: ts_,
                    write_rank: wr_,
                    sector_data: data_,
                } => {
                    log::debug!("\nAR proc:{} Value handler 1 sent by {} \n read_ident {} cmd.header.read_ident {} !write_phase {}",
                        self.self_identifier, cmd.header.process_identifier, self.read_ident, cmd.header.read_ident, !self.write_phase);

                    if self.read_ident == cmd.header.read_ident && !self.write_phase {
                        let sender = cmd.header.process_identifier;
                        let SectorVec(d) = data_.clone(); // TODO usunac
                        if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                            log::debug!("\nAR proc:{} inserting data from Value into readlist data[:-10] {:?}\ndata: {:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                        }
                        self.readlist.insert(sender, (ts_, wr_, data_));
                        log::debug!("\nAR proc:{} Value handler 2 sent by {}.\
                        \n 2*#readlist={} >? processes_count={} \
                        \n reading {} writin {},\
                        \n read_list {:?} ack_list {:?}",
                            self.self_identifier, sender,
                            2 * self.readlist.len(), self.processes_count,
                            self.reading, self.writing,
                            self.readlist.keys(), self.acklist);

                        if 2 * self.readlist.len() > self.processes_count
                            && (self.reading || self.writing)
                        {
                            let sec_idx = cmd.header.sector_idx;
                            let (ts, wr) = self.sectors_manager.read_metadata(sec_idx).await;
                            let data = self.sectors_manager.read_data(sec_idx).await;
                            let SectorVec(d) = data.clone();
                            if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                                log::debug!("\nAR proc:{} reading from sector after Value quorum data[:-10] {:?}\ndata: {:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                            }

                            self.readlist.insert(self.self_identifier, (ts, wr, data));
                            let (maxts, r, read_data) = Nnar::highest(self.readlist.clone());
                            let SectorVec(d) = read_data.clone(); // todo usunac
                            if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                                log::debug!("\nAR proc:{} highest read_data after Value quorum data[:-10] {:?}\ndata: {:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                            }
                            self.readval = Some(read_data.clone());
                            self.readlist.clear();
                            self.acklist.clear();
                            self.write_phase = true;
                            if self.reading {
                                log::debug!("\nAR porc:{} Value quorum (reading). broadcasting Write_proc", self.self_identifier);
                                let content = WriteProc {
                                    timestamp: maxts,
                                    write_rank: r,
                                    data_to_write: read_data,
                                };
                                self.register_client.broadcast(Broadcast {
                                    cmd: Arc::new(SystemRegisterCommand { header, content }),
                                }).await;
                            } else {
                                log::debug!("\nAR proc:{} Value quorum (writing). broadcasting Write_proc", self.self_identifier);
                                let ts = maxts + 1;
                                let wr = self.self_identifier;
                                if let Some(writeval) = self.writeval.clone() {
                                    let SectorVec(d) = writeval.clone();
                                    if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                                        log::debug!("\nAR d[0] {}, d[SECTOR_SIZE-1] {}, d[0] != d[SECTOR_SIZE-1] {}", d[0], d[SECTOR_SIZE-1], d[0] != d[SECTOR_SIZE-1]);
                                        log::debug!("\nAR proc:{} writing to sector after Value (writing) quorum data[:-10] {:?}\ndata: {:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                                    }
                                    self.sectors_manager.write(sec_idx, &(writeval.clone(), ts, wr)).await;
                                    let content = WriteProc {
                                        timestamp: ts,
                                        write_rank: wr,
                                        data_to_write: writeval,
                                    };
                                    self.register_client.broadcast(Broadcast {
                                        cmd: Arc::new(SystemRegisterCommand { header, content }),
                                    }).await;
                                } else {
                                    log::error!("AR proc:{} value handler no writeval", self.self_identifier);
                                    panic!("writaval = None w atomic_register 251 system_command handling Value")
                                }
                            }
                        }
                    }
                }
                /*
                upon event < sbeb, Deliver | p, [WRITE_PROC, r, ts', wr', v'] > do // dostalem nową wartość, jeśli lepsza niż obecna to zapisuje i wysyłam ack
                    if (ts', wr') > (ts, wr) then
                        (ts, wr, val) := (ts', wr', v');
                        store(ts, wr, val);
                    trigger < sl, Send | p, [ACK, r] >;
                 */
                WriteProc {
                    timestamp,
                    write_rank,
                    data_to_write,
                } => {
                    log::debug!("\nAR proc:{} writeproc handler sending ack to {}", self.self_identifier, cmd.header.process_identifier);
                    let sec_idx = cmd.header.sector_idx;
                    let (ts, wr) = self.sectors_manager.read_metadata(sec_idx).await;
                    if timestamp > ts || (timestamp == ts && write_rank > wr) {
                        let SectorVec(d) = data_to_write.clone();
                        if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                            log::debug!("\nAR proc:{} writing to sector in response to WriteProc data[:-10] {:?}\ndata:{:?}", self.self_identifier, &d[SECTOR_SIZE-10..], d);
                            //  245, 138, 18, 62, 123
                        }
                        self.sectors_manager.write(sec_idx, &(data_to_write, timestamp, write_rank)).await;
                       /* if let Some(writeval) = self.writeval.clone() {
                            let sec_idx = writeval.sector_idx;
                            self.sectors_manager.write(sec_idx, &(data_to_write, ts, wr)).await;
                        } else {
                            panic!("writaval = None w atomic_register 264 system_command handling Value")
                        }*/
                    }
                    let content = Ack;
                    let sender = cmd.header.process_identifier;
                    self.register_client.send(crate::Send {
                        cmd: Arc::new(SystemRegisterCommand { header, content }),
                        target: sender as usize,
                    }).await;
                }
                /*
                upon event < sl, Deliver | q, [ACK, r] > such that r == rid and write_phase do // dostałem ack czyli ktoś sb zapisał to co mu wyslalem
                 acklist[q] := Ack;
                 if #(acklist) > N / 2 and (reading or writing) then
                     acklist := [ _ ] `of length` N;
                     write_phase := FALSE;
                     if reading = TRUE then
                         reading := FALSE;
                         trigger < nnar, ReadReturn | readval >;
                     else
                         writing := FALSE;
                         trigger < nnar, WriteReturn >;
                         */
                Ack => {
                    if self.read_ident == cmd.header.read_ident && self.write_phase {
                        self.acklist.insert(cmd.header.process_identifier);
                        if self.acklist.len() > (self.processes_count/2) && (self.reading || self.writing) {
                            self.acklist.clear();
                            self.write_phase = false;
                            let op_return= if self.reading {
                                self.reading = false;
                                if let Some(readval) = self.readval.clone() {
                                    OperationReturn::Read(ReadReturn { read_data: Some(readval) })
                                } else {
                                    log::error!("AR proc:{} ack handler no readval", self.self_identifier);
                                    panic!("readval = None w atomic_register 308 system_command handling Ack")
                                }
                            } else {
                                self.writing = false;
                                OperationReturn::Write
                            };
                            if let Some(_) = self.callback {
                                (self.callback.take().unwrap())(OperationComplete{
                                    status_code: StatusCode::Ok,
                                    request_identifier: cmd.header.msg_ident.as_u128() as u64,
                                    op_return
                                }).await;
                            }
                           /* if let Some(callback) = self.callback {
                                callback(OperationComplete{
                                    status_code: StatusCode::Ok,
                                    request_identifier,
                                    op_return
                                });
                                self.callback = None;
                            }*/
                        }
                    }
                }
            }
        }
    }
}