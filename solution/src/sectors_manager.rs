pub mod sectors_manager {
    use crate::{RankT, TimestampT, SectorIdx, SectorVec, SectorsManager};
    use std::collections::HashMap;
    use std::path::{PathBuf};
    use std::sync::{Arc};
    use tokio::sync::{RwLock, Mutex};
    use tokio::fs::{File};
    use tokio::io::AsyncWriteExt;
    use std::str::FromStr;

    pub struct MySectorsManager {
        dir: PathBuf,
        cashed_metadata: RwLock<HashMap<SectorIdx, (String, Metadata)>>,
        recovered: Mutex<bool>, // inner mutability
    }

    pub struct Metadata {
        ts: TimestampT,
        wr: RankT
    }

    impl MySectorsManager {
        pub fn new(path: PathBuf) -> Arc<dyn SectorsManager> {
            Arc::new(MySectorsManager {
                dir: path,
                cashed_metadata: RwLock::new(HashMap::new()),
                recovered: /*Arc::new(*/Mutex::new(false),
            })
        }

        async fn maybe_recover(&self) {
            let mut already_recovered = self.recovered.lock().await;
            if !*already_recovered {
                self.recover_metadata().await;
                *already_recovered = true;
            }
        }

        async fn recover_metadata(&self) {
            match tokio::fs::read_dir(self.dir.clone()).await {
                Ok(mut entries) => {
                    let mut metas = vec![];
                    while let Ok(Some(entry)) = entries.next_entry().await {
                        log::info!("SM REC eentrypath {:?}", entry.path().file_name());
                        let path = entry.path();
                        let os_str = match path.file_name() {
                            Some(os) => os,
                            None => {
                                log::error!("SM could not get filename of path: {:?}", path);
                                panic!();
                            }
                        };
                        let filename = match os_str.to_str() {
                            Some(f) => f,
                           None => {
                                log::error!("SM could not get &str from &OsStr {:?}", os_str);
                                panic!();
                            }
                        };
                        let v: Vec<&str> = filename.split('_').collect();
                        log::info!("SM REC vec: {:?}", v);
                        match v.len() {
                            3 => {
                                // format!("{}_{}_{}", idx, ts, wr));
                                let idx = match SectorIdx::from_str(v[0]) {
                                    Ok(i) => i,
                                    Err(e) => {
                                        log::error!("SM could parse ifx from filename {:?} {:?}", filename, e);
                                        panic!();
                                    }
                                };
                                let ts = match u64::from_str(v[1]) {
                                    Ok(u) => u,
                                    Err(e) => {
                                        log::error!("SM could parse ts from filename {:?} {:?}", filename, e);
                                        panic!();
                                    }
                                };
                                let wr = match u8::from_str(v[2]) {
                                    Ok(u) => u,
                                    Err(e) => {
                                        log::error!("SM could parse ifx from filename {:?} {:?}", filename, e);
                                        panic!();
                                    }
                                };
                                log::info!("SM parsed filename {} into idx {} ts {} wr {}", filename, idx, ts, wr);
                                metas.push((idx, ts, wr));
                            },
                            4 => {
                                // format!("tmp_{}_{}_{}", idx, ts, wr));
                                log::info!("SM tmp file found {:?}", filename);
                                if let Err(e) = tokio::fs::remove_file(path.clone()).await {
                                    log::info!("SM could not remove old tmp file {:?} {:?}", path, e);
                                }

                            },
                            _ => {
                                log::error!("SM malformed filename {:?} in path {:?}", filename, path);
                                if let Err(e) = tokio::fs::remove_file(path.clone()).await {
                                    log::error!("SM could not remove old tmp file {:?} {:?}", path, e);
                                }
                           }
                        }

                    }

                    let mut meta = self.cashed_metadata.write().await;
                    for (idx, ts, wr) in metas {
                        (*meta).insert(idx, (format!("{}_{}_{}", idx, ts, wr), Metadata { ts, wr}));
                    }

                }
                Err(e) => {
                    log::error!("Sectors manager could not explore {:?} directory to recover metadata {:?}", self.dir, e);
                }
            }


            //(*meta).insert(idx, (format!("{}_{}_{}", idx, ts, wr), Metadata{ts: *ts, wr: *wr}));

           /* for dir in dirs {
                let v: Vec<&str> = filename.split('_').collect();
                // coś takiego
                // let i = "123".parse::<i64>();
                let idx = u64::from_ne_bytes(v[0]);
                let ts = u64::from_ne_bytes(v[1]);
                let wr = u8::from_ne_bytes(v[2]);
            }*/
        }

 /*       fn my_deserialize(filename: String) -> (SectorIdx, Metadata) {
            let v: Vec<&str> = filename.split('_').collect();
            let idx = u64::from_ne_bytes(v[0]);
            let ts = u64::from_ne_bytes(v[1]);
            let wr = u8::from_ne_bytes(v[2]);
            (idx, Metadata{ts, wr})
        }*/
      /*  fn my_serialize(idx: u64, meta: Metadata) -> String {
            let mut idx_vec = idx.to_ne_bytes().to_vec();
            let mut ts_vec = meta.ts.to_ne_bytes().to_vec();
            let mut wr_vec = meta.ts.to_ne_bytes().to_vec();
            let mut name = String::from(str::from_utf8(&idx_vec).unwrap());
            name.push_str("_");
            name.push_str(str::from_utf8(&ts_vec).unwrap());
            name.push_str("_");
            name.push_str(str::from_utf8(&wr_vec).unwrap());
            name
        }*/
    }

    #[async_trait::async_trait]
    impl SectorsManager for MySectorsManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            self.maybe_recover().await;
            let meta = self.cashed_metadata.read().await;
            if let Some((file_name, _)) = (*meta).get(&idx) {
                let mut path = self.dir.clone();
                path.push(file_name);
                match tokio::fs::read(path).await {
                    Ok(value) => SectorVec(value),
                    Err(e) => {
                        log::error!("Reading file {:?} resulted in an error {:?}", file_name, e);
                        SectorVec(vec![])
                    },
                }
            } else {
                SectorVec(vec![0; 4096])
            }
        }

        async fn read_metadata(&self, idx: SectorIdx) -> (TimestampT, RankT) {
            self.maybe_recover().await;
            let meta = self.cashed_metadata.read().await;
            if let Some((_, Metadata{ts, wr})) = (*meta).get(&idx) {
                (*ts, *wr)
            } else {
                log::debug!("Getting meta of sector not yet written into {:?}", idx);
                (0, 0)
            }
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, TimestampT, RankT)) {
            self.maybe_recover().await;
            let (SectorVec(data), ts, wr) = sector;
            let d = data.clone(); // todo usunac
            let SECTOR_SIZE = 4096;
            if d[0] != d[SECTOR_SIZE-1] || d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-2] ||d[0] != d[SECTOR_SIZE-4] ||d[0] != d[SECTOR_SIZE-5] {
                log::info!("SM writing to sec {} data: {:?}", idx, data);
            }

            let tmp_path = self.dir.clone().join(format!("tmp_{}_{}_{}", idx, ts, wr));
            let path = self.dir.clone().join(format!("{}_{}_{}", idx, ts, wr));
            {
                let mut meta = self.cashed_metadata.write().await;
                (*meta).insert(idx, (format!("{}_{}_{}", idx, ts, wr), Metadata{ts: *ts, wr: *wr}));
            } // drop lock
            let mut f = File::create(tmp_path.clone()).await.unwrap();
            f.write_all(data).await.unwrap();
            f.sync_data().await.unwrap();
            tokio::fs::rename(tmp_path, path).await.unwrap();
            f.sync_data().await.unwrap();
        }
    }
}
