pub mod sectors_manager {
    use crate::{RankT, TimestampT, SectorIdx, SectorVec, SectorsManager};
    use std::collections::HashMap;
    use std::path::{PathBuf};
    use std::sync::{Arc};
    use tokio::sync::RwLock;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    pub struct MySectorsManager {
        dir: PathBuf,
        cashed_metadata: RwLock<HashMap<SectorIdx, (String, Metadata)>>,
    }

    pub struct Metadata {
        ts: TimestampT,
        wr: RankT
    }

    impl MySectorsManager {
        pub fn new(path: PathBuf) -> Arc<dyn SectorsManager> {
            let metadata = MySectorsManager::recover_metadata(&path);
            Arc::new(MySectorsManager {
                dir: path,
                cashed_metadata: RwLock::new(metadata),
            })
        }

        fn recover_metadata(
            path: &PathBuf,
        ) -> HashMap<SectorIdx, (String, Metadata)> {
            HashMap::new()
            // TODO actual recovery i usunać przy okazji smięci "tmp_xyz"
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
            let meta = self.cashed_metadata.read().await;
            if let Some((_, Metadata{ts, wr})) = (*meta).get(&idx) {
                (*ts, *wr)
            } else {
                log::debug!("Getting meta of sector not yet written into {:?}", idx);
                (0, 0)
            }
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, TimestampT, RankT)) {
            let (SectorVec(data), ts, wr) = sector;

            let mut tmp_path = self.dir.clone().join(format!("tmp_{}_{}_{}", idx, ts, wr));
            let mut path = self.dir.clone().join(format!("{}_{}_{}", idx, ts, wr));
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
