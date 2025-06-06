use object_store::path::Path;
use rand::Rng;
use rand::distr::Alphanumeric;
use slatedb::config::CheckpointOptions;
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use slatedb::{Db, SlateDBError, WriteBatch, admin};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), SlateDBError> {
    console_subscriber::init();

    // Using a local file system, as I am not sure if InMemory will cause issues as it may be latency bound.
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix("/tmp/slatedb-repro")?);

    let path = Path::from("slatedb-repro");

    let kv_store = Arc::new(
        Db::builder(path.clone(), object_store.clone())
            .build()
            .await
            .expect("failed to open db"),
    );

    let semaphore = Arc::new(tokio::sync::Semaphore::new(100));

    tokio::spawn(async move {
        loop {
            let _result = admin::create_checkpoint(
                path.clone(),
                object_store.clone(),
                &CheckpointOptions {
                    lifetime: Some(Duration::from_secs(10 * 60)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            println!("Checkpoint created");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    loop {
        let kv_store = kv_store.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        tokio::spawn(async move {
            let _permit = permit;
            loop {
                let mut batch = WriteBatch::new();

                for _ in 1..=1_000 {
                    let key: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(20)
                        .map(char::from)
                        .collect();

                    let value: String = rand::rng()
                        .sample_iter(&Alphanumeric)
                        .take(30)
                        .map(char::from)
                        .collect();

                    batch.put(&key, &value);
                }

                kv_store.write(batch).await.unwrap();
            }
        });
    }

    Ok(())
}
