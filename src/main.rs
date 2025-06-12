use object_store::path::Path;
use rand::Rng;
use rand::distr::Alphanumeric;
use slatedb::config::{
    CompactorOptions, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use slatedb::{Db, SlateDBError, WriteBatch};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), SlateDBError> {
    tracing_subscriber::fmt::init();

    // Using a local file system, as I am not sure if InMemory will cause issues as it may be latency bound.
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix("/tmp/slatedb-repro")?);

    let path = Path::from("slatedb-repro");

    let compaction_options = CompactorOptions {
        max_concurrent_compactions: 10,
        poll_interval: Duration::from_secs(1),
        ..Default::default()
    };

    let gc_directory_options = GarbageCollectorDirectoryOptions {
        interval: Some(Duration::from_secs(1)),
        min_age: Duration::from_secs(1),
    };

    let garbage_collection_options = GarbageCollectorOptions {
        manifest_options: Some(gc_directory_options.clone()),
        wal_options: Some(gc_directory_options.clone()),
        compacted_options: Some(gc_directory_options),
        ..Default::default()
    };

    let settings = slatedb::config::Settings {
        compactor_options: Some(compaction_options),
        garbage_collector_options: Some(garbage_collection_options),
        ..Default::default()
    };

    let kv_store = Arc::new(
        Db::builder(path.clone(), object_store.clone())
            .with_settings(settings)
            .build()
            .await
            .expect("failed to open db"),
    );

    let semaphore = Arc::new(tokio::sync::Semaphore::new(100));

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
                kv_store.get("something").await.unwrap();
            }
        });
    }

    Ok(())
}
