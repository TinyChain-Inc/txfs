use std::io::{self, BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use freqfs::Cache;

use super::{Data, InstalledPublication, Observation, TestDir, PUBLICATION_OBSERVER};
use crate::{Dir, Error, Result};

const ROOT_ENV: &str = "TXFS_TEST_PARTIAL_PUBLICATION_ROOT";
const READY: &str = "txfs-first-publication-durable";

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn process_death_preserves_partial_publication_evidence() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    if let Some(path) = std::env::var_os(ROOT_ENV) {
        return runtime.block_on(async {
            let root =
                Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.into())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            for name in ["left", "right"] {
                dir.create_file(1, name.parse()?, Data(vec![1])).await?;
            }

            dir.commit(1).await?;
            dir.finalize(1).await?;
            for name in ["left", "right"] {
                dir.delete(2, name.parse()?).await?;
                dir.create_file(2, name.parse()?, Data(vec![2])).await?;
            }

            root.sync_all().await?;
            let observer = Observation::new(None);
            PUBLICATION_OBSERVER.with(|slot| *slot.borrow_mut() = Some(observer.clone()));
            let _installed = InstalledPublication;
            let mut commit = Box::pin(dir.commit(2));
            tokio::time::timeout(
                Duration::from_secs(3),
                std::future::poll_fn(|cx| match std::future::Future::poll(commit.as_mut(), cx) {
                    std::task::Poll::Pending if observer.active.load(Ordering::SeqCst) == 1 => {
                        std::task::Poll::Ready(())
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                    std::task::Poll::Ready(result) => {
                        panic!("expected first durable publication: {result:?}")
                    }
                }),
            )
            .await
            .unwrap();

            assert!(!dir.entries.is_committed(2)?);
            println!("{READY}");
            io::stdout().flush()?;
            // Keep every owner and the incomplete commit alive until the parent kills us.
            std::future::pending::<()>().await;
            unreachable!()
        });
    }

    let path = TestDir::new()?;
    let mut process = Process(Command::new(std::env::current_exe()?)
        .args(["--exact", "lifecycle_tests::publication_process::process_death_preserves_partial_publication_evidence", "--nocapture"])
        .env(ROOT_ENV, &path.0)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?);

    let stdout = process.0.stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let ready = io::BufReader::new(stdout)
            .lines()
            .any(|line| line.is_ok_and(|line| line == READY));

        let _ = send.send(ready);
    });

    let ready = receive.recv_timeout(Duration::from_secs(10));
    // Kill and reap even when readiness failed, before joining the pipe reader.
    process.0.kill()?;
    let status = process.0.wait()?;
    reader.join().unwrap();
    assert!(
        matches!(ready, Ok(true)),
        "child did not reach publication boundary: {ready:?}"
    );

    assert!(!status.success());

    runtime.block_on(async {
        let mut changed = 0;
        for name in ["left", "right"] {
            let bytes = tokio::fs::read(path.0.join(name)).await?;
            assert!(bytes == [1] || bytes == [2]);
            changed += usize::from(bytes == [2]);
            assert_eq!(
                tokio::fs::read(path.0.join(crate::VERSIONS).join(name).join("2")).await?,
                [2]
            );
        }

        assert_eq!(changed, 1);
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        assert!(matches!(
            Dir::<u64, Data>::load(root).await,
            Err(Error::Corrupt(_))
        ));

        // Startup rejection must retain both stages for the caller's recovery process.
        for name in ["left", "right"] {
            assert_eq!(
                tokio::fs::read(path.0.join(crate::VERSIONS).join(name).join("2")).await?,
                [2]
            );
        }

        Ok(())
    })
}
