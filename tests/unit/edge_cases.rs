use std::io;
use std::sync::atomic::Ordering;
use std::time::Duration;

use freqfs::{Cache, FileLoad, FileSave};
use futures::{poll, StreamExt};
use get_size::GetSize;
use safecast::as_type;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{Data, Installed, Observation, TestDir, OBSERVER};
use crate::{Dir, Error, Id, Result};

#[derive(Clone)]
enum Payload {
    Bytes(Vec<u8>),
    Text(String),
}

as_type!(Payload, Bytes, Vec<u8>);
as_type!(Payload, Text, String);

impl GetSize for Payload {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Bytes(value) => value.capacity(),
            Self::Text(value) => value.capacity(),
        }
    }
}

impl FileSave for Payload {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let (tag, bytes) = match self {
            Self::Bytes(bytes) => (0, bytes.as_slice()),
            Self::Text(text) => (1, text.as_bytes()),
        };

        file.write_all(&[tag]).await?;
        file.write_all(bytes).await?;
        Ok(bytes.len() as u64 + 1)
    }
}

impl FileLoad for Payload {
    async fn load(
        _: &std::path::Path,
        mut file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut tag = [0];
        file.read_exact(&mut tag).await?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        match tag[0] {
            0 => Ok(Self::Bytes(bytes)),
            1 => String::from_utf8(bytes)
                .map(Self::Text)
                .map_err(|cause| io::Error::new(io::ErrorKind::InvalidData, cause)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid fixture payload tag",
            )),
        }
    }
}

#[tokio::test]
async fn stale_handles_follow_payload_and_entry_kind_changes() -> Result<()> {
    let path = TestDir::new()?;
    {
        let root =
            Cache::<Payload>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Payload>::load(root.clone()).await?;
        let name: Id = "value".parse()?;
        let file = dir.create_file(1, name.clone(), vec![1u8]).await?;
        dir.commit(1).await?;
        dir.finalize(1).await?;
        for error in [
            file.read::<String>(2).await.err().unwrap(),
            dir.read_file::<String>(2, &name).await.err().unwrap(),
        ] {
            assert!(
                matches!(error, Error::IO(ref cause) if cause.kind() == io::ErrorKind::InvalidData)
            );
        }

        // Failed type selection released both native and transaction access.
        tokio::time::timeout(Duration::from_secs(3), dir.delete(2, name.clone()))
            .await
            .unwrap()?;

        dir.create_file(2, name.clone(), String::from("replacement"))
            .await?;

        dir.commit(2).await?;
        dir.finalize(2).await?;
        assert_eq!(&*file.read::<String>(2).await?, "replacement");
        assert_eq!(&*dir.read_file::<String>(2, &name).await?, "replacement");
        let mut stream = dir.files::<String>(2).await?;
        let (_, read) = stream.next().await.unwrap()?;
        assert_eq!(&*read, "replacement");
        drop((read, stream));
        assert!(
            matches!(file.read::<Vec<u8>>(2).await, Err(Error::IO(ref cause)) if cause.kind() == io::ErrorKind::InvalidData)
        );

        dir.delete(3, name.clone()).await?;
        dir.commit(3).await?;
        dir.finalize(3).await?;
        dir.create_dir(4, name.clone()).await?.sync_all().await?;
        dir.commit(4).await?;
        for error in [
            file.read::<String>(4).await.err().unwrap(),
            dir.read_file::<String>(4, &name).await.err().unwrap(),
        ] {
            assert!(
                matches!(error, Error::IO(ref cause) if cause.kind() == io::ErrorKind::InvalidData)
            );
        }

        // An independently retained handle still follows the name after failure.
        dir.delete(5, name.clone()).await?;
        dir.rollback(5).await?;
        assert!(dir.get_dir(5, &name).await?.is_some());
        dir.finalize(4).await?;
        root.sync_all().await?;
    }

    let root =
        Cache::<Payload>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

    let dir = Dir::<u64, Payload>::load(root).await?;
    assert!(dir.get_dir(6, &"value".parse()?).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn concurrent_duplicate_decisions_share_one_application() -> Result<()> {
    for commit in [false, true] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            dir.create_file(1, "value".parse()?, Data(vec![7])).await?;
            let observer = Observation::new(None);
            OBSERVER.with(|slot| *slot.borrow_mut() = Some(observer.clone()));
            let installed = Installed;
            let mut first = Box::pin(async {
                if commit {
                    dir.commit(1).await
                } else {
                    dir.rollback(1).await
                }
            });

            let mut second = Box::pin(async {
                if commit {
                    dir.commit(1).await
                } else {
                    dir.rollback(1).await
                }
            });

            tokio::time::timeout(
                Duration::from_secs(3),
                std::future::poll_fn(|cx| match std::future::Future::poll(first.as_mut(), cx) {
                    std::task::Poll::Pending if observer.active.load(Ordering::SeqCst) == 1 => {
                        std::task::Poll::Ready(())
                    }
                    std::task::Poll::Pending => std::task::Poll::Pending,
                    std::task::Poll::Ready(result) => panic!("expected admitted work: {result:?}"),
                }),
            )
            .await
            .unwrap();

            assert!(poll!(&mut second).is_pending());
            observer.released.store(true, Ordering::SeqCst);
            observer.release.notify_waiters();
            tokio::time::timeout(Duration::from_secs(3), async {
                futures::try_join!(first, second)
            })
            .await
            .unwrap()?;

            drop(installed);
            assert_eq!(observer.started.load(Ordering::SeqCst), 1);
            assert_eq!(observer.active.load(Ordering::SeqCst), 0);
            assert_eq!(dir.entries.is_committed(1)?, commit);
            assert_eq!(dir.contains(2, &"value".parse()?).await?, commit);
            if commit {
                assert_eq!(dir.read_file::<Data>(2, &"value".parse()?).await?.0, [7]);
            }

            drop(root.try_write()?);
            dir.finalize(1).await?;
            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        assert_eq!(
            Dir::<u64, Data>::load(root)
                .await?
                .contains(2, &"value".parse()?)
                .await?,
            commit
        );
    }

    Ok(())
}
