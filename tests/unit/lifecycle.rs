use std::cell::RefCell;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use freqfs::{Cache, FileLoad, FileSave};
use futures::poll;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;

use crate::{Dir, Result};

#[tokio::test]
async fn directory_commits_allow_either_causal_polling_order() -> Result<()> {
    for newer_first in [false, true] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            for (txn, name) in [(2, "earlier"), (3, "later")] {
                dir.create_dir(txn, name.parse()?).await?.sync_all().await?;
            }

            let earlier = dir.commit(2);
            let later = dir.commit(3);
            futures::pin_mut!(earlier, later);
            if newer_first {
                assert!(poll!(&mut later).is_pending());
            } else {
                assert!(poll!(&mut earlier).is_pending());
            }

            tokio::time::timeout(Duration::from_secs(3), async {
                futures::try_join!(earlier, later)
            })
            .await
            .unwrap()?;

            assert!(dir.contains(3, &"earlier".parse()?).await?);
            assert!(dir.contains(3, &"later".parse()?).await?);
            dir.finalize(3).await?;
            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        assert!(dir.contains(4, &"earlier".parse()?).await?);
        assert!(dir.contains(4, &"later".parse()?).await?);
    }

    Ok(())
}

#[tokio::test]
async fn file_predecessors_finish_before_directory_publication_exclusion() -> Result<()> {
    for newer_first in [false, true] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            let name: crate::Id = "value".parse()?;
            let file = dir.create_file(1, name.clone(), Data(vec![1])).await?;
            dir.commit(1).await?;
            assert!(dir.delete(2, "value".parse()?).await?);
            dir.create_file(2, "value".parse()?, Data(vec![2])).await?;
            dir.create_dir(3, "later".parse()?)
                .await?
                .sync_all()
                .await?;

            let earlier = dir.commit(2);
            let later = dir.commit(3);
            futures::pin_mut!(earlier, later);
            if newer_first {
                assert!(poll!(&mut later).is_pending());
            } else {
                assert!(poll!(&mut earlier).is_pending());
            }

            tokio::time::timeout(Duration::from_secs(3), async {
                futures::try_join!(earlier, later)
            })
            .await
            .unwrap()?;

            assert_eq!(file.read::<Data>(4).await?.0, vec![2]);
            dir.commit(3).await?;
            dir.finalize(3).await?;
            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        assert_eq!(
            dir.get_file(4, &"value".parse()?)
                .await?
                .unwrap()
                .read::<Data>(4)
                .await?
                .0,
            vec![2]
        );

        assert!(dir.contains(4, &"later".parse()?).await?);
    }

    Ok(())
}

#[tokio::test]
async fn busy_directory_commit_does_not_publish_file_work() -> Result<()> {
    let path = TestDir::new()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root.clone()).await?;
    let name: crate::Id = "value".parse()?;
    let file = dir.create_file(1, name.clone(), Data(vec![1])).await?;
    let guard = dir.iter(1).await?.next().unwrap().1;
    assert!(matches!(
        dir.commit(1).await,
        Err(crate::Error::Conflict(txn_lock::Error::Busy))
    ));

    assert!(!root.read().await.contains(name.as_str()));
    assert_eq!(file.read::<Data>(1).await?.0, vec![1]);
    drop(guard);
    let read = file.read::<Data>(1).await?;
    assert!(matches!(
        dir.commit(1).await,
        Err(crate::Error::Conflict(txn_lock::Error::Busy))
    ));

    assert!(!root.read().await.contains(name.as_str()));
    drop(read);
    dir.commit(1).await?;
    assert_eq!(file.read::<Data>(2).await?.0, vec![1]);
    Ok(())
}

#[tokio::test]
async fn commit_serializes_deletion_with_newer_creation() -> Result<()> {
    let path = TestDir::new()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let name: crate::Id = "child".parse()?;
        dir.create_dir(1, name.clone()).await?.sync_all().await?;
        dir.commit(1).await?;
        assert!(dir.delete(2, name.clone()).await?);
        {
            let guard = root.write().await;
            let commit = dir.commit(2);
            let create = dir.create_dir(3, name.clone());
            futures::pin_mut!(commit, create);
            assert!(poll!(&mut commit).is_pending());
            assert!(poll!(&mut create).is_pending());
            drop(guard);
            let (_, created) = tokio::time::timeout(Duration::from_secs(3), async {
                futures::try_join!(commit, create)
            })
            .await
            .unwrap()?;

            assert!(dir.contains(3, &name).await?);
            assert!(root.read().await.contains(name.as_str()));
            created.sync_all().await?;
        }

        dir.commit(2).await?;
        dir.commit(3).await?;
        dir.finalize(3).await?;
        root.sync_all().await?;
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    assert!(
        Dir::<u64, Data>::load(root)
            .await?
            .contains(4, &"child".parse()?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn conflicting_native_kinds_reject_before_stage_creation() -> Result<()> {
    for committed in [false, true] {
        let path = TestDir::new()?;
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let name: crate::Id = "child".parse()?;
        let child = dir.create_dir(1, name.clone()).await?;
        let txn = if committed {
            child.sync_all().await?;
            dir.commit(1).await?;
            2
        } else {
            1
        };

        assert!(dir.delete(txn, name.clone()).await?);
        assert!(
            matches!(dir.create_file(txn, name.clone(), Data(vec![2])).await,
            Err(crate::Error::IO(error)) if error.kind() == io::ErrorKind::AlreadyExists)
        );

        let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
        assert!(!versions.read().await.contains(name.as_str()));
        dir.rollback(txn).await?;
        assert_eq!(dir.contains(txn, &name).await?, committed);
        if committed {
            child.sync_all().await?;
        }

        dir.finalize(txn).await?;
    }

    Ok(())
}

#[tokio::test]
async fn finalization_reclaims_removed_files_and_cleaned_children() -> Result<()> {
    for (committed_first, recreate) in [(false, false), (true, false), (true, true)] {
        let path = TestDir::new()?;
        let name: crate::Id = "value".parse()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root).await?;
            let original = dir.create_file(1, name.clone(), Data(vec![1])).await?;
            let mut deletion = if committed_first {
                dir.commit(1).await?;
                2
            } else {
                1
            };

            assert!(dir.delete(deletion, name.clone()).await?);
            dir.commit(deletion).await?;
            if recreate {
                let replacement = dir.create_file(3, name.clone(), Data(vec![3])).await?;
                dir.commit(3).await?;
                assert!(dir.delete(4, name.clone()).await?);
                dir.commit(4).await?;
                dir.finalize(4).await?;
                assert!(matches!(
                    replacement.read::<Data>(3).await,
                    Err(crate::Error::Conflict(txn_lock::Error::Outdated))
                ));

                deletion = 4;
            }

            dir.finalize(deletion).await?;
            dir.finalize(deletion).await?;
            dir.finalize(deletion - 1).await?;
            if committed_first {
                assert!(matches!(
                    original.read::<Data>(0).await,
                    Err(crate::Error::Conflict(txn_lock::Error::Outdated))
                ));
            } else {
                // An unpublished owner removed from pending membership has no
                // retained identity; its caller must not reuse that alias.
                assert!(original.read::<Data>(0).await.is_err());
            }
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        assert!(dir.is_empty(3).await?);
    }

    let path = TestDir::new()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let parent = Dir::<u64, Data>::load(root).await?;
        let child = parent.create_dir(1, "child".parse()?).await?;
        let child = Dir::<u64, Data>::load(child).await?;
        child.rollback(1).await?;
        child.finalize(1).await?;
        parent.rollback(1).await?;
        parent.finalize(1).await?;
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    assert!(Dir::<u64, Data>::load(root).await?.is_empty(3).await?);
    for directory_stage in [false, true] {
        let path = TestDir::new()?;
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        dir.create_file(1, "bad".parse()?, Data(vec![1])).await?;
        dir.commit(1).await?;
        let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
        let stages = versions.read().await.get_dir("bad").unwrap().clone();
        if directory_stage {
            let mut stages = stages.write().await;
            assert!(stages.delete("1").await);
            stages.sync_deleted().await?;
            stages.create_dir("1".into())?;
        } else {
            stages
                .write()
                .await
                .create_file("not-a-transaction".into(), Data(vec![0]))
                .await?;
        }

        assert!(matches!(
            dir.finalize(1).await,
            Err(crate::Error::Corrupt(_))
        ));

        // Failed validation never silently discards the malformed entry.
        assert!(stages.read().await.contains(if directory_stage {
            "1"
        } else {
            "not-a-transaction"
        }));
    }

    Ok(())
}

#[tokio::test]
async fn finalization_preserves_newer_membership_and_stages() -> Result<()> {
    let path = TestDir::new()?;
    let name: crate::Id = "value".parse()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let historical = dir
            .create_file(1, "historical".parse()?, Data(vec![1]))
            .await?;

        dir.commit(1).await?;
        assert!(dir.delete(3, "historical".parse()?).await?);
        dir.create_file(3, "historical".parse()?, Data(vec![30]))
            .await?;

        dir.create_dir(3, "child".parse()?).await?;
        let file = dir.create_file(3, name.clone(), Data(vec![3])).await?;
        dir.finalize(2).await?;
        assert!(root.read().await.contains("child"));
        assert_eq!(file.read::<Data>(3).await?.0, [3]);
        assert_eq!(historical.read::<Data>(3).await?.0, [30]);
        dir.commit(3).await?;
        dir.finalize(3).await?;
        root.sync_all().await?;
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root).await?;
    assert!(dir.get_dir(4, &"child".parse()?).await?.is_some());
    assert_eq!(dir.read_file::<Data>(4, &name).await?.0, [3]);
    assert_eq!(
        dir.read_file::<Data>(4, &"historical".parse()?).await?.0,
        [30]
    );

    Ok(())
}

#[tokio::test]
async fn cleanup_serializes_native_creation_with_membership_publication() -> Result<()> {
    for file in [false, true] {
        let path = TestDir::new()?;
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let barrier = if file {
            root.read().await.get_dir(crate::VERSIONS).unwrap().clone()
        } else {
            root.clone()
        };

        let guard = barrier.read().await;
        let create = async {
            if file {
                dir.create_file(3, "value".parse()?, Data(vec![3])).await?;
            } else {
                dir.create_dir(3, "value".parse()?).await?;
            }

            Ok::<_, crate::Error>(())
        };

        let cleanup = dir.finalize(2);
        futures::pin_mut!(create, cleanup);
        assert!(poll!(&mut create).is_pending());
        assert!(poll!(&mut cleanup).is_pending());
        drop(guard);
        tokio::time::timeout(Duration::from_secs(3), async {
            futures::try_join!(create, cleanup)
        })
        .await
        .unwrap()?;

        assert!(dir.contains(3, &"value".parse()?).await?);
        dir.commit(3).await?;
        dir.finalize(3).await?;
    }

    Ok(())
}

thread_local! {
    static OBSERVER: RefCell<Option<Arc<Observation>>> = const { RefCell::new(None) };
    static SAVE_OBSERVER: RefCell<Option<Arc<Observation>>> = const { RefCell::new(None) };
}

struct Observation {
    started: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
    released: AtomicBool,
    release: Notify,
    fail_at: Option<usize>,
}

impl Observation {
    fn new(fail_at: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            started: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            released: AtomicBool::new(false),
            release: Notify::new(),
            fail_at,
        })
    }
}

struct Active(Arc<Observation>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Installed;

impl Drop for Installed {
    fn drop(&mut self) {
        OBSERVER.with(|slot| slot.borrow_mut().take());
    }
}

pub(crate) async fn entered() -> Result<()> {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.enter().await?;
    }

    Ok(())
}

impl Observation {
    async fn enter(self: &Arc<Self>) -> io::Result<()> {
        let index = self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let _active = Active(self.clone());
        if self.fail_at == Some(index) {
            return Err(io::Error::other("injected lifecycle error"));
        }

        while !self.released.load(Ordering::SeqCst) {
            let released = self.release.notified();
            if self.released.load(Ordering::SeqCst) {
                break;
            }

            released.await;
        }

        Ok(())
    }
}

struct InstalledSave;

impl Drop for InstalledSave {
    fn drop(&mut self) {
        SAVE_OBSERVER.with(|slot| slot.borrow_mut().take());
    }
}

#[tokio::test]
async fn historical_file_reads_never_use_newer_canonical_bytes() -> Result<()> {
    let path = TestDir::new()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;
    root.write()
        .await
        .create_file("value".into(), Data(vec![0]))
        .await?
        .sync_all()
        .await?;

    let dir = Dir::<u64, Data>::load(root).await?;
    let name = "value".parse()?;
    let file = dir.get_file(1, &name).await?.unwrap().clone();
    assert_eq!(file.read::<Data>(1).await?.0, [0]);
    dir.commit(1).await?;

    assert!(dir.delete(3, "value".parse()?).await?);
    dir.create_file(3, "value".parse()?, Data(vec![3])).await?;
    dir.commit(3).await?;
    assert!(matches!(
        file.read::<Data>(1).await,
        Err(crate::Error::Conflict(txn_lock::Error::Outdated))
    ));

    assert!(dir.delete(2, "value".parse()?).await?);
    assert!(matches!(
        dir.create_file(2, "value".parse()?, Data(vec![2])).await,
        Err(crate::Error::Conflict(txn_lock::Error::Conflict))
    ));

    dir.rollback(2).await?;

    assert!(dir.delete(5, "value".parse()?).await?);
    dir.create_file(5, "value".parse()?, Data(vec![5])).await?;
    dir.commit(5).await?;
    assert_eq!(file.read::<Data>(3).await?.0, [3]);
    dir.finalize(3).await?;
    assert!(matches!(
        file.read::<Data>(3).await,
        Err(crate::Error::Conflict(txn_lock::Error::Outdated))
    ));

    assert_eq!(file.read::<Data>(5).await?.0, [5]);
    dir.finalize(5).await?;
    assert_eq!(file.read::<Data>(5).await?.0, [5]);
    assert_eq!(file.read::<Data>(6).await?.0, [5]);
    dir.rollback(6).await?;
    Ok(())
}

#[tokio::test]
async fn canonical_selection_waits_for_publication_and_logical_commit() -> Result<()> {
    let path = TestDir::new()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;
    root.write()
        .await
        .create_file("value".into(), Data(vec![0]))
        .await?
        .sync_all()
        .await?;

    let dir = Dir::<u64, Data>::load(root.clone()).await?;
    let file = dir.get_file(1, &"value".parse()?).await?.unwrap().clone();
    dir.commit(1).await?;
    assert!(dir.delete(2, "value".parse()?).await?);
    dir.create_file(2, "value".parse()?, Data(vec![2])).await?;

    let parent = root.write().await;
    let mut commit = std::pin::pin!(dir.commit(2));
    assert!(poll!(&mut commit).is_pending());
    let mut read = std::pin::pin!(file.read::<Data>(1));
    assert!(poll!(&mut read).is_pending());
    drop(parent);

    tokio::time::timeout(Duration::from_secs(3), &mut commit)
        .await
        .unwrap()?;

    assert!(matches!(
        read.await,
        Err(crate::Error::Conflict(txn_lock::Error::Outdated))
    ));

    dir.commit(2).await?;
    assert_eq!(file.read::<Data>(2).await?.0, [2]);
    Ok(())
}

#[tokio::test]
async fn missing_pending_and_retained_file_versions_are_corruption() -> Result<()> {
    let path = TestDir::new()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root.clone()).await?;
    let file = dir.create_file(1, "value".parse()?, Data(vec![1])).await?;
    dir.commit(1).await?;
    let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
    let versions = versions.read().await.get_dir("value").unwrap().clone();
    assert!(dir.delete(2, "value".parse()?).await?);
    dir.create_file(2, "value".parse()?, Data(vec![2])).await?;
    assert!(versions.write().await.delete("2").await);
    assert!(matches!(
        file.read::<Data>(2).await,
        Err(crate::Error::Corrupt(_))
    ));

    dir.rollback(2).await?;
    assert!(dir.delete(3, "value".parse()?).await?);
    dir.create_file(3, "value".parse()?, Data(vec![3])).await?;
    dir.commit(3).await?;
    assert!(versions.write().await.delete("3").await);
    assert!(matches!(
        file.read::<Data>(3).await,
        Err(crate::Error::Corrupt(_))
    ));

    assert!(versions.write().await.delete("1").await);
    assert!(matches!(
        file.read::<Data>(1).await,
        Err(crate::Error::Corrupt(_))
    ));

    Ok(())
}

#[tokio::test]
async fn rollback_cleans_removed_and_replaced_file_owners() -> Result<()> {
    for operation in ["delete", "truncate", "replace"] {
        let path = TestDir::new()?;
        let name: crate::Id = "value".parse()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            let file = dir.create_file(1, name.clone(), Data(vec![1])).await?;
            let other = dir.create_file(1, "other".parse()?, Data(vec![9])).await?;
            dir.commit(1).await?;
            dir.finalize(1).await?;
            assert!(dir.delete(2, "value".parse()?).await?);
            dir.create_file(2, "value".parse()?, Data(vec![2])).await?;

            if operation == "truncate" {
                dir.clone().truncate(2).await?;
            } else {
                assert!(dir.delete(2, name.clone()).await?);
                if operation == "replace" {
                    dir.create_file(2, name.clone(), Data(vec![8])).await?;
                }
            }

            if operation != "truncate" {
                assert!(dir.delete(3, "other".parse()?).await?);
                dir.create_file(3, "other".parse()?, Data(vec![30])).await?;
            }

            dir.rollback(2).await?;
            dir.rollback(2).await?;
            let read =
                tokio::time::timeout(Duration::from_secs(3), dir.read_file::<Data>(2, &name))
                    .await
                    .unwrap()?;

            assert_eq!(read.0, [1], "{operation}");
            drop(read);
            let future = tokio::time::timeout(Duration::from_secs(3), file.read::<Data>(4))
                .await
                .unwrap()?;

            assert_eq!(future.0, [1], "future read after {operation}");
            drop(future);
            // A full-directory pending truncate excludes later membership work.
            if operation == "truncate" {
                assert!(dir.delete(3, "other".parse()?).await?);
                dir.create_file(3, "other".parse()?, Data(vec![30])).await?;
            }

            assert_eq!(other.read::<Data>(3).await?.0, [30]);
            let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
            let other_versions = versions.read().await.get_dir("other").unwrap().clone();
            assert!(other_versions.read().await.contains("3"));
            dir.rollback(3).await?;
            dir.finalize(2).await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        assert_eq!(dir.read_file::<Data>(4, &name).await?.0, [1]);
    }

    Ok(())
}

#[tokio::test]
async fn rollback_removes_created_then_deleted_file_stages() -> Result<()> {
    let path = TestDir::new()?;
    let name: crate::Id = "discarded".parse()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let file = dir.create_file(2, name.clone(), Data(vec![2])).await?;
        assert!(dir.delete(2, name.clone()).await?);
        dir.create_file(2, name.clone(), Data(vec![20])).await?;
        drop(file);
        assert!(dir.delete(2, name.clone()).await?);
        dir.rollback(2).await?;
        let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
        assert!(!versions.read().await.contains(name.as_str()));
        dir.rollback(2).await?;
        assert!(dir.is_empty(1).await?);
        root.sync_all().await?;
    }

    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        assert!(!dir.contains(3, &name).await?);
        dir.create_file(3, name.clone(), Data(vec![3])).await?;
        dir.commit(3).await?;
        dir.finalize(3).await?;
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root).await?;
    assert_eq!(dir.read_file::<Data>(4, &name).await?.0, [3]);
    Ok(())
}

#[tokio::test]
async fn replacement_preserves_historical_file_identity() -> Result<()> {
    let path = TestDir::new()?;
    let name: crate::Id = "value".parse()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        let retained = dir.create_file(1, name.clone(), Data(vec![1])).await?;
        dir.commit(1).await?;
        dir.finalize(1).await?;
        assert!(dir.delete(2, name.clone()).await?);
        dir.create_file(2, name.clone(), Data(vec![2])).await?;
        assert!(dir.delete(2, name.clone()).await?);
        dir.create_file(2, name.clone(), Data(vec![3])).await?;
        dir.commit(2).await?;
        assert_eq!(dir.read_file::<Data>(2, &name).await?.0, [3]);
        assert!(matches!(
            dir.read_file::<Data>(1, &name).await,
            Err(crate::Error::Conflict(txn_lock::Error::Outdated))
        ));

        dir.finalize(2).await?;
        assert!(dir.delete(3, name.clone()).await?);
        assert!(matches!(
            retained.read::<Data>(3).await,
            Err(crate::Error::NotFound(_))
        ));

        dir.rollback(3).await?;
        assert_eq!(retained.read::<Data>(3).await?.0, [3]);
        drop(dir);
        assert_eq!(retained.read::<Data>(3).await?.0, [3]);
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root).await?;
    assert_eq!(dir.read_file::<Data>(3, &name).await?.0, [3]);
    Ok(())
}

#[tokio::test]
async fn rollback_rejects_non_file_orphan_stages() -> Result<()> {
    let path = TestDir::new()?;
    let name: crate::Id = "discarded".parse()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root.clone()).await?;
    drop(dir.create_file(2, name.clone(), Data(vec![2])).await?);
    assert!(dir.delete(2, name.clone()).await?);
    let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
    let staged = versions
        .read()
        .await
        .get_dir(name.as_str())
        .unwrap()
        .clone();

    {
        let mut staged = staged.write().await;
        assert!(staged.delete("2").await);
        staged.sync_deleted().await?;
        staged.create_dir("2".into())?;
    }

    assert!(matches!(
        dir.rollback(2).await,
        Err(crate::Error::Corrupt(_))
    ));

    assert!(staged.read().await.get_dir("2").is_some());
    Ok(())
}

#[tokio::test]
async fn repeated_file_commit_preserves_newer_canonical_version() -> Result<()> {
    let path = TestDir::new()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root).await?;
        let file = dir.create_file(1, "value".parse()?, Data(vec![1])).await?;
        dir.commit(1).await?;
        assert!(dir.delete(2, "value".parse()?).await?);
        dir.create_file(2, "value".parse()?, Data(vec![2])).await?;
        dir.commit(2).await?;

        dir.commit(1).await?;
        dir.commit(1).await?;
        dir.commit(2).await?;
        dir.finalize(2).await?;
        dir.commit(2).await?;
        assert!(matches!(
            dir.commit(1).await,
            Err(crate::Error::Conflict(txn_lock::Error::Outdated))
        ));

        assert_eq!(file.read::<Data>(3).await?.0, [2]);
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;
    let reopened = Dir::<u64, Data>::load(root).await?;
    assert_eq!(
        reopened.read_file::<Data>(3, &"value".parse()?).await?.0,
        [2]
    );

    Ok(())
}

#[tokio::test]
async fn repeated_directory_commit_preserves_recreated_entry() -> Result<()> {
    for initially_file in [false, true] {
        let path = TestDir::new()?;
        let name: crate::Id = "child".parse()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            if initially_file {
                dir.create_file(1, name.clone(), Data(vec![1])).await?;
            } else {
                let child = dir.create_dir(1, name.clone()).await?;
                child
                    .write()
                    .await
                    .create_file("value".into(), Data(vec![1]))
                    .await?
                    .sync_all()
                    .await?;
            }

            dir.commit(1).await?;
            assert!(dir.delete(2, name.clone()).await?);
            dir.commit(2).await?;

            let replacement = dir.create_dir(3, name.clone()).await?;
            replacement
                .write()
                .await
                .create_file("value".into(), Data(vec![3]))
                .await?
                .sync_all()
                .await?;

            dir.commit(3).await?;
            dir.commit(2).await?;
            assert_eq!(
                replacement.read().await.read_file::<Data>("value").await?.0,
                [3]
            );

            dir.finalize(3).await?;
            let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
            assert!(!versions.read().await.contains(name.as_str()));
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(1)).load(path.0.clone())?;

        let reopened = Dir::<u64, Data>::load(root).await?;
        let child = reopened.get_dir(4, &name).await?.unwrap().clone();
        assert_eq!(child.read().await.read_file::<Data>("value").await?.0, [3]);
    }

    Ok(())
}

#[derive(Clone)]
struct Data(Vec<u8>);

impl get_size::GetSize for Data {
    fn get_size(&self) -> usize {
        self.0.capacity()
    }
}

impl safecast::AsType<Data> for Data {
    fn as_type(&self) -> Option<&Data> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut Data> {
        Some(self)
    }

    fn into_type(self) -> Option<Data> {
        Some(self)
    }
}

impl FileLoad for Data {
    async fn load(
        _: &std::path::Path,
        mut file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> io::Result<Self> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await?;
        Ok(Self(bytes))
    }
}

impl FileSave for Data {
    async fn save(&self, file: &mut tokio::fs::File) -> io::Result<u64> {
        let observer = SAVE_OBSERVER.with(|slot| slot.borrow().clone());
        if let Some(observer) = observer {
            observer.enter().await?;
        }

        file.write_all(&self.0).await?;
        Ok(self.0.len() as u64)
    }
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        for attempt in 0.. {
            let path = std::env::temp_dir().join(format!(
                "txfs_lifecycle_{}_{nonce}_{attempt}",
                std::process::id()
            ));

            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }

        unreachable!()
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn exercise(operation: &str, mode: &str) -> Result<()> {
    let path = TestDir::new()?;
    let root =
        Cache::<Data>::new(1024 * 1024, Some(2), 0, Duration::from_secs(3)).load(path.0.clone())?;

    let dir = Dir::<u64, Data>::load(root.clone()).await?;
    let mut files = Vec::new();
    for i in 0..5 {
        files.push(
            dir.create_file(1, format!("file-{i}").parse()?, Data(vec![1]))
                .await?,
        );
    }

    dir.commit(1).await?;
    for i in 0..files.len() {
        let name = format!("file-{i}").parse()?;
        assert!(dir.delete(2, name).await?);
        dir.create_file(2, format!("file-{i}").parse()?, Data(vec![2]))
            .await?;
    }

    let observer = Observation::new((mode == "error").then_some(1));
    OBSERVER.with(|slot| *slot.borrow_mut() = Some(observer.clone()));
    let _installed = Installed;
    let mut pending = Box::pin(async {
        match operation {
            "commit" => dir.commit(2).await,
            "rollback" => dir.rollback(2).await,
            "finalize" => dir.finalize(1).await,
            _ => unreachable!(),
        }
    });

    fn assert_send(_: &impl Send) {}
    assert_send(&pending);
    if mode == "error" {
        let error = pending.await.unwrap_err();
        let crate::Error::IO(error) = error else {
            panic!("expected the original I/O error");
        };

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "injected lifecycle error");
        assert_eq!(observer.started.load(Ordering::SeqCst), 2);
    } else {
        tokio::time::timeout(
            Duration::from_secs(3),
            std::future::poll_fn(|cx| match std::future::Future::poll(pending.as_mut(), cx) {
                std::task::Poll::Pending if observer.started.load(Ordering::SeqCst) >= 2 => {
                    std::task::Poll::Ready(())
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(result) => {
                    panic!("expected blocked lifecycle work: {result:?}")
                }
            }),
        )
        .await
        .unwrap();

        assert_eq!(observer.started.load(Ordering::SeqCst), 2);
        assert_eq!(observer.active.load(Ordering::SeqCst), 2);
        if mode == "cancel" {
            drop(pending);
        } else {
            observer.released.store(true, Ordering::SeqCst);
            observer.release.notify_waiters();
            tokio::time::timeout(Duration::from_secs(3), pending)
                .await
                .unwrap()?;

            // Each physical version directory is visited once. Native work is
            // bounded by two; there is no duplicate per-file metadata lifecycle.
            assert_eq!(observer.started.load(Ordering::SeqCst), 5);
            let txn = if operation == "finalize" { 2 } else { 3 };
            let expected = if operation == "rollback" { 1 } else { 2 };
            for file in &files {
                assert_eq!(file.read::<Data>(txn).await?.0, [expected]);
            }
        }
    }

    assert_eq!(observer.active.load(Ordering::SeqCst), 0);
    assert_eq!(observer.peak.load(Ordering::SeqCst), 2);
    drop(root.try_write()?);
    let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
    drop(versions.try_write()?);
    Ok(())
}

#[tokio::test]
async fn lifecycle_admission_is_bounded_and_completes() -> Result<()> {
    for operation in ["commit", "rollback", "finalize"] {
        exercise(operation, "complete").await?;
    }

    Ok(())
}

#[tokio::test]
async fn lifecycle_errors_release_siblings_without_admitting_more_work() -> Result<()> {
    for operation in ["commit", "rollback", "finalize"] {
        exercise(operation, "error").await?;
    }

    Ok(())
}

#[tokio::test]
async fn lifecycle_cancellation_releases_admitted_work() -> Result<()> {
    for operation in ["commit", "rollback", "finalize"] {
        exercise(operation, "cancel").await?;
    }

    Ok(())
}

#[tokio::test]
async fn obsolete_same_name_creation_rejects_before_native_effects() -> Result<()> {
    for directory in [false, true] {
        for deleted in [false, true] {
            let path = TestDir::new()?;
            let name: crate::Id = "value".parse()?;
            {
                let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                    .load(path.0.clone())?;

                let dir = Dir::<u64, Data>::load(root.clone()).await?;
                if deleted {
                    if directory {
                        dir.create_dir(1, name.clone()).await?.sync_all().await?;
                    } else {
                        dir.create_file(1, name.clone(), Data(vec![1])).await?;
                    }

                    dir.commit(1).await?;
                }

                assert_eq!(dir.contains(2, &name).await?, deleted);
                if deleted {
                    assert!(dir.delete(3, name.clone()).await?);
                } else if directory {
                    dir.create_dir(3, name.clone()).await?.sync_all().await?;
                } else {
                    dir.create_file(3, name.clone(), Data(vec![3])).await?;
                }

                dir.commit(3).await?;
                // Unrelated creation at an older ID remains supported.
                dir.create_file(2, "other".parse()?, Data(vec![2])).await?;
                if deleted {
                    assert!(dir.delete(2, name.clone()).await?);
                }

                let native_before: Vec<_> = root
                    .read()
                    .await
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect();

                let error = if directory {
                    dir.create_dir(2, name.clone()).await.err().unwrap()
                } else {
                    dir.create_file(2, name.clone(), Data(vec![2]))
                        .await
                        .err()
                        .unwrap()
                };

                assert!(matches!(
                    error,
                    crate::Error::Conflict(txn_lock::Error::Conflict)
                ));

                assert_eq!(
                    root.read()
                        .await
                        .iter()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>(),
                    native_before
                );

                let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
                if let Some(stages) = versions.read().await.get_dir(name.as_str()).cloned() {
                    assert!(!stages.read().await.contains(&2.to_string()));
                }

                dir.rollback(2).await?;
                assert_eq!(dir.contains(4, &name).await?, !deleted);
                if !deleted && !directory {
                    assert_eq!(dir.read_file::<Data>(4, &name).await?.0, vec![3]);
                }

                dir.finalize(3).await?;
                root.sync_all().await?;
            }

            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root).await?;
            assert_eq!(dir.contains(4, &name).await?, !deleted);
            if !deleted && !directory {
                assert_eq!(dir.read_file::<Data>(4, &name).await?.0, vec![3]);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn rollback_reclaims_created_files_and_cleaned_children_before_reopening() -> Result<()> {
    for kind in ["file", "directory", "child metadata"] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            if kind == "file" {
                dir.create_file(1, "value".parse()?, Data(vec![1])).await?;
            } else {
                let child = dir.create_dir(1, "value".parse()?).await?;
                if kind == "child metadata" {
                    let owner = Dir::<u64, Data>::load(child.clone()).await?;
                    owner.rollback(1).await?;
                }

                child.sync_all().await?;
            }

            dir.rollback(1).await?;
            dir.rollback(1).await?;
            assert!(dir.is_empty(0).await?);
            assert!(!root.read().await.contains("value"));
            let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
            assert!(versions.read().await.is_empty());
            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        assert!(dir.is_empty(2).await?);
        dir.create_file(3, "value".parse()?, Data(vec![3])).await?;
        dir.commit(3).await?;
        dir.finalize(3).await?;
        assert_eq!(dir.read_file::<Data>(3, &"value".parse()?).await?.0, [3]);
    }

    Ok(())
}

#[tokio::test]
async fn rollback_preserves_recipient_contents_and_reports_cleanup_errors() -> Result<()> {
    for malformed in [false, true] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            let child = dir.create_dir(1, "child".parse()?).await?;
            let name = if malformed { crate::VERSIONS } else { "native" };
            child
                .write()
                .await
                .create_file(name.into(), Data(vec![7]))
                .await?;

            child.sync_all().await?;
            let error = dir.rollback(1).await.unwrap_err();
            if malformed {
                assert!(matches!(error, crate::Error::Corrupt(_)));
            } else {
                let source = std::error::Error::source(&error)
                    .and_then(|cause| cause.downcast_ref::<io::Error>())
                    .unwrap();

                assert_eq!(source.kind(), io::ErrorKind::DirectoryNotEmpty);
            }

            assert!(!dir.contains(2, &"child".parse()?).await?);
            assert_eq!(child.read().await.read_file::<Data>(name).await?.0, [7]);
            assert_eq!(tokio::fs::read(path.0.join("child").join(name)).await?, [7]);
            // The recipient cleans its own contents before parent cleanup resumes.
            assert!(child.write().await.delete(name).await);
            child.sync_deleted().await?;
            dir.rollback(1).await?;
            dir.rollback(1).await?;
            assert!(!root.read().await.contains("child"));
            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        assert!(Dir::<u64, Data>::load(root).await?.is_empty(2).await?);
    }

    Ok(())
}

#[tokio::test]
async fn rollback_protects_retained_children_and_unrelated_versions() -> Result<()> {
    let path = TestDir::new()?;
    {
        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let stable = dir.create_dir(1, "stable".parse()?).await?;
        stable.sync_all().await?;
        let historical = dir
            .create_file(1, "history".parse()?, Data(vec![1]))
            .await?;

        dir.commit(1).await?;
        dir.create_dir(2, "discarded".parse()?).await?;
        dir.create_file(2, "temporary".parse()?, Data(vec![2]))
            .await?;

        let newer = dir.create_dir(3, "newer".parse()?).await?;
        newer.sync_all().await?;
        let future = dir.create_file(3, "future".parse()?, Data(vec![3])).await?;
        {
            // Protected native children need not release their own locks for rollback.
            let _stable = stable.write().await;
            let _newer = newer.write().await;
            tokio::time::timeout(Duration::from_secs(3), dir.rollback(2))
                .await
                .unwrap()?;
        }

        let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
        assert!(!versions.read().await.contains("temporary"));
        let retained = versions.read().await.get_dir("history").unwrap().clone();
        assert!(retained.read().await.contains("1"));
        let pending = versions.read().await.get_dir("future").unwrap().clone();
        assert!(pending.read().await.contains("3"));
        assert_eq!(historical.read::<Data>(1).await?.0, [1]);
        assert_eq!(future.read::<Data>(3).await?.0, [3]);
        assert!(dir.contains(3, &"newer".parse()?).await?);
        assert!(!dir.contains(3, &"discarded".parse()?).await?);
        dir.commit(3).await?;
        dir.rollback(2).await?;
        assert_eq!(future.read::<Data>(3).await?.0, [3]);
        assert!(dir.contains(3, &"stable".parse()?).await?);
        assert!(dir.contains(3, &"newer".parse()?).await?);
        dir.finalize(3).await?;
        root.sync_all().await?;
    }

    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root).await?;
    assert!(!dir.contains(4, &"temporary".parse()?).await?);
    assert!(!dir.contains(4, &"discarded".parse()?).await?);
    assert!(dir.contains(4, &"stable".parse()?).await?);
    assert!(dir.contains(4, &"newer".parse()?).await?);
    assert_eq!(dir.read_file::<Data>(4, &"history".parse()?).await?.0, [1]);
    assert_eq!(dir.read_file::<Data>(4, &"future".parse()?).await?.0, [3]);
    Ok(())
}

#[tokio::test]
async fn missing_file_reads_share_the_same_error_variant() -> Result<()> {
    let path = TestDir::new()?;
    let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;
    let dir = Dir::<u64, Data>::load(root).await?;
    let name: crate::Id = "value".parse()?;
    let file = dir.create_file(1, name.clone(), Data(vec![1])).await?;
    dir.commit(1).await?;
    assert!(dir.delete(2, name.clone()).await?);
    dir.commit(2).await?;
    for error in [
        file.read::<Data>(3).await.err().unwrap(),
        dir.read_file::<Data>(3, &name).await.err().unwrap(),
    ] {
        assert!(matches!(error, crate::Error::NotFound(ref missing) if missing == name.as_str()));
    }

    dir.finalize(2).await?;
    Ok(())
}

#[tokio::test]
async fn durable_publications_overlap_without_retaining_canonical_exclusion() -> Result<()> {
    for limit in [1, 2] {
        let path = TestDir::new()?;
        let root = Cache::<Data>::new(1024 * 1024, Some(limit), 0, Duration::from_secs(3))
            .load(path.0.clone())?;

        let dir = Dir::<u64, Data>::load(root.clone()).await?;
        let first = dir.create_file(1, "file-0".parse()?, Data(vec![0])).await?;
        for i in 1..3 {
            dir.create_file(1, format!("file-{i}").parse()?, Data(vec![i]))
                .await?;
        }

        let observer = Observation::new(None);
        SAVE_OBSERVER.with(|slot| *slot.borrow_mut() = Some(observer.clone()));
        let installed = InstalledSave;
        let mut commit = Box::pin(dir.commit(1));
        tokio::time::timeout(
            Duration::from_secs(3),
            std::future::poll_fn(|cx| match std::future::Future::poll(commit.as_mut(), cx) {
                std::task::Poll::Pending if observer.active.load(Ordering::SeqCst) == limit => {
                    std::task::Poll::Ready(())
                }
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(result) => panic!("publication must wait: {result:?}"),
            }),
        )
        .await
        .unwrap();

        assert_eq!(observer.started.load(Ordering::SeqCst), limit);
        assert!(!dir.entries.is_committed(1)?);
        drop(root.try_write()?);
        let versions = root.read().await.get_dir(crate::VERSIONS).unwrap().clone();
        assert!(versions.try_write().is_err());
        {
            let mut read = std::pin::pin!(first.read::<Data>(1));
            assert!(poll!(&mut read).is_pending());
        }

        observer.released.store(true, Ordering::SeqCst);
        observer.release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(3), commit)
            .await
            .unwrap()?;

        drop(installed);
        assert!(dir.entries.is_committed(1)?);
        assert_eq!(observer.started.load(Ordering::SeqCst), 3);
        assert_eq!(observer.peak.load(Ordering::SeqCst), limit);
        assert_eq!(observer.active.load(Ordering::SeqCst), 0);
        for i in 0..3 {
            assert_eq!(
                tokio::fs::read(path.0.join(format!("file-{i}"))).await?,
                [i]
            );
        }

        dir.finalize(1).await?;
    }

    Ok(())
}

#[tokio::test]
async fn cleanup_retries_pending_native_removals_without_new_entries() -> Result<()> {
    for operation in ["rollback", "finalize"] {
        let path = TestDir::new()?;
        {
            let root = Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3))
                .load(path.0.clone())?;

            let dir = Dir::<u64, Data>::load(root.clone()).await?;
            dir.create_dir(1, "child".parse()?)
                .await?
                .sync_all()
                .await?;

            let child = path.0.join("child");
            // Test-only filesystem fault: native removal must report its original cause.
            tokio::fs::remove_dir(&child).await?;
            tokio::fs::write(&child, [9]).await?;
            let expected = tokio::fs::remove_dir_all(&child).await.unwrap_err();
            let error = match operation {
                "rollback" => dir.rollback(1).await.unwrap_err(),
                "finalize" => dir.finalize(1).await.unwrap_err(),
                _ => unreachable!(),
            };

            let source = std::error::Error::source(&error)
                .and_then(|cause| cause.downcast_ref::<io::Error>())
                .unwrap();

            assert_eq!(source.kind(), expected.kind());
            assert_eq!(source.raw_os_error(), expected.raw_os_error());
            assert_eq!(tokio::fs::read(&child).await?, [9]);
            assert!(!root.read().await.contains("child"));

            tokio::fs::remove_file(&child).await?;
            tokio::fs::create_dir(&child).await?;
            match operation {
                "rollback" => dir.rollback(1).await?,
                "finalize" => dir.finalize(1).await?,
                _ => unreachable!(),
            }

            assert!(
                !child.exists(),
                "{operation} left a pending native deletion"
            );

            root.sync_all().await?;
        }

        let root =
            Cache::<Data>::new(4096, Some(1), 0, Duration::from_secs(3)).load(path.0.clone())?;

        assert!(Dir::<u64, Data>::load(root).await?.is_empty(2).await?);
    }

    Ok(())
}
