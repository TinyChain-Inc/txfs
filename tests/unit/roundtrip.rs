use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use freqfs::Cache;
use get_size::GetSize;
use safecast::{as_type, AsType};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct Txn(u64);

impl fmt::Display for Txn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Txn {
    type Err = std::num::ParseIntError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl PartialEq<str> for Txn {
    fn eq(&self, other: &str) -> bool {
        if let Ok(other) = other.parse() {
            self.0 == other
        } else {
            false
        }
    }
}

impl PartialOrd<str> for Txn {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        if let Ok(other) = other.parse() {
            PartialOrd::partial_cmp(&self.0, &other)
        } else {
            None
        }
    }
}

#[derive(Clone)]
enum Entry {
    Bin(Vec<u8>),
}

impl freqfs::FileLoad for Entry {
    async fn load(
        _path: &std::path::Path,
        mut file: tokio::fs::File,
        metadata: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        let len = usize::try_from(metadata.len()).map_err(std::io::Error::other)?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes).await?;
        let mut extra = [0];
        if file.read(&mut extra).await? != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file grew during load",
            ));
        }

        Ok(Self::Bin(bytes))
    }
}

impl freqfs::FileSave for Entry {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        match self {
            Self::Bin(bytes) => {
                file.write_all(bytes).await?;
                Ok(bytes.len() as u64)
            }
        }
    }
}

impl GetSize for Entry {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Bin(bytes) => bytes.get_heap_size(),
        }
    }
}

impl AsType<Entry> for Entry {
    fn as_type(&self) -> Option<&Entry> {
        Some(self)
    }

    fn as_type_mut(&mut self) -> Option<&mut Entry> {
        Some(self)
    }

    fn into_type(self) -> Option<Entry> {
        Some(self)
    }
}

as_type!(Entry, Bin, Vec<u8>);

#[tokio::test]
async fn file_roundtrip_persists_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    path.push(format!("txfs_test_{}_{}_λ", std::process::id(), unique));

    fs::create_dir(&path).await?;

    let cache = Cache::<Entry>::new(1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;
    let dir = super::Dir::load(root.clone()).await?;

    // Reserved names are caller input errors, not corruption of live storage.
    let reserved = super::VERSIONS.parse()?;
    assert!(matches!(
        dir.create_dir(Txn(1), reserved).await,
        Err(super::Error::IO(error)) if error.kind() == std::io::ErrorKind::InvalidInput
    ));

    let reserved = super::VERSIONS.parse()?;
    assert!(matches!(
        dir.create_file(Txn(1), reserved, Entry::Bin(vec![])).await,
        Err(super::Error::IO(error)) if error.kind() == std::io::ErrorKind::InvalidInput
    ));

    let name: super::Id = "file-one".parse()?;
    let file = dir
        .create_file(Txn(1), name.clone(), Entry::Bin(vec![1u8, 2, 3]))
        .await?;

    let read = file.read::<Entry>(Txn(1)).await?;
    match &*read {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[1u8, 2, 3]),
    }

    drop(read);

    dir.commit(Txn(1)).await?;
    assert!(dir.delete(Txn(2), name.clone()).await?);
    dir.create_file(Txn(2), name.clone(), Entry::Bin(vec![9u8, 8]))
        .await?;

    dir.commit(Txn(2)).await?;
    let rejected = dir.rollback(Txn(2)).await.unwrap_err();
    assert_eq!(
        std::error::Error::source(&rejected)
            .and_then(|cause| cause.downcast_ref::<txn_lock::Error>()),
        Some(&txn_lock::Error::Committed),
    );

    let read = file.read::<Entry>(Txn(3)).await?;
    match &*read {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[9u8, 8]),
    }

    drop(read);
    assert!(dir.delete(Txn(4), name.clone()).await?);
    dir.create_file(Txn(4), name.clone(), Entry::Bin(vec![4]))
        .await?;

    let native = root;
    let versions = native
        .read()
        .await
        .get_dir(super::VERSIONS)
        .unwrap()
        .clone();

    let versions = versions.read().await.get_dir("file-one").unwrap().clone();
    assert!(versions.write().await.delete("4").await);
    assert!(matches!(
        file.read::<Entry>(Txn(4)).await,
        Err(super::Error::Corrupt(_))
    ));

    dir.rollback(Txn(4)).await?;
    assert!(matches!(&*file.read::<Entry>(Txn(5)).await?, Entry::Bin(bytes) if bytes == &[9, 8]));

    drop(versions);
    drop(native);
    drop(file);
    drop(dir);

    fs::remove_dir_all(&path).await?;

    Ok(())
}

#[tokio::test]
async fn load_initializes_state_for_transactional_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    path.push(format!("txfs_load_test_{}_{}", std::process::id(), unique));
    fs::create_dir(&path).await?;
    fs::write(path.join("file-one"), [4u8, 5, 6]).await?;

    let cache = Cache::<Entry>::new(1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;
    let dir = super::Dir::<Txn, Entry>::load(root.clone()).await?;
    let versions = root.read().await.get_dir(super::VERSIONS).unwrap().clone();
    assert!(versions.read().await.is_empty());
    let name: super::Id = "file-one".parse()?;
    let entries = dir
        .iter(Txn(7))
        .await?
        .map(|(name, _)| (*name).clone())
        .collect::<Vec<_>>();

    assert_eq!(entries.as_slice(), std::slice::from_ref(&name));
    let file = dir
        .get_file(Txn(7), &name)
        .await?
        .expect("committed file")
        .clone();

    match &*file.read::<Entry>(Txn(7)).await? {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[4u8, 5, 6]),
    }

    assert!(versions.read().await.is_empty());

    assert!(dir.delete(Txn(8), name.clone()).await?);
    dir.create_file(Txn(8), name.clone(), Entry::Bin(vec![8]))
        .await?;

    assert!(versions.read().await.contains(name.as_str()));
    match &*file.read::<Entry>(Txn(7)).await? {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[4u8, 5, 6]),
    }

    dir.rollback(Txn(8)).await?;
    dir.rollback(Txn(8)).await?;

    assert!(dir.delete(Txn(9), name.clone()).await?);
    dir.create_file(Txn(9), name.clone(), Entry::Bin(vec![9]))
        .await?;

    dir.commit(Txn(9)).await?;
    dir.commit(Txn(9)).await?;
    match &*file.read::<Entry>(Txn(10)).await? {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[9]),
    }

    dir.finalize(Txn(9)).await?;
    dir.finalize(Txn(9)).await?;
    assert!(versions.read().await.is_empty());
    match &*file.read::<Entry>(Txn(10)).await? {
        Entry::Bin(bytes) => assert_eq!(bytes.as_slice(), &[9]),
    }

    fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn load_rejects_pending_state_without_deleting_it() -> Result<(), Box<dyn std::error::Error>>
{
    let mut path = std::env::temp_dir();
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    path.push(format!(
        "txfs_pending_test_{}_{}",
        std::process::id(),
        unique
    ));

    let child_path = path.join("child");
    let pending = child_path
        .join(super::dir::VERSIONS)
        .join("file-one")
        .join("5");

    fs::create_dir_all(pending.parent().expect("pending parent")).await?;
    fs::write(&pending, [9u8]).await?;

    let cache = Cache::<Entry>::new(1024, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;
    let dir = super::Dir::<Txn, Entry>::load(root).await?;
    let child = dir
        .get_dir(Txn(1), &"child".parse()?)
        .await?
        .expect("child")
        .clone();

    // Only the recipient interprets this child's unresolved versions.
    assert!(matches!(
        super::Dir::<Txn, Entry>::load(child).await,
        Err(super::Error::Corrupt(_))
    ));

    assert_eq!(fs::read(&pending).await?, [9u8]);

    fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn directory_membership_leaves_native_contents_to_the_recipient(
) -> Result<(), Box<dyn std::error::Error>> {
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!("txfs_members_{}_{unique}", std::process::id()));

    fs::create_dir(&path).await?;
    let cache = Cache::<Entry>::new(4096, None, 0, std::time::Duration::from_secs(3));
    let root = cache.load(path.clone())?;
    let dir = super::Dir::<Txn, Entry>::load(root.clone()).await?;
    let name: super::Id = "child".parse()?;
    let child = dir.create_dir(Txn(1), name.clone()).await?;
    assert!(!child.read().await.contains(super::VERSIONS));
    let file = child
        .write()
        .await
        .create_file("native".into(), Entry::Bin(vec![1, 2, 3]))
        .await?;

    file.sync_all().await?;

    // No parent lifecycle or loading operation may acquire this child's lock.
    {
        let _child = child.write().await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            dir.commit(Txn(1)).await?;
            dir.commit(Txn(1)).await?;
            dir.finalize(Txn(1)).await?;
            dir.finalize(Txn(1)).await?;
            let reopened = super::Dir::<Txn, Entry>::load(root.clone()).await?;
            assert!(reopened.get_dir(Txn(2), &name).await?.is_some());
            assert!(dir.delete(Txn(2), name.clone()).await?);
            dir.rollback(Txn(2)).await?;
            dir.truncate(Txn(3)).await?;
            dir.rollback(Txn(3)).await?;
            Ok::<_, super::Error>(())
        })
        .await??;
    }

    assert!(!child.read().await.contains(super::VERSIONS));
    assert_eq!(fs::read(path.join("child/native")).await?, [1, 2, 3]);
    assert!(dir.get_dir(Txn(4), &name).await?.is_some());
    assert!(dir.delete(Txn(4), name.clone()).await?);
    dir.commit(Txn(4)).await?;
    dir.commit(Txn(4)).await?;
    dir.finalize(Txn(4)).await?;
    assert!(!path.join("child").exists());

    let replacement = dir.create_dir(Txn(5), name.clone()).await?;
    assert!(replacement.read().await.is_empty());
    replacement
        .write()
        .await
        .create_file("native".into(), Entry::Bin(vec![4]))
        .await?
        .sync_all()
        .await?;

    dir.commit(Txn(5)).await?;
    dir.finalize(Txn(5)).await?;
    let reopened = super::Dir::<Txn, Entry>::load(root).await?;
    assert!(reopened.get_dir(Txn(6), &name).await?.is_some());
    assert_eq!(fs::read(path.join("child/native")).await?, [4]);
    fs::remove_dir_all(path).await?;
    Ok(())
}

#[tokio::test]
async fn historical_cleanup_does_not_recreate_a_deleted_subtree(
) -> Result<(), Box<dyn std::error::Error>> {
    for cleanup in ["directory finalize", "directory rollback"] {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path =
            std::env::temp_dir().join(format!("txfs_retired_{}_{}", std::process::id(), unique));

        fs::create_dir(&path).await?;
        let cache = Cache::<Entry>::new(4096, None, 0, std::time::Duration::from_secs(3));
        let parent = super::Dir::<Txn, Entry>::load(cache.load(path.clone())?).await?;
        let name: super::Id = "child".parse()?;
        let child = super::Dir::load(parent.create_dir(Txn(1), name.clone()).await?).await?;
        let _file = child
            .create_file(Txn(1), "data".parse()?, Entry::Bin(vec![1]))
            .await?;

        child.commit(Txn(1)).await?;
        parent.commit(Txn(1)).await?;
        if cleanup == "directory rollback" {
            assert!(child.delete(Txn(2), "data".parse()?).await?);
            child
                .create_file(Txn(2), "data".parse()?, Entry::Bin(vec![2]))
                .await?;
        }

        assert!(parent.delete(Txn(3), name.clone()).await?);
        parent.commit(Txn(3)).await?;
        assert!(!path.join("child").exists());

        // Historical owners can still reach their cutoff after their parent
        // has committed deletion. Cleanup must not publish storage again.
        match cleanup {
            "directory finalize" => child.finalize(Txn(1)).await?,
            "directory rollback" => child.rollback(Txn(2)).await?,
            _ => unreachable!(),
        }

        assert!(
            !path.join("child").exists(),
            "{cleanup} recreated its subtree"
        );

        let reopened_cache = Cache::<Entry>::new(4096, None, 0, std::time::Duration::from_secs(3));

        let reopened = super::Dir::<Txn, Entry>::load(reopened_cache.load(path.clone())?).await?;

        assert!(reopened.get_dir(Txn(4), &name).await?.is_none());
        fs::remove_dir_all(path).await?;
    }

    Ok(())
}
