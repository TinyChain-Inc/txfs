use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, io};

use freqfs::*;
use get_size::GetSize;
use hr_id::Id;
use safecast::AsType;
use txn_lock::scalar::{TxnLock, TxnLockReadGuard, TxnLockWriteGuard};

use super::{Error, Result};

/// A read guard on a version of a transactional [`File`]
pub struct FileVersionRead<TxnId, FE, F> {
    _modified: TxnLockReadGuard<Option<TxnId>>,
    version: FileReadGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionRead<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.version.deref()
    }
}

/// A write guard on a version of a transactional [`File`]
pub struct FileVersionWrite<TxnId, FE, F> {
    _modified: TxnLockWriteGuard<Option<TxnId>>,
    version: FileWriteGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionWrite<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.version.deref()
    }
}

impl<TxnId, FE, F> DerefMut for FileVersionWrite<TxnId, FE, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.version.deref_mut()
    }
}

/// A transactional file
pub struct File<TxnId, FE> {
    last_modified: TxnLock<TxnId, Option<TxnId>>,
    versions: DirLock<FE>,
    parent: DirLock<FE>,
    name: Arc<Id>,
}

impl<TxnId, FE> Clone for File<TxnId, FE> {
    fn clone(&self) -> Self {
        Self {
            last_modified: self.last_modified.clone(),
            versions: self.versions.clone(),
            parent: self.parent.clone(),
            name: self.name.clone(),
        }
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: Clone + Send + Sync,
{
    pub(super) async fn create<F>(
        txn_id: TxnId,
        name: Id,
        parent: DirLock<FE>,
        versions: DirLock<FE>,
        version: F,
    ) -> Result<Self>
    where
        FE: AsType<F>,
        F: GetSize,
    {
        debug_assert!(versions
            .try_read()
            .expect("version dir")
            .path()
            .to_str()
            .expect("path")
            .ends_with(name.as_str()));

        {
            let size = version.get_size();
            let mut versions = versions.write().await;
            versions
                .create_file(txn_id.to_string(), version, size)
                .await?;
        }

        Ok(Self {
            last_modified: TxnLock::new(Some(txn_id)),
            versions,
            parent,
            name: Arc::new(name),
        })
    }

    pub(super) async fn load(name: Id, parent: DirLock<FE>, versions: DirLock<FE>) -> Result<Self> {
        #[cfg(feature = "logging")]
        log::debug!("load file {} into the transactional filesystem cache", name);

        debug_assert!(versions
            .try_read()
            .expect("version dir")
            .path()
            .to_str()
            .expect("path")
            .ends_with(name.as_str()));

        {
            let parent = parent.try_read().map_err(Error::from)?;

            let _canon = parent.get_file(&name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "cannot load the transactional file {} without a canonical version",
                        name
                    ),
                )
            })?;
        }

        if !versions.read().await.is_empty() {
            return Err(Error::Corrupt(format!(
                "unresolved transactional versions for {name}"
            )));
        }

        Ok(Self {
            last_modified: TxnLock::new(None),
            versions,
            parent,
            name: Arc::new(name),
        })
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: Send + Sync,
{
    /// Lock this file for reading at the given `txn_id`.
    pub async fn read<F>(&self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        let last_modified = self.last_modified.read(txn_id).await?;
        let staged = {
            let versions = self.versions.read().await;
            last_modified
                .as_ref()
                .and_then(|version| versions.get_file(&version.to_string()).cloned())
        };
        let version = if let Some(staged) = staged {
            staged.read_owned::<F>().await?
        } else {
            let canonical = {
                let parent = self.parent.read().await;
                parent.get_file(&*self.name).cloned().ok_or_else(|| {
                    Error::Corrupt(format!("missing canonical version for {}", self.name))
                })?
            };
            canonical.read_owned::<F>().await?
        };

        Ok(FileVersionRead {
            _modified: last_modified,
            version,
        })
    }

    /// Lock this file for reading at the given `txn_id` without borrowing.
    pub async fn into_read<F>(self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        self.read(txn_id).await
    }

    /// Lock this file for writing at the given `txn_id`.
    pub async fn write<F>(&self, txn_id: TxnId) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        F: FileLoad + Clone + GetSize,
        FE: AsType<F>,
    {
        let mut last_modified = self.last_modified.write(txn_id).await?;
        let version = if last_modified
            .as_ref()
            .is_none_or(|modified| *modified < txn_id)
        {
            let staged = {
                let versions = self.versions.read().await;
                last_modified
                    .as_ref()
                    .and_then(|version| versions.get_file(&version.to_string()).cloned())
            };
            let canon = if let Some(staged) = staged {
                staged.read_owned::<F>().await?
            } else {
                let canonical = {
                    let parent = self.parent.read().await;
                    parent.get_file(&*self.name).cloned().ok_or_else(|| {
                        Error::Corrupt(format!("missing canonical version for {}", self.name))
                    })?
                };
                canonical.read_owned::<F>().await?
            };
            *last_modified = Some(txn_id);
            let version = F::clone(&*canon);
            let size = version.get_size();
            let mut versions = self.versions.write().await;
            versions
                .create_file(txn_id.to_string(), version, size)
                .await?
        } else if *last_modified == Some(txn_id) {
            let versions = self.versions.read().await;
            versions
                .get_file(&txn_id.to_string())
                .expect("version")
                .clone()
        } else {
            return Err(txn_lock::Error::Outdated.into());
        };

        Ok(FileVersionWrite {
            _modified: last_modified,
            version: version.write_owned().await?,
        })
    }

    /// Lock this file for writing at the given `txn_id` without borrowing.
    pub async fn into_write<F>(self, txn_id: TxnId) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        F: FileLoad + Clone + GetSize,
        FE: AsType<F>,
    {
        self.write(txn_id).await
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: Hash + Ord + fmt::Display + fmt::Debug + Copy + Send + Sync,
    FE: FileSave + Clone + Send + Sync,
{
    /// Commit the state of this file at `txn_id`.
    /// This will un-block any pending future write locks.
    /// If this file was modified at `txn_id`, it will replace the canonical version with
    /// the modified version and sync with the host filesystem.
    pub async fn commit(&self, txn_id: TxnId) -> Result<()> {
        let last_modified = self.last_modified.read(txn_id).await?;

        if *last_modified == Some(txn_id) {
            let staged = self
                .versions
                .read()
                .await
                .get_file(&txn_id.to_string())
                .cloned()
                .ok_or_else(|| {
                    Error::Corrupt(format!(
                        "missing staged version {txn_id:?} for {}",
                        self.name
                    ))
                })?;
            let mut parent = self.parent.write().await;
            let canon = parent
                .copy_file_from(self.name.to_string(), &staged)
                .await?;
            canon.sync_all().await?;
        }
        drop(last_modified);
        self.last_modified.read_and_commit(txn_id).await;
        Ok(())
    }

    pub async fn rollback(&self, txn_id: TxnId) -> Result<()> {
        let last_modified = self.last_modified.read_and_rollback(txn_id).await;

        if *last_modified == Some(txn_id) {
            let mut versions = self.versions.write().await;
            versions.delete(&txn_id.to_string()).await;
            if let Err(error) = versions.sync().await {
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: Hash + Ord + FromStr + fmt::Display + fmt::Debug + Copy + Send + Sync,
    FE: FileSave + Clone + Send + Sync,
{
    pub async fn finalize(&self, txn_id: TxnId) -> Result<()> {
        if let Some(last_modified) = self.last_modified.read_and_finalize(txn_id) {
            let mut versions = self.versions.write().await;
            let mut to_delete = Vec::new();
            for version_id in versions.names() {
                let parsed = version_id.parse::<TxnId>().map_err(|_| {
                    Error::Corrupt(format!("invalid staged transaction version {version_id}"))
                })?;
                if last_modified
                    .as_ref()
                    .is_some_and(|modified| modified >= parsed)
                {
                    to_delete.push(version_id.clone());
                }
            }
            for version_id in to_delete {
                versions.delete(&version_id).await;
            }
            if let Err(error) = versions.sync().await {
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}

impl<TxnId, FE> fmt::Debug for File<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        #[cfg(debug_assertions)]
        write!(f, "transactional file {}", self.name)?;

        #[cfg(not(debug_assertions))]
        f.write_str("transactional file")?;

        Ok(())
    }
}
