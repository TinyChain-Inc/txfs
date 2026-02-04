use std::hash::Hash;
use std::ops::{Deref, DerefMut};
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
    _modified: TxnLockReadGuard<TxnId>,
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
    _modified: TxnLockWriteGuard<TxnId>,
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
    last_modified: TxnLock<TxnId, TxnId>,
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
    TxnId: Name + fmt::Display + fmt::Debug + Hash + Ord + Copy,
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
            versions.create_file(txn_id.to_string(), version, size)?;
        }

        Ok(Self {
            last_modified: TxnLock::new(txn_id),
            versions,
            parent,
            name: Arc::new(name),
        })
    }

    pub(super) async fn load(
        txn_id: TxnId,
        name: Id,
        parent: DirLock<FE>,
        versions: DirLock<FE>,
    ) -> Result<Self> {
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

            let canon = parent.get_file(&name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "cannot load the transactional file {} without a canonical version",
                        name
                    ),
                )
            })?;

            #[cfg(feature = "logging")]
            log::trace!("acquiring write lock on versions dir for file {name}...");

            let mut versions = versions.write().await;

            #[cfg(feature = "logging")]
            log::trace!("truncate obsolete versions of {name}...");
            versions.truncate().await;

            versions.copy_file_from(txn_id.to_string(), &canon).await?;

            #[cfg(feature = "logging")]
            log::trace!("copied canonical version of {:?}", canon);
        }

        Ok(Self {
            last_modified: TxnLock::new(txn_id),
            versions,
            parent,
            name: Arc::new(name),
        })
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: Name + fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: Send + Sync,
{
    /// Lock this file for reading at the given `txn_id`.
    pub async fn read<F>(&self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        let last_modified = self.last_modified.read(txn_id).await?;
        let versions = self.versions.read().await;
        let version = versions.read_file_owned(&*last_modified).await?;

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
        let mut versions = self.versions.write().await;

        let version = if last_modified < txn_id {
            let canon = versions.read_file_owned(&*last_modified).await?;
            *last_modified = txn_id;

            let version = F::clone(&*canon);
            let size = version.get_size();

            versions.create_file(txn_id.to_string(), version, size)?
        } else if last_modified == txn_id {
            versions.get_file(&*last_modified).expect("version").clone()
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
    TxnId: Name + Hash + Ord + PartialOrd<str> + fmt::Debug + Copy + Send + Sync,
    FE: FileSave + Send + Sync,
{
    /// Commit the state of this file at `txn_id`.
    /// This will un-block any pending future write locks.
    /// If this file was modified at `txn_id`, it will replace the canonical version with
    /// the modified version and sync with the host filesystem.
    pub async fn commit(&self, txn_id: TxnId)
    where
        FE: Clone,
    {
        let last_modified = self.last_modified.read_and_commit(txn_id).await;

        if &*last_modified == &txn_id {
            let versions = self.versions.read().await;
            if let DirEntry::File(file) = versions.get(&txn_id).expect("version") {
                let mut parent = self.parent.write().await;

                let canon = parent
                    .copy_file_from(self.name.to_string(), file)
                    .await
                    .expect("copy canonical version");

                canon
                    .sync()
                    .await
                    .expect("sync canonical version with the filesystem");
            } else {
                unreachable!("transactional file out of sync with filesystem");
            }
        }
    }

    pub async fn rollback(&self, txn_id: TxnId) {
        let last_modified = self.last_modified.read_and_rollback(txn_id).await;

        if &*last_modified == &txn_id {
            let mut versions = self.versions.write().await;
            versions.delete(&txn_id).await;
        }
    }

    pub async fn finalize(&self, txn_id: TxnId) {
        if let Some(last_modified) = self.last_modified.read_and_finalize(txn_id) {
            let mut versions = self.versions.write().await;

            let to_delete = versions
                .names()
                .filter(|version_id| *last_modified >= *version_id.as_str())
                .cloned()
                .collect::<Vec<_>>();

            for version_id in to_delete {
                versions.delete(&version_id).await;
            }
        }
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
