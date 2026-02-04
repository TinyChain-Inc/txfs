use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::pin::Pin;
use std::{fmt, io};

use freqfs::{DirLock, FileLoad, FileSave, Name};
use futures::future::{self, try_join_all, Future, TryFutureExt};
use futures::stream::{self, FuturesUnordered, Stream, StreamExt};
use get_size::GetSize;
use hr_id::Id;
use safecast::AsType;
use txn_lock::map::{
    Entry as TxnMapEntry, Iter, TxnMapLock, TxnMapValueReadGuard, TxnMapValueReadGuardMap,
};

use super::file::*;
use super::{Error, Result};

/// The name of an entry in a [`Dir`], used to avoid unnecessary allocations
pub type Key = txn_lock::map::Key<Id>;

/// The name of the directory where un-committed file versions are cached
pub const VERSIONS: &str = ".txfs";

/// An entry in a [`Dir`]
pub enum DirEntry<TxnId, FE> {
    Dir(Dir<TxnId, FE>),
    File(File<TxnId, FE>),
}

impl<TxnId, FE> Clone for DirEntry<TxnId, FE> {
    fn clone(&self) -> Self {
        match self {
            Self::Dir(dir) => Self::Dir(dir.clone()),
            Self::File(file) => Self::File(file.clone()),
        }
    }
}

impl<TxnId, FE> DirEntry<TxnId, FE> {
    /// Return `true` if this [`DirEntry`] is itself a [`Dir`].
    pub fn is_dir(&self) -> bool {
        match self {
            Self::Dir(_) => true,
            _ => false,
        }
    }

    /// Return `true` if this [`DirEntry`] is a [`File`].
    fn is_file(&self) -> bool {
        match self {
            Self::File(_) => true,
            _ => false,
        }
    }
}

impl<TxnId, FE> fmt::Debug for DirEntry<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Dir(dir) => dir.fmt(f),
            Self::File(file) => file.fmt(f),
        }
    }
}

/// A transactional directory
pub struct Dir<TxnId, FE> {
    canon: DirLock<FE>,
    versions: DirLock<FE>,
    entries: TxnMapLock<TxnId, Id, DirEntry<TxnId, FE>>,
}

impl<TxnId, FE> Clone for Dir<TxnId, FE> {
    fn clone(&self) -> Self {
        Self {
            canon: self.canon.clone(),
            versions: self.versions.clone(),
            entries: self.entries.clone(),
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    FE: Send + Sync,
{
    /// Destructure this [`Dir`] into its underlying [`DirLock`].
    /// The caller of this method must implement transactional state management explicitly.
    pub fn into_inner(self) -> DirLock<FE> {
        debug_assert!(self.canon.try_read().expect("canon").contains(VERSIONS));
        self.canon
    }
}

impl<TxnId: Copy + Hash + Eq + Ord + fmt::Debug, FE> Dir<TxnId, FE> {
    /// Return `true` if there is at least one [`File`] in this [`Dir`] at `txn_id`.
    pub async fn contains_files(&self, txn_id: TxnId) -> Result<bool> {
        let entries = self.entries.iter(txn_id).await?;

        for (_, entry) in entries {
            if entry.is_file() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Return the number of entries in this [`Dir`] as of the given `txn_id`.
    pub async fn len(&self, txn_id: TxnId) -> Result<usize> {
        self.entries.len(txn_id).map_err(Error::from).await
    }

    /// Return `true` if this [`Dir`] is empty at the given `txn_id`.
    pub async fn is_empty(&self, txn_id: TxnId) -> Result<bool> {
        self.entries.is_empty(txn_id).map_err(Error::from).await
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + Hash + Ord + Copy + fmt::Display + fmt::Debug + Send + Sync + 'static,
    FE: FileSave + Clone,
{
    /// Load a transactional [`Dir`] from a [`DirLock`].
    pub fn load(
        txn_id: TxnId,
        canon: DirLock<FE>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + Send>> {
        #[cfg(feature = "log")]
        log::debug!("load transactional dir from {:?}", canon);

        Box::pin(async move {
            let (contents, versions) = {
                #[cfg(feature = "log")]
                log::trace!("lock canonical dir for writing");

                let versions = {
                    let mut dir = canon.write().await;
                    dir.get_or_create_dir(VERSIONS.to_string())?
                };

                let contents = {
                    #[cfg(feature = "log")]
                    log::trace!("lock version dir for writing");

                    let mut versions = versions.write().await;

                    #[cfg(feature = "logging")]
                    log::trace!("truncating {} past versions...", versions.len());
                    versions.truncate().await;
                    versions.sync().await?;

                    let mut contents = HashMap::new();

                    for (name, entry) in canon.try_read()?.iter() {
                        let name: Id = if name.starts_with('.') {
                            #[cfg(feature = "logging")]
                            log::trace!("skipping hidden dir entry {name}");
                            continue;
                        } else {
                            name.parse()?
                        };

                        let entry = match entry.clone() {
                            freqfs::DirEntry::Dir(dir) => {
                                #[cfg(feature = "log")]
                                log::trace!("load sub-dir {}: {:?}", name, dir);
                                Self::load(txn_id, dir).map_ok(DirEntry::Dir).await?
                            }
                            freqfs::DirEntry::File(_file) => {
                                #[cfg(debug_assertions)]
                                if !_file.path().exists() {
                                    #[cfg(feature = "log")]
                                    log::warn!("there is no file at {}", _file.path().display());
                                }

                                #[cfg(feature = "log")]
                                log::trace!("load file {}: {:?}", name, _file);

                                let file_versions = versions.get_or_create_dir(name.to_string())?;

                                #[cfg(feature = "log")]
                                log::trace!("created versions dir for file {}: {:?}", name, _file);

                                File::load(txn_id, name.clone(), canon.clone(), file_versions)
                                    .map_ok(DirEntry::File)
                                    .await?
                            }
                        };

                        contents.insert(name, entry);
                    }

                    contents
                };

                (contents, versions)
            };

            Ok(Self {
                canon,
                versions,
                entries: TxnMapLock::with_contents(txn_id, contents),
            })
        })
    }

    /// Create a new [`Dir`] with the given `name` at `txn_id`.
    pub async fn create_dir(&self, txn_id: TxnId, name: Id) -> Result<Self> {
        #[cfg(feature = "logging")]
        log::trace!("Dir::create_dir {name}");

        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        let mut canon = self.canon.write().await;

        let sub_dir = canon.get_or_create_dir(name.to_string())?;
        let sub_dir = Self::load(txn_id, sub_dir).await?;

        entry.insert(DirEntry::Dir(sub_dir.clone()));

        Ok(sub_dir)
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + Hash + Ord + Copy + fmt::Display + fmt::Debug + Send + Sync + 'static,
    FE: Clone + Send + Sync + 'static,
{
    /// Return `true` if this [`Dir`] has an entry at the given `name` at `txn_id`.
    pub async fn contains(&self, txn_id: TxnId, name: &Id) -> Result<bool> {
        self.entries
            .contains_key(txn_id, name)
            .map_err(Error::from)
            .await
    }

    /// Delete the entry at `name` at `txn_id` and return `true` if it was present.
    pub async fn delete(&self, txn_id: TxnId, name: Id) -> Result<bool> {
        if let Some(entry) = self.entries.remove(txn_id, &name).await? {
            if let DirEntry::Dir(dir) = &*entry {
                dir.clone().truncate(txn_id).await?;
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Construct an iterator over the names of the sub-directories in this [`Dir`] at `txn_id`.
    pub async fn dir_names(&self, txn_id: TxnId) -> Result<impl Iterator<Item = Key>> {
        let iterator = self.entries.iter(txn_id).await?;
        Ok(iterator.filter_map(|(name, entry)| if entry.is_dir() { Some(name) } else { None }))
    }

    /// Construct an iterator over the names of the files in this [`Dir`] at `txn_id`.
    pub async fn file_names(&self, txn_id: TxnId) -> Result<impl Iterator<Item = Key>> {
        let iterator = self.entries.iter(txn_id).await?;
        Ok(iterator.filter_map(|(name, entry)| if entry.is_file() { Some(name) } else { None }))
    }

    /// Construct an iterator over the contents of the files in this [`Dir`] at `txn_id`.
    pub async fn files<F>(
        &self,
        txn_id: TxnId,
    ) -> Result<impl Stream<Item = Result<(Key, FileVersionRead<TxnId, FE, F>)>> + Send + Unpin + '_>
    where
        FE: AsType<F>,
        F: FileLoad,
    {
        let entries = self.entries.iter(txn_id).await?;
        let files = entries.filter_map(|(name, entry)| match &*entry {
            DirEntry::File(file) => Some((name, file.clone())),
            _ => None,
        });

        let files = stream::iter(files)
            .then(move |(name, file)| file.into_read(txn_id).map_ok(|file| (name, file)));

        Ok(Box::pin(files))
    }

    /// Construct an iterator over the contents of this [`Dir`] at `txn_id`.
    pub async fn iter(&self, txn_id: TxnId) -> Result<Iter<TxnId, Id, DirEntry<TxnId, FE>>> {
        self.entries.iter(txn_id).map_err(Error::from).await
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id`.
    pub async fn get_dir(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, Self>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            expect_dir(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id` synchronously, if possible.
    pub fn try_get_dir(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, Self>>> {
        if let Some(entry) = self.entries.try_get(txn_id, name).map_err(Error::from)? {
            expect_dir(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Delete the contents of this [`Dir`] at `txn_id`.
    pub fn truncate(self, txn_id: TxnId) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        Box::pin(async move {
            let entries = self.entries.clear(txn_id).map_err(Error::from).await?;

            let truncates = entries
                .into_iter()
                .filter_map(|(_name, entry)| {
                    if let DirEntry::Dir(dir) = &*entry {
                        Some(dir.clone())
                    } else {
                        None
                    }
                })
                .map(move |dir| dir.truncate(txn_id));

            try_join_all(truncates).map_ok(|_| ()).await
        })
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: Clone + Send + Sync,
{
    /// Create a new [`File`] with the given `name`, `contents` at `txn_id`.
    pub async fn create_file<F>(
        &self,
        txn_id: TxnId,
        name: Id,
        contents: F,
    ) -> Result<File<TxnId, FE>>
    where
        FE: AsType<F>,
        F: GetSize + Clone,
    {
        #[cfg(feature = "logging")]
        log::trace!("Dir::create_file {name}");

        // this write permit ensures that there is no other pending entry with this name
        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        let versions = {
            let mut versions = self.versions.write().await;
            versions.get_or_create_dir(name.to_string())?
        };

        let file = File::create(txn_id, name, self.canon.clone(), versions, contents).await?;

        entry.insert(DirEntry::File(file.clone()));

        Ok(file)
    }

    /// Get a [`File`] present in this [`Dir`] at the given `txn_id`.
    pub async fn get_file(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            expect_file(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a [`File`] present in this [`Dir`] at the given `txn_id` synchronously, if possible.
    pub fn try_get_file(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.try_get(txn_id, name).map_err(Error::from)? {
            expect_file(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Convenience method to lock a file in this [`Dir`] for reading at the given `txn_id`.
    /// This returns an owned read guard or an error if the file is not found.
    pub async fn read_file<F>(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        F: FileLoad,
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.read(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }

    /// Convenience method to lock a file in this [`Dir`] for writing at the given `txn_id`.
    /// This returns an owned write guard or an error if the file is not found.
    pub async fn write_file<F>(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        F: FileLoad + GetSize + Clone,
        FE: FileSave + AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.write(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + PartialOrd<str> + Hash + Copy + Ord + fmt::Debug + Send + Sync,
    FE: FileSave + Clone,
{
    /// Commit the state of this [`Dir`] at `txn_id`.
    pub fn commit<'a>(
        &'a self,
        txn_id: TxnId,
        recursive: bool,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            #[cfg(feature = "logging")]
            log::trace!("Dir::commit, recursive={recursive}");

            let (contents, deltas) = self.entries.read_and_commit(txn_id).await;

            if recursive {
                let commits = FuturesUnordered::new();

                for (_name, entry) in &contents {
                    #[cfg(feature = "logging")]
                    log::trace!("Dir::commit {}: {:?}", _name, entry);

                    let entry = DirEntry::clone(&*entry);

                    commits.push(async move {
                        match entry {
                            DirEntry::Dir(dir) => dir.commit(txn_id, recursive).await,
                            DirEntry::File(file) => file.commit(txn_id).await,
                        }
                    });
                }

                commits.fold((), |(), ()| future::ready(())).await;
            }

            let mut needs_sync = false;
            if let Some(deltas) = deltas {
                let mut canon = self.canon.write().await;

                for (name, entry) in deltas {
                    if entry.is_none() {
                        assert!(!contents.contains_key(&*name));

                        if let Some(entry) = canon.get(&*name) {
                            needs_sync = needs_sync || entry.is_file();
                        }

                        canon.delete(&*name).await;
                    }
                }
            };

            if needs_sync {
                // remove the canonical version of any file that was deleted in this transaction
                self.canon.sync().await.expect("sync");
            }
        })
    }

    /// Roll back the state of this [`Dir`] at `txn_id`.
    pub fn rollback<'a>(
        &'a self,
        txn_id: TxnId,
        recursive: bool,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let (contents, _deltas) = self.entries.read_and_rollback(txn_id).await;

            if recursive {
                let rollbacks = FuturesUnordered::new();

                for (_name, entry) in contents {
                    let entry = DirEntry::clone(&*entry);

                    rollbacks.push(async move {
                        match entry {
                            DirEntry::Dir(dir) => dir.rollback(txn_id, recursive).await,
                            DirEntry::File(file) => file.rollback(txn_id).await,
                        }
                    });
                }

                rollbacks.fold((), |(), ()| future::ready(())).await;
            }
        })
    }

    /// Finalize the state of this [`Dir`] at `txn_id`.
    pub async fn finalize(&self, txn_id: TxnId) {
        let mut sync_canon = false;

        if let Some(entries) = self.entries.read_and_finalize(txn_id) {
            let names = entries
                .into_iter()
                .map(|(name, _)| name.to_string())
                .collect::<HashSet<_>>();

            let delete_versions = {
                let mut versions = self.versions.write().await;
                let mut to_delete = Vec::with_capacity(versions.len());

                for (name, entry) in versions.iter() {
                    if names.contains(name) || name.starts_with('.') {
                        continue;
                    }

                    // this assumes that a file's version directory will only be empty
                    // after the last file version has been finalized
                    // and before any new version has been created
                    if let freqfs::DirEntry::Dir(dir) = entry {
                        let dir = dir.read().await;
                        if dir.is_empty() {
                            to_delete.push(name.to_string());
                        }
                    }
                }

                for name in to_delete {
                    versions.delete(name.as_str()).await;
                }

                versions.is_empty()
            };

            let mut canon = self.canon.write().await;
            let mut to_delete = Vec::with_capacity(canon.len());

            for (name, entry) in canon.iter() {
                if names.contains(name) || name.starts_with('.') {
                    continue;
                }

                // this assumes that a directory will be empty after all its files are deleted
                if let freqfs::DirEntry::Dir(dir) = entry {
                    let dir = dir.read().await;
                    if dir.is_empty() {
                        to_delete.push(name.clone());
                        sync_canon = true;
                    }
                }
            }

            for name in to_delete {
                canon.delete(&name).await;
            }

            if delete_versions {
                canon.delete(VERSIONS).await;
                sync_canon = true;
            }
        }

        if sync_canon {
            self.canon.sync().await.expect("sync canonical directory");
        }
    }
}

impl<TxnId, FE> fmt::Debug for Dir<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}

#[inline]
fn expect_dir<TxnId, FE>(
    entry: TxnMapValueReadGuard<Id, DirEntry<TxnId, FE>>,
) -> Result<TxnMapValueReadGuardMap<Id, Dir<TxnId, FE>>> {
    entry.try_map(|entry| match entry {
        DirEntry::Dir(dir) => Ok(dir.clone()),
        DirEntry::File(file) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("not a directory: {:?}", file),
        )
        .into()),
    })
}

#[inline]
fn expect_file<TxnId, FE>(
    entry: TxnMapValueReadGuard<Id, DirEntry<TxnId, FE>>,
) -> Result<TxnMapValueReadGuardMap<Id, File<TxnId, FE>>> {
    entry.try_map(|entry| match entry {
        DirEntry::Dir(dir) => {
            Err(io::Error::new(io::ErrorKind::InvalidData, format!("not a file: {:?}", dir)).into())
        }
        DirEntry::File(file) => Ok(file.clone()),
    })
}
