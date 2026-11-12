use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::io;
use std::ops::Deref;
use std::str::FromStr;

use freqfs::{DirLock, FileLoad, FileSave};
use futures::future::TryFutureExt;
use futures::stream::{self, Stream, StreamExt};
use get_size::GetSize;
use hr_id::Id;
use safecast::AsType;
use txn_lock::map::{
    Entry as TxnMapEntry, TxnMapLock, TxnMapLockIterGuard, TxnMapValueReadGuardMap,
};

use super::file::{File, FileVersionRead};
use super::{Error, Result};

/// The name of an entry in a [`Dir`], used to avoid unnecessary allocations
pub type Key = txn_lock::map::Key<Id>;

/// The name of the directory where un-committed file versions are cached
pub const VERSIONS: &str = ".txfs";

/// An entry in a [`Dir`]
pub enum DirEntry<TxnId, FE> {
    Dir(DirLock<FE>),
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
        matches!(self, Self::Dir(_))
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

// Membership and the stage naming each file's bytes share one history.
// Read handles are projected from these records, never stored inside their owner.
#[derive(Clone)]
pub(super) enum Entry<TxnId, FE> {
    Dir(DirLock<FE>),
    File(Option<TxnId>),
}

impl<TxnId, FE> Entry<TxnId, FE> {
    fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(_))
    }

    fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }
}

impl<TxnId: fmt::Debug, FE> fmt::Debug for Entry<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Dir(dir) => dir.fmt(f),
            Self::File(version) => f.debug_tuple("file version").field(version).finish(),
        }
    }
}

// An iterator item retains the original membership guard while exposing a read handle.
struct EntryRead<TxnId, FE> {
    entry: DirEntry<TxnId, FE>,
    _version: TxnMapLockIterGuard<Id, Entry<TxnId, FE>>,
}

impl<TxnId, FE> Deref for EntryRead<TxnId, FE> {
    type Target = DirEntry<TxnId, FE>;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

/// Transactional membership and file versions in one directory.
///
/// Child directories are native handles. Their recipients own loading and
/// lifecycle delegation; this directory never versions or synchronizes their contents.
pub struct Dir<TxnId, FE> {
    pub(super) canon: DirLock<FE>,
    pub(super) versions: DirLock<FE>,
    pub(super) entries: TxnMapLock<TxnId, Id, Entry<TxnId, FE>>,
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
    TxnId: Hash + Ord + Copy + fmt::Display + fmt::Debug + Send + Sync + 'static,
    FE: FileSave + GetSize + Clone,
{
    /// Load immediate membership and file versions from a [`DirLock`].
    /// Child contents are left untouched for their owners to load and validate.
    pub async fn load(canon: DirLock<FE>) -> Result<Self> {
        #[cfg(feature = "log")]
        log::debug!("load transactional dir from {:?}", canon);

        let versions = {
            let mut dir = canon.write().await;
            dir.get_or_create_dir(VERSIONS.to_string())?
        };

        let mut contents = HashMap::new();
        let canonical = canon
            .try_read()?
            .iter()
            .filter(|(name, _)| name.as_str() != VERSIONS)
            .map(|(name, entry)| (name.clone(), entry.clone()))
            .collect::<BTreeMap<_, _>>();

        {
            let versions = versions.read().await;
            for (name, entry) in versions.iter() {
                let canonical_file = matches!(canonical.get(name), Some(freqfs::DirEntry::File(_)));
                match entry {
                    freqfs::DirEntry::Dir(dir) if canonical_file && dir.read().await.is_empty() => {
                    }
                    _ => {
                        return Err(Error::Corrupt(format!(
                            "inconsistent transactional storage at {VERSIONS}/{name}"
                        )));
                    }
                }
            }
        }

        for (name, entry) in canonical {
            let name: Id = name.parse()?;
            let entry = match entry {
                freqfs::DirEntry::Dir(dir) => Entry::Dir(dir),
                freqfs::DirEntry::File(_) => Entry::File(None),
            };

            contents.insert(name, entry);
        }

        Ok(Self {
            canon,
            versions,
            entries: TxnMapLock::from_committed(contents),
        })
    }

    /// Create a child directory with transactional membership at `txn_id`.
    /// Its recipient owns native contents, or explicitly loads another [`Dir`].
    pub async fn create_dir(&self, txn_id: TxnId, name: Id) -> Result<DirLock<FE>> {
        #[cfg(feature = "logging")]
        log::trace!("Dir::create_dir {name}");

        if name.as_str() == VERSIONS {
            return Err(Error::IO(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("reserved storage name {name}"),
            )));
        }

        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into());
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        let _versions = self.versions.write().await;
        // Reject obsolete membership before creating or reusing a native owner.
        entry.get_committed()?;
        let mut canon = self.canon.write().await;
        let sub_dir = canon.get_or_create_dir(name.to_string())?;

        entry.insert(Entry::Dir(sub_dir.clone()))?;

        Ok(sub_dir)
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Hash + Ord + Copy + fmt::Display + fmt::Debug + Send + Sync + 'static,
    FE: Clone + Send + Sync + 'static,
{
    /// Return `true` if this [`Dir`] has an entry at the given `name` at `txn_id`.
    pub async fn contains(&self, txn_id: TxnId, name: &Id) -> Result<bool> {
        self.entries
            .contains_key(txn_id, name)
            .map_err(Error::from)
            .await
    }

    /// Stage removal of the entry at `name`, returning whether it was present.
    /// Child contents remain intact until commit removes the physical subtree.
    /// Callers must release child operations before committing its deletion.
    pub async fn delete(&self, txn_id: TxnId, name: Id) -> Result<bool> {
        Ok(self.entries.remove(txn_id, &name).await?.is_some())
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
        FE: FileLoad + AsType<F>,
        F: Send + Sync + 'static,
    {
        let entries = self.iter(txn_id).await?;
        let files = entries.filter_map(|(name, entry)| match &*entry {
            DirEntry::File(file) => Some((name, file.clone())),
            _ => None,
        });

        let files = stream::iter(files)
            .then(move |(name, file)| file.into_read(txn_id).map_ok(|file| (name, file)));

        Ok(Box::pin(files))
    }

    /// Construct an iterator over the contents of this [`Dir`] at `txn_id`.
    pub async fn iter(
        &self,
        txn_id: TxnId,
    ) -> Result<
        impl Iterator<Item = (Key, impl Deref<Target = DirEntry<TxnId, FE>>)> + Send + 'static,
    > {
        let entries = self.entries.iter(txn_id).await?;
        let directory = self.clone();
        Ok(entries.map(move |(name, version)| {
            let entry = match &*version {
                Entry::Dir(dir) => DirEntry::Dir(dir.clone()),
                Entry::File(_) => DirEntry::File(File {
                    directory: directory.clone(),
                    name: (*name).clone(),
                }),
            };

            (
                name,
                EntryRead {
                    entry,
                    _version: version,
                },
            )
        }))
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id`.
    pub async fn get_dir(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, DirLock<FE>>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            entry
                .try_map(|entry| match entry {
                    Entry::Dir(dir) => Ok(dir.clone()),
                    Entry::File(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a directory: {name}"),
                    )
                    .into()),
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id` synchronously, if possible.
    pub fn try_get_dir(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, DirLock<FE>>>> {
        if let Some(entry) = self.entries.try_get(txn_id, name).map_err(Error::from)? {
            entry
                .try_map(|entry| match entry {
                    Entry::Dir(dir) => Ok(dir.clone()),
                    Entry::File(_) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a directory: {name}"),
                    )
                    .into()),
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Stage removal of all entries, without changing child transaction state.
    pub async fn truncate(self, txn_id: TxnId) -> Result<()> {
        self.entries.clear(txn_id).await?;
        Ok(())
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: fmt::Display + fmt::Debug + Hash + Ord + Copy,
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
        FE: FileLoad + AsType<F>,
        F: GetSize + Clone,
    {
        #[cfg(feature = "logging")]
        log::trace!("Dir::create_file {name}");

        if name.as_str() == VERSIONS {
            return Err(Error::IO(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("reserved storage name {name}"),
            )));
        }

        // this write permit ensures that there is no other pending entry with this name
        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into());
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        // Keep native creation and membership publication indivisible to cleanup.
        let mut versions_root = self.versions.write().await;
        let prior = entry.get_committed()?;
        if matches!(prior.as_deref(), Some(Entry::Dir(_)))
            || self.canon.read().await.get_dir(name.as_str()).is_some()
        {
            return Err(
                io::Error::new(io::ErrorKind::AlreadyExists, format!("directory {name}")).into(),
            );
        }

        let versions = versions_root.get_or_create_dir(name.to_string())?;
        let mut versions = versions.write().await;
        let txn_name = txn_id.to_string();
        // Same-transaction replacement changes only its own stage.
        if versions.get_file(&txn_name).is_some() {
            versions.delete(&txn_name).await;
        }

        versions.create_file(txn_name, contents).await?;
        entry.insert(Entry::File(Some(txn_id)))?;
        let file = File {
            directory: self.clone(),
            name,
        };

        Ok(file)
    }

    /// Get a [`File`] present in this [`Dir`] at the given `txn_id`.
    pub async fn get_file(
        &self,
        txn_id: TxnId,
        name: &Id,
    ) -> Result<Option<TxnMapValueReadGuardMap<Id, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            entry
                .try_map(|entry| match entry {
                    Entry::File(_) => Ok(File {
                        directory: self.clone(),
                        name: name.clone(),
                    }),
                    Entry::Dir(dir) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a file: {dir:?}"),
                    )
                    .into()),
                })
                .map(Some)
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
            entry
                .try_map(|entry| match entry {
                    Entry::File(_) => Ok(File {
                        directory: self.clone(),
                        name: name.clone(),
                    }),
                    Entry::Dir(dir) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a file: {dir:?}"),
                    )
                    .into()),
                })
                .map(Some)
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
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.read(txn_id).await
        } else {
            Err(Error::NotFound(name.to_string()))
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: FromStr + fmt::Display + Hash + Copy + Ord + fmt::Debug + Send + Sync,
    FE: FileSave + GetSize + Clone,
{
    /// Commit immediate file versions and membership at `txn_id`.
    /// Child owners must complete their own lifecycle under caller coordination.
    pub async fn commit(&self, txn_id: TxnId) -> Result<()> {
        #[cfg(feature = "logging")]
        log::trace!("Dir::commit");

        // Admission resolves causal predecessors before native publication exclusion.
        let mut files = self.entries.iter(txn_id).await?;
        self.entries.check_commit(txn_id)?;
        if self.entries.is_committed(txn_id)? {
            return Ok(());
        }

        let versions = self.versions.write().await;
        // A concurrent identical decision may have finished while we waited.
        self.entries.check_commit(txn_id)?;
        if self.entries.is_committed(txn_id)? {
            return Ok(());
        }

        let limit = self.canon.read().await.max_file_handles();
        let mut publications = stream::iter(std::iter::from_fn(move || loop {
            let (name, entry) = files.next()?;
            if matches!(&*entry, Entry::File(Some(id)) if *id == txn_id) {
                // Native work retains names, not guards on pending membership.
                return Some(name);
            }
        }))
        .map(|name| {
            let versions = &versions;
            async move {
                #[cfg(test)]
                crate::lifecycle_tests::entered().await?;

                let staged = {
                    let dir = versions.get_dir(name.as_str()).ok_or_else(|| {
                        Error::Corrupt(format!("missing staged versions for {name}"))
                    })?;

                    dir.read()
                        .await
                        .get_file(&txn_id.to_string())
                        .cloned()
                        .ok_or_else(|| {
                            Error::Corrupt(format!("missing staged version {txn_id:?} for {name}"))
                        })?
                };

                let file = {
                    let mut canon = self.canon.write().await;
                    canon.copy_file_from(name.to_string(), &staged).await?
                };

                file.sync_all().await?;
                Ok::<_, Error>(())
            }
        })
        .buffer_unordered(limit);

        while let Some(result) = publications.next().await {
            result?;
        }

        drop(publications);

        // All bytes are durable before their single logical decision. Revalidate
        // without waiting, retaining exclusion through deletions and their sync.
        let (contents, deltas) = self.entries.try_read_and_commit(txn_id)?;

        // New directory entries need durable publication as well as deletions.
        let needs_sync = deltas.as_ref().is_some_and(|deltas| !deltas.is_empty());
        if let Some(deltas) = deltas {
            let mut canon = self.canon.write().await;

            for (name, entry) in deltas {
                if entry.is_none() {
                    assert!(!contents.contains_key(&*name));

                    canon.delete(name.as_str()).await;
                }
            }
        };

        if needs_sync {
            // Publish membership changes and finish deletions before same-name reuse.
            self.canon.sync_deleted().await?;
        }

        Ok(())
    }

    /// Roll back immediate file versions and membership at `txn_id`.
    /// Reclaim unused workspaces and cleaned children without advancing the cutoff.
    /// Nonempty, unprotected children return `DirectoryNotEmpty`; their recipient
    /// must finish cleanup. Logical rollback may already have completed on error.
    pub async fn rollback(&self, txn_id: TxnId) -> Result<()> {
        // Wait for causal membership before excluding publication and cleanup.
        let admission = self.entries.iter(txn_id).await?;
        self.entries.check_commit(txn_id)?;
        let mut versions = self.versions.write().await;
        self.entries.check_commit(txn_id)?;
        self.entries.rollback(&txn_id)?;
        drop(admission);

        let mut protected = HashSet::new();
        self.entries.inspect_versions(txn_id, |_, name, _| {
            protected.insert(name.clone());
        })?;

        // Includes stages whose names disappeared from pending membership.
        let limit = self.canon.read().await.max_file_handles();
        let txn_name = txn_id.to_string();
        let txn_name = txn_name.as_str();
        let mut entries = versions.iter();
        let entries = std::iter::from_fn(move || {
            let (name, entry) = entries.next()?;
            Some((name.clone(), entry.clone()))
        });

        let mut cleanup = stream::iter(entries)
            .map(|(name, entry)| async move {
                let freqfs::DirEntry::Dir(dir) = entry else {
                    return Err(Error::Corrupt(
                        "invalid transactional version directory".into(),
                    ));
                };

                #[cfg(test)]
                crate::lifecycle_tests::entered().await?;

                let mut dir = dir.write().await;
                match dir.get(txn_name) {
                    Some(freqfs::DirEntry::File(_)) => {
                        dir.delete(txn_name).await;
                        if let Err(error) = dir.sync_deleted().await {
                            if error.kind() != io::ErrorKind::NotFound {
                                return Err(error.into());
                            }
                        }
                    }
                    Some(freqfs::DirEntry::Dir(_)) => {
                        return Err(Error::Corrupt("invalid transaction stage".into()));
                    }
                    None => {}
                }

                Ok(dir.is_empty().then_some(name))
            })
            .buffer_unordered(limit);

        let mut remove_versions = Vec::new();
        while let Some(result) = cleanup.next().await {
            if let Some(name) = result? {
                remove_versions.push(name);
            }
        }

        drop(cleanup);
        for name in remove_versions {
            versions.delete(&name).await;
        }

        // Deleted historical owners may finish cleanup after parent removal.
        if let Err(error) = versions.sync_deleted().await {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }

        let mut canon = self.canon.write().await;
        let mut remove_dirs = Vec::new();
        for (name, entry) in canon.iter() {
            if name == VERSIONS || protected.contains(name.as_str()) {
                continue;
            }

            if let freqfs::DirEntry::Dir(dir) = entry {
                if !cleaned_child(dir).await? {
                    return Err(io::Error::new(
                        io::ErrorKind::DirectoryNotEmpty,
                        format!("recipient must clean rolled-back directory {name}"),
                    )
                    .into());
                }

                remove_dirs.push(name.clone());
            }
        }

        for name in &remove_dirs {
            canon.delete(name).await;
        }

        // A prior failed or cancelled pass may have left deletion obligations.
        if let Err(error) = canon.sync_deleted().await {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }

        Ok(())
    }

    /// Finalize immediate file versions and membership through `txn_id`.
    /// This does not finalize or synchronize surviving child directories.
    pub async fn finalize(&self, txn_id: TxnId) -> Result<()> {
        // Native publication and cleanup share these guards, in this order.
        // No transaction bookkeeping guard is retained across storage work.
        let mut versions = self.versions.write().await;
        let mut canon = self.canon.write().await;
        let mut protected = HashSet::new();
        let inspected = self.entries.inspect_versions(txn_id, |id, name, _| {
            if id.is_some_and(|id| id > txn_id) {
                protected.insert(name.clone());
            }
        });

        match inspected {
            Err(txn_lock::Error::Outdated) => return Ok(()),
            result => result?,
        }

        let Some(entries) = self.entries.read_and_finalize(txn_id)? else {
            return Ok(());
        };

        protected.extend(entries.keys().map(|name| (**name).clone()));

        // Removed owners and create-then-delete files may have orphan stages.
        // Only the cutoff's stages are reclaimable, regardless of current names.
        let mut remove_versions = Vec::new();
        let mut dirs = versions.iter();
        let dirs = std::iter::from_fn(move || {
            let (name, entry) = dirs.next()?;
            Some((name.clone(), entry.clone()))
        });

        let mut cleanup = stream::iter(dirs)
            .map(|(name, entry)| async move {
                let freqfs::DirEntry::Dir(dir) = entry else {
                    return Err(Error::Corrupt(
                        "invalid transactional version directory".into(),
                    ));
                };

                #[cfg(test)]
                crate::lifecycle_tests::entered().await?;

                let mut dir = dir.write().await;
                let mut stages = Vec::new();
                for (stage, entry) in dir.iter() {
                    let id = stage.parse::<TxnId>().map_err(|_| {
                        Error::Corrupt(format!("invalid staged transaction version {stage}"))
                    })?;

                    if !matches!(entry, freqfs::DirEntry::File(_)) {
                        return Err(Error::Corrupt("invalid transaction stage".into()));
                    }

                    if id <= txn_id {
                        stages.push(stage.clone());
                    }
                }

                for stage in stages {
                    dir.delete(&stage).await;
                }

                if let Err(error) = dir.sync_deleted().await {
                    if error.kind() != io::ErrorKind::NotFound {
                        return Err(error.into());
                    }
                }

                Ok(dir.is_empty().then_some(name))
            })
            .buffer_unordered(canon.max_file_handles());

        while let Some(result) = cleanup.next().await {
            if let Some(name) = result? {
                remove_versions.push(name);
            }
        }

        drop(cleanup);
        for name in remove_versions {
            versions.delete(&name).await;
        }

        // Historical owners may finish cleanup after parent removal.
        if let Err(error) = versions.sync_deleted().await {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }

        let mut remove_dirs = Vec::new();
        for (name, entry) in canon.iter() {
            if name == VERSIONS || protected.contains(name.as_str()) {
                continue;
            }

            if let freqfs::DirEntry::Dir(dir) = entry {
                if cleaned_child(dir).await? {
                    remove_dirs.push(name.clone());
                }
            }
        }

        for name in &remove_dirs {
            canon.delete(name).await;
        }

        // A prior failed or cancelled pass may have left deletion obligations.
        if let Err(error) = canon.sync_deleted().await {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error.into());
            }
        }

        Ok(())
    }
}

impl<TxnId, FE> fmt::Debug for Dir<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}

// Parent cleanup owns only empty children or completed child metadata.
async fn cleaned_child<FE: Send + Sync>(dir: &DirLock<FE>) -> Result<bool> {
    let dir = dir.read().await;
    if dir.is_empty() {
        return Ok(true);
    }

    match dir.get(VERSIONS) {
        Some(freqfs::DirEntry::File(_)) => Err(Error::Corrupt(
            "invalid transactional version directory".into(),
        )),
        Some(freqfs::DirEntry::Dir(metadata)) if dir.len() == 1 => {
            Ok(metadata.read().await.is_empty())
        }
        _ => Ok(false),
    }
}
