use std::fmt;
use std::hash::Hash;
use std::ops::Deref;

use freqfs::{FileLoad, FileReadGuardOwned};
use hr_id::Id;
use safecast::AsType;
use txn_lock::map::TxnMapValueReadGuard;

use super::dir::Entry;
use super::{Dir, Result};

/// A read guard on a version of a transactional [`File`].
pub struct FileVersionRead<TxnId, FE, F> {
    // Release native access before the directory's transaction permit can notify waiters.
    pub(super) version: FileReadGuardOwned<FE, F>,
    pub(super) _entry: TxnMapValueReadGuard<Id, Entry<TxnId, FE>>,
}

impl<TxnId, FE, F> Deref for FileVersionRead<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.version.deref()
    }
}

/// A read handle to versions in its owning [`Dir`]'s history.
/// Retained handles resolve later same-name replacements through that history.
/// Replace complete contents and apply lifecycle decisions through the directory.
///
/// ```compile_fail
/// fn mutate<F>(file: txfs::File<u64, F>)
/// where F: freqfs::FileLoad + Clone + Send + Sync + get_size::GetSize + safecast::AsType<F> + 'static {
///     let _ = file.write::<F>(1);
/// }
/// ```
///
/// ```compile_fail
/// fn commit<F>(file: txfs::File<u64, F>)
/// where F: freqfs::FileSave + Clone + Send + Sync + get_size::GetSize {
///     let _ = file.commit(1);
/// }
/// ```
pub struct File<TxnId, FE> {
    pub(super) directory: Dir<TxnId, FE>,
    pub(super) name: Id,
}

impl<TxnId, FE> Clone for File<TxnId, FE> {
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
            name: self.name.clone(),
        }
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
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        self.directory.read_version(txn_id, &self.name).await
    }

    /// Lock this file for reading at the given `txn_id` without borrowing.
    pub async fn into_read<F>(self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        F: Send + Sync + 'static,
        FE: FileLoad + AsType<F>,
    {
        self.read(txn_id).await
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
