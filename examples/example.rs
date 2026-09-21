use std::cmp::Ordering;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use destream::{de, en};
use freqfs::{Cache, DirLock};
use hr_id::Id;
use rand::Rng;
use safecast::as_type;
use tokio::fs;
use txfs::Dir;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
struct Txn(u64);

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
            self.0.partial_cmp(&other)
        } else {
            None
        }
    }
}

impl freqfs::Name for Txn {
    fn partial_cmp(&self, key: &str) -> Option<Ordering> {
        freqfs::Name::partial_cmp(&self.0, key)
    }
}

impl fmt::Display for Txn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Txn {
    type Err = std::num::ParseIntError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

#[derive(Clone)]
enum File {
    Bin(Vec<u8>),
    Text(String),
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Bin(bytes) => bytes.to_stream(encoder),
            Self::Text(string) => string.to_stream(encoder),
        }
    }
}

as_type!(File, Bin, Vec<u8>);
as_type!(File, Text, String);

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::rng();
    loop {
        let rand: u32 = rng.random();
        let path = PathBuf::from(format!("/tmp/test_txfs_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

async fn run_example(cache: DirLock<File>) -> Result<(), txfs::Error> {
    let first_txn = Txn(1);
    let second_txn = Txn(2);
    let third_txn = Txn(3);

    let root = Dir::load(cache).await?;

    let file_one: Id = "file-one".parse()?;
    let file_two: Id = "file-two".parse()?;
    let subdir_name: Id = "subdir".parse()?;

    // just holding a reference to a file doesn't block any transactional I/O
    let file = root
        .create_file(
            first_txn,
            file_one.clone(),
            String::from("file one contents"),
        )
        .await?;

    {
        let read_guard = file.read::<String>(first_txn).await?;

        // but holding a read guard will block acquiring a write guard, and vice versa
        // let read_guard = file.read::<String>(first_txn).await?;
        assert_eq!(&*read_guard, "file one contents");

        // a read in the past won't block a write in the future
        assert!(file.write::<String>(second_txn).await.is_ok());
    }

    // but a write in the past will block a read in the future
    assert!(root.try_get_file(second_txn, &file_one).is_err());

    // committing a Dir with recursive=true commits all its children
    root.commit(first_txn, true).await?;

    let subdir = root.create_dir(second_txn, subdir_name.clone()).await?;

    subdir
        .create_file(second_txn, file_two.clone(), vec![2, 3, 4])
        .await?;

    root.commit(second_txn, true).await?;

    // deleting a directory will delete all its children, recursively
    root.delete(third_txn, subdir_name.clone()).await?;

    // accessing "subdir" after this can cause the filesystem to get out of sync with the cache!
    root.commit(third_txn, true).await?;

    // call "finalize" to drop all information about commits earlier than the given transaction ID
    root.finalize(third_txn).await?;

    let fourth_txn = Txn(4);

    // anything that was deleted is now safe to re-create
    let subdir = root.create_dir(fourth_txn, subdir_name).await?;

    let file = subdir
        .create_file(fourth_txn, file_two, vec![3, 4, 5])
        .await?;

    root.commit(fourth_txn, true).await?;

    let fifth_txn = Txn(5);

    // and access in later transactions
    assert_eq!(&*file.read::<Vec<u8>>(fifth_txn).await?, &[3u8, 4, 5]);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), txfs::Error> {
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::new(40, None, 0, Duration::from_secs(3));

    // load the directory and file paths into memory (not file contents, yet)
    let root = cache.load(path.clone())?;

    // all I/O under the cache directory at `path` MUST now go through the cache methods
    // otherwise concurrent filesystem access may cause errors
    run_example(root).await?;

    Ok(())
}

impl de::FromStream for File {
    type Context = ();
    async fn from_stream<D: de::Decoder>(_: (), decoder: &mut D) -> Result<Self, D::Error> {
        decoder.decode_any(FileVisitor).await
    }
}
struct FileVisitor;
impl de::Visitor for FileVisitor {
    type Value = File;
    fn expecting() -> &'static str {
        "a filesystem entry"
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<File, E> {
        Ok(File::Text(value))
    }
    async fn visit_seq<A: de::SeqAccess>(self, mut seq: A) -> Result<File, A::Error> {
        let mut bytes = Vec::new();
        while let Some(byte) = seq.next_element::<u8>(()).await? {
            bytes.push(byte);
        }
        Ok(File::Bin(bytes))
    }
}

impl freqfs::FileLoad for File {
    async fn load(
        _: &std::path::Path,
        file: tokio::fs::File,
        _: std::fs::Metadata,
    ) -> std::io::Result<Self> {
        tbon::de::read_from((), file)
            .await
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}
impl freqfs::FileSave for File {
    async fn save(&self, file: &mut tokio::fs::File) -> std::io::Result<u64> {
        use futures::TryStreamExt;
        use tokio::io::AsyncWriteExt;
        let mut stream = tbon::en::encode(self).map_err(std::io::Error::other)?;
        let mut size = 0;
        while let Some(chunk) = stream.try_next().await.map_err(std::io::Error::other)? {
            file.write_all(&chunk).await?;
            size += chunk.len() as u64;
        }
        Ok(size)
    }
}
