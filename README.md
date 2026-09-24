# txfs
A cached transactional filesystem layer for Rust

## Filesystem codecs

Callers implement `freqfs::FileLoad` and `FileSave` for their complete file-entry
type. txfs does not choose a byte codec and no longer has a `stream` feature.
The example explicitly chooses TBON; the library retains codec-independent I/O.

## Delegated native storage

`Dir::create_native` allocates the reserved `.native` subtree for an unpublished
owner with its own persistence lifecycle. `Dir::native` loads that handle without
creating missing storage. Transactional membership belongs to the enclosing
directory; its recipient owns native contents, durability, and recovery. Loading
and recursive lifecycle operations never interpret or synchronize those contents.
Deleting the enclosing directory deletes the subtree with it. Both `.native` and
`.txfs` are reserved and cannot be ordinary transactional entries.
