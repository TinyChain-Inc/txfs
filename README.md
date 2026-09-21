# txfs
A cached transactional filesystem layer for Rust

## Filesystem codecs

Callers implement `freqfs::FileLoad` and `FileSave` for their complete file-entry
type. txfs does not choose a byte codec and no longer has a `stream` feature.
The example explicitly chooses TBON; the library retains codec-independent I/O.
