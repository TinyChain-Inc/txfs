# txfs
A cached transactional filesystem layer for Rust

## Filesystem codecs

Callers implement `freqfs::FileLoad` and `FileSave` for their complete file-entry
type. txfs does not choose a byte codec and no longer has a `stream` feature.
The example explicitly chooses TBON; the library retains codec-independent I/O.

## Directory ownership

`Dir` owns immediate membership and transactional file versions. `load` validates
those versions without opening child directories as transactional owners.
`create_dir`, `get_dir`, and `DirEntry::Dir` expose native `freqfs::DirLock` handles.
Their recipients may load another `Dir` or supply their own storage lifecycle.
Only `.txfs` is reserved by this crate.

`commit(id)`, `rollback(id)`, and `finalize(id)` delegate to immediate files and
manage membership. Callers coordinate child lifecycle separately, releasing child
operations before committing deletion of a subtree. `delete` and `truncate`
stage membership removal; rollback leaves native contents intact, while commit
removes the corresponding physical storage through `freqfs`.

Ordinary writeback remains buffered. Immediate file commit explicitly makes its
canonical version durable; directory membership synchronization applies deletions
without writing surviving children. A native child's owner supplies its durability
boundaries. `into_inner()` exposes this directory's underlying native handle; it
does not revoke transactional state held by other clones.

Run `cargo test --all-targets --all-features`, `cargo test --doc --all-features`,
`cargo fmt --check`, and `cargo clippy --all-targets --all-features -- -D warnings`.
