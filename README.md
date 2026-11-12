# txfs

A cached transactional filesystem layer for Rust.

## Directory ownership

`Dir` owns one transactional history for immediate membership and file content
versions. A file record identifies its staged version; a loaded canonical file
starts at the finalized baseline. Loading does not create per-file version
workspaces; staging a mutation creates its workspace. `File` is a read handle resolving its name
through that directory history, including later same-name replacements and removals. An absent name returns
`NotFound`. It has
no independent transaction lock or lifecycle. Retained handles keep their history
alive without making an ownership cycle.

Every observation after loading carries a transaction ID. File reads retain both
the native read guard and the directory's transaction guard, releasing native
access first. Missing required stages are corruption errors. A historical baseline
whose bytes have been superseded returns `txn_lock::Error::Outdated`, never newer
contents. Finalization discards stages through its cutoff and rejects older reads;
reopening does not reconstruct discarded historical contents.

Files expose read handles only. Replace complete contents using directory `delete`
followed by `create_file`. Same-transaction recreation replaces only that stage.
Creation rejects incompatible native kinds and newer committed changes to the same
name before native effects; unrelated names remain independent. Ordinary caller
rollback releases the rejected transaction's reservations.

`create_dir`, `get_dir`, and `DirEntry::Dir` delegate native `freqfs::DirLock`
handles. Their recipients own contents, durability, and lifecycle, or explicitly
load another transactional `Dir`. Parent lifecycle never recursively commits or
finalizes native children. Only `.txfs` is reserved by this crate.

Handing a native root to `Dir::load` is exclusive: stop using retained mutable
native aliases and do not reopen the storage independently. Read handles retain
access to the directory history; they do not revoke preexisting native aliases.

## Decisions and cleanup

Callers finish transaction operations before applying lifecycle decisions and
coordinate child owners before deleting a subtree. The lock order is the version
root before the canonical root; no bookkeeping lock is held across storage awaits.
Causal admission happens before native exclusion, allowing predecessor commits to
finish regardless of polling order.

Commit publishes and durably synchronizes changed immediate files before making
one directory decision. Copying holds the canonical-directory lock; each admitted
file then synchronizes independently after releasing that lock. Version-root
exclusion remains held through every publication, the decision, native membership
deletions, and deletion synchronization, preventing recreation from overtaking
deletion. Repeated successful decisions do not republish old bytes
or reapply removals to newer entries.

Rollback removes the transaction's changes from the same history, then removes
only its exact stages, including create-then-delete orphans. It reclaims empty
per-file workspaces and unused, fully cleaned child directories, preserving every
name still referenced by retained history. The finalized cutoff does not advance.
With no other unresolved work, successful rollback permits reopening without
finalization. Nonempty, unprotected children return `Error::IO` with
`DirectoryNotEmpty`; their recipients must clean their contents before parent
cleanup can finish. Malformed child metadata remains a corruption error. Logical
rollback may already have completed when cleanup fails, so the caller must finish
cleanup/recovery before reuse.

Finalization advances history and removes stages through its cutoff. Newer
membership and stages remain protected. Both cleanup paths reclaim empty per-file
workspaces even when their names remain in membership: retained File handles
resolve them through the directory and later creation can recreate them. They
share the same check for fully cleaned children. Finalization leaves nonempty
delegated child contents to their owners. Cleanup never republishes a subtree
removed by its parent. Pending native deletion obligations are synchronized even
when a later cleanup pass selects no new entries.

Native publication and stage-cleanup work is admitted lazily, bounded by the
native directory's `max_file_handles()`. The first completed error drops remaining
futures. Cancellation and lifecycle errors can leave partially applied native
work: the caller must recover before reuse. Repeating a decision is not a repair
mechanism. No background completion, distributed decision, or recovery policy is
added by this library.

Ordinary freqfs writeback is buffered. File publication during commit is durable;
membership cleanup synchronizes deletions without writing surviving delegated
children. These durability boundaries and the `.txfs/<name>/<transaction>` stage
layout remain independent of the in-memory history representation.

`Dir::iter` exposes opaque dereferenceable entry guards rather than the underlying
map iterator. Retained entries still protect pending membership. `get_file`,
`read_file`, and `FileVersionRead` retain their owned read interfaces. Missing
files consistently return `Error::NotFound` through either a file handle or
`Dir::read_file`; native I/O errors retain their original causes.

## Filesystem codecs and validation

Callers implement `freqfs::FileLoad` and `FileSave` for their complete file-entry
type. txfs does not select a codec. The example chooses TBON explicitly.

For history or lifecycle changes, run the affected `--lib` fixtures, doctests,
example compilation, and strict library/test Clippy. Complete validation remains
available with `cargo test --all-targets --all-features` and
`cargo test --doc --all-features`; routine edits select their affected behavior.
