# Contributing to txfs

`txfs` is a standalone transactional filesystem library. Keep changes minimal,
general-purpose, and defined by this repository's public API and tests. TinyChain
is one consumer and does not define this crate's local contract.

## Before you start

- Read `README.md` for the supported API and storage model.
- Use `cargo fmt` and Clippy's repository-local configuration.

## Development checklist

1. **Preserve storage semantics.** Path and layout changes require focused
   create, load, transaction, and restart coverage.
2. **Require transactional reads.** Canonical files may initialize an
   unpublished `Dir`, but every observation after construction must carry a
   transaction ID and retain the corresponding read guard.
3. **Serialization symmetry.** If you change `destream` encoders/decoders,
   add round-trip coverage to keep `IntoStream`/`FromStream` pairs stable.
4. **Avoid bespoke storage.** Keep new behavior expressed in terms of existing
   primitives and traits; prefer general-purpose traits over special cases.
5. **Testing.** Run `cargo test --all-features`. Add focused unit tests for any
   new transactional or locking behavior.
6. **Docs.** Update `README.md` (and other relevant docs) when user-visible
   behavior changes.

## Pre-submit

- `cargo fmt`
- `cargo clippy --all-targets --all-features`
- `cargo test --all-features`

## Rights and licensing

By contributing to this crate you represent that (a) you authored the work (or
otherwise have the rights to contribute it), (b) the contribution is
unencumbered by third-party intellectual property claims, and (c) you transfer
and assign all right, title, and interest in the contribution to The TinyChain
Contributors for distribution under the Apache 2.0 license (see `LICENSE`).
