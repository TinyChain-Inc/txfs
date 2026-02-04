# Contributing to txfs

`txfs` provides a transactional filesystem layer used by TinyChain. Keep changes
minimal, general, and aligned with the shared txfs layout contract so every
adapter can hydrate the same on-disk tree.

## Before you start

- Read `README.md` and `AGENTS.md` for scope and design constraints.
- Follow the repo-wide code style in `CODE_STYLE.md` (import grouping and
  rustfmt/clippy expectations).

## Development checklist

1. **Preserve URI ↔ txfs mirroring.** Any path/layout changes must keep the
   `<data-dir>/<segment>` layout aligned with canonical URI helpers.
2. **Serialization symmetry.** If you change `destream` encoders/decoders,
   add round-trip coverage to keep `IntoStream`/`FromStream` pairs stable.
3. **Avoid bespoke storage.** Keep new behavior expressed in terms of existing
   primitives and traits; prefer general-purpose traits over special cases.
4. **Testing.** Run `cargo test --all-features`. Add focused unit tests for any
   new transactional or locking behavior.
5. **Docs.** Update `README.md` (and other relevant docs) when user-visible
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
