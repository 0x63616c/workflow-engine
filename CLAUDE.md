# WFE

A durable workflow engine: deterministic replicated state machines (VSR + LSM) driven by a
seeded simulator. Research notes live in `docs/research/`.

## Done means `just check` passes

`just check` runs formatting, clippy, ast-grep, typos, cargo-deny, cargo-shear and every test.
`just fmt` formats. Never mark work done with a failing check, and never weaken a check to pass it.

## Rules and what enforces them

When a rule here says "prose only", nothing stops you breaking it. Every other rule fails a check.

| Rule | Enforced by |
|---|---|
| Replicated crates never read clocks, env, threads, OS randomness or files directly | root `clippy.toml` |
| No `HashMap`/`HashSet`/floats in deterministic crates | root `clippy.toml` |
| Every crate is classified as Replicated, Deterministic or Io | `crates/rules` (`wfe_rules::CRATES`) |
| Replicated crates never depend on I/O crates, even transitively | `crates/rules` |
| Lint levels live only in `[workspace.lints]` | `crates/rules` |
| Exceptions are `#[expect(lint, reason = "...")]`, never `#[allow]` | clippy `allow_attributes` |
| No `unwrap()` outside tests; no `todo!`, `dbg!`, `unsafe` | `[workspace.lints]` |
| No `_ =>` arms over our own enums: a new variant must be handled everywhere | clippy `wildcard_enum_match_arm` |
| No lossy `as` casts: use `T::from` / `T::try_from` | clippy `cast_*` |
| No discarded `Result`s, no `map_err(\|_\| ...)` | clippy `let_underscore_must_use`, `map_err_ignore` |
| No comments in Rust code, except the keep list below | `ast-grep/rules/no-line-comments.yml` |
| `///` docs only on public API: `pub` items, their fields and variants, or clap/JSON-schema help text | `ast-grep/rules/docs-only-on-public-api.yml` |
| `//!` docs only in a crate root (`lib.rs`, `main.rs`) or a `pub mod` file | `crates/rules` |
| `pub` means exported; anything only this crate uses is `pub(crate)` | rustc `unreachable_pub` |
| `expect()` messages state the invariant | `ast-grep/rules/expect-states-invariant.yml` |
| Libraries return typed errors; `anyhow` only in binaries | `deny.toml` |
| US English everywhere | `_typos.toml` |
| No unused dependencies | `cargo shear` |
| Enum ↔ string names come from `strum` derives, not hand-written `match` tables | `ast-grep/rules/enum-names-from-strum.yml` |
| Error messages say what is wrong, the value received, and the fix; missing fields use `Error::require_fields` | prose only |

## Comments

Make the code say it: rename, extract a function, add a type, add an `assert!`, or turn the rule
into a lint. What may stay:

- `///` docs that define the contract of a `pub` item (its fields and variants included), and `//!` docs
  on a crate root or `pub mod`.
- `///` on a type deriving clap (`Parser`, `Subcommand`, `Args`, `ValueEnum`) or `JsonSchema`: those docs
  are the `--help` or schema text users read.
- A `// <https://...>` link for a constraint we cannot change (a protocol, vendor or paper).

## Error messages are written for the agent who reads them next

Every error, panic, `expect` and check failure answers three things: what is wrong (with the actual
value), which rule it broke, and exactly what to do instead (name the file, type or function).
Clippy bans carry their fix in `reason`; `crates/rules` failures print the fix.

## When an agent makes the same mistake twice

1. Fix it at the highest level that works: architecture (one owner, one way, hide the wrong
   import), then a type, then a check whose error names the fix (`clippy.toml`, `ast-grep/rules`
   with tests in `ast-grep/tests`, or `crates/rules`), then a test. Prose is the last resort.
2. Prove the check fails on the real mistake before keeping it.
3. Add a row to the table above. A "prose only" row is a debt to pay down.
