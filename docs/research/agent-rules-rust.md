# Rules for an agent-written Rust codebase: "no comments" and what else to ban

Research notes for setting house rules in WFE, a codebase mostly written by agents.

- **Primary source for pstack:** `github.com/cursor/plugins`, path `harness/pstack/claude-code/`, latest
  commit touching it `9e278fe4d4ecd8bfe98b1b79bf4be662efb338cc` (2026-11-24). Files read:
  `skills/no-comments/SKILL.md`, `agents/comment-sicko.md`, `skills/correct/SKILL.md`,
  `docs/guide/08-principles.md`.
- **Secondary sources for the talk** (not watched; summaries only, and they disagree on details):
  [aiengineerguide.com](https://aiengineerguide.com/til/all-code-is-slop-except-mine-by-lauren-tan/),
  [youtubesummary.com](https://youtubesummary.com/summary/O_zcc6Xl12g).
- **Lint numbers** come from running `cargo clippy --workspace --all-targets` (clippy 0.1.99) on this
  workspace with each lint switched on, deduplicated by source location. "prod" excludes `tests/`
  and `crates/sim/`.
- Clippy behavior claimed below (`reason` rendering, `clippy.toml` lookup, banning `f64`) was
  tested in a throwaway crate on the same toolchain.

---

## 1. Lauren Tan's "no comments"

Lauren Tan (`poteto`) gave the talk "All Code Is Slop (Except Mine)" at the AI Engineer Code Summit.
The practice ships as `/no-comments` in **pstack**, her agent skill stack. Her own GitHub has no
pstack repo. It lives in Cursor's `cursor/plugins` repo.

### How the skill works

`/no-comments` spawns a reviewer subagent, **Comment Sicko**, over the current diff against `main`.
The reviewer deletes comments and flags the code that made a comment seem necessary. The main
agent then fixes that code. It touches comments only and never writes application code.

What Comment Sicko lets live (its "only leash", verbatim):

> - Legal or license headers.
> - Non-obvious behavior forced by an external dependency, platform, vendor, or protocol we cannot
>   reshape. Surprises in our own code are meat. Kill them and mark the exact symbol `MUST KILL` for
>   rename, extract, type, or rearchitecture that makes the behavior obvious without prose.
> - `// prettier-ignore`. Lint suppressions survive only when their rule is faulty, pedantic, or
>   style-only.
> - Doc comments that define a public API contract.
> - Issue or RFC links that explain a constraint code cannot express.

Everything else dies, including "narration, banners, commented-out corpses, workaround sermons."
Words like `IMPORTANT`, `do not remove`, `too risky` and `fine for now` are "scent, not conviction".
The reviewer traces the code to check the claim. "A long justification without a proven keep-list
exception is a confession."

After the review, the main agent:

1. Fixes trivial flags directly: deletes the dead path, drops the parameter, or calls the real API.
2. Runs `/architect` when a fix needs a new shape, then implements "the smallest root-cause fix in
   scope". "Remove every named workaround … Never bolt on symptom guards."
3. Handles constraint comments (`do not remove`, `talk to X before changing`) like this: "Offer the
   cheapest in-scope type, runtime, test, or CI lint … If approved, encode then delete."

### Why ban comments

Her reasons, as reported by the summaries of the talk:

- **Comments go stale.** Nothing fails when a comment stops being true.
- **Comments can't be enforced.** In the talk's four layers (codebase → static analysis →
  verification → prose), comments sit with AGENTS.md at the bottom, as "anything that can't be
  enforced".
- **Agents use them as alibis.** An agent writes a comment to justify a quick fix instead of fixing
  the cause, and the next agent reads that comment as a reason to keep the bad fix.

### The part that matches "two mistakes, then a rule"

pstack's `/correct` skill is the same idea as our rule. Verbatim:

> Group the mistakes into classes. A class counts once it has happened twice.

It fixes each class at the highest level that works:

1. **Architecture.** "Give each piece of state one owner and each task one supported way. Hide
   internals so the wrong import fails. Replace hand-synced lists with one source of truth. Delete
   old ways and dead code an agent would copy."
2. **Types**, so the bad state can't be written. "If bad code still compiles, add a lint or CI
   check whose error names the file, type, or function to use instead. If the pattern is already
   common, fail only when a change adds more."
3. **Tests** of behavior.
4. **Docs or agent rules last**, and only for judgment calls. "Nothing fails when an agent skips
   them."

Exceptions to a rule go "on the offending line with a reason, an expiry date, and a human's
approval". The agent instruction file keeps a table that pairs each rule with what enforces it.
"If the rule was already there and nothing enforces it, that's a repeat, so fix it at the highest
level in the same change."

The skill's premise also explains why each rule has to be enforced: "Assume every contributor is an
agent that sees only the files it opened, copies the nearest example, and takes the shortest path
that compiles."

Her team bans more than comments. Per the summaries, React's `useEffect` is banned outside
designated framework code because it is easy to misuse. Imports between Electron's main and
renderer code are banned too.

---

## 2. What this means for WFE

The repo already has rules written as prose in comments. These are the first things to turn into
checks:

| Prose rule today | Where | Enforce with |
|---|---|---|
| "never `HashMap`/`HashSet` (iteration order is not deterministic)" | `crates/core/src/codec.rs:8` | `disallowed-types` |
| "no floats in replicated state" | `crates/core/src/codec.rs:9` | `disallowed-types` on `f32`/`f64` (tested: primitives work) |
| "Nothing in here reads a clock, spawns a thread, or draws entropy" | `crates/core/src/lib.rs:3` | `disallowed-methods` |
| "Deterministic-core dependencies (no I/O, no clocks, no threads)" / "Shell dependencies … never used by the deterministic core" | root `Cargo.toml` | a dependency-closure check (section 4) |

Comment counts today: 231 `//` line comments, 15 trailing `//` comments and 1,101 `///`/`//!` doc
lines. Three of them are `// ------` banners in `vsr/src/replica.rs` and `sim/src/world.rs`. A banner
is a sign that a module wants to be split.

**Rust specifics for the keep list:**

- `///` on `pub` items is the public API contract (the `ErrorCode` variants are a good example).
  Keep it. Doc comments on private items and fields fall under the normal rules. For example,
  `/// The id the next event gets.` on a field means the field should be renamed `next_event_id`.
- Module docs that restate the code are a second source of truth. For example, the node layout in
  `engine/src/workflow.rs` (`h/<event id>`, `a/<…>`) restates `hist()` and `act_path()`. They will
  drift apart.
- Links to the VSR paper, the TigerBeetle source or an RFC for a protocol rule we cannot reshape:
  keep them.
- We forbid `unsafe`, so `// SAFETY:` comments never come up.

Rust has no built-in "no comments" lint (`missing_docs` is the opposite). Enforce it with the
`/no-comments` review on each diff, plus the ratchet in section 4.

---

## 3. What to ban in Rust, and why

The guiding question for each ban: is this a mistake an agent will make, which the compiler accepts,
and which costs us later? The codebase has no commits yet. Rules are cheapest to adopt now, while
there is no legacy code to grandfather in.

### Tier 1: ban now (cheap, and each catches a real bug class)

| Ban | Hits today (prod / test+sim) | Why |
|---|---|---|
| `disallowed_types`: `HashMap`, `HashSet`, `f32`, `f64` in deterministic crates | 0 / 0 | Replay depends on a deterministic iteration order. Floats can give different results across platforms (libm, FMA), so replicas could disagree. |
| `disallowed_methods`: `SystemTime::now`, `Instant::now`, `thread::spawn`, `env::var`, OS randomness in deterministic crates | 0 in core crates | Any of these breaks simulation. Time and randomness come in as parameters (`Timestamp`, `Prng`). |
| `unsafe_code = "forbid"` | 0 / 0 | Not needed. Forbidding it removes a whole class of review. |
| `allow_attributes` + `allow_attributes_without_reason` | 2 / 0 | Forces `#[expect(lint, reason = "…")]`. `expect` errors once the lint stops firing, so stale exceptions can't pile up. This is pstack's "exceptions go on the offending line with a reason". |
| `todo`, `unimplemented`, `dbg_macro` | 0 / 0 | Placeholders that compile. An agent will "finish later" and never come back. |
| `wildcard_enum_match_arm` | 28 / 11 | Single source of truth: adding a variant (a new `ErrorCode`, a new event) must make every `match` decide. `_ =>` silently opts into the wrong behavior. |
| `let_underscore_must_use` | 10 / 5 | `let _ = fallible()` throws away a `Result` without a trace. |
| `map_err_ignore` | 3 / 1 | `map_err(\|_\| …)` drops the cause, which is the part of the error an agent needs most. |
| `cast_possible_truncation`, `cast_possible_wrap`, `cast_sign_loss` | 76+1 / 30 | `x as u32` silently truncates. In a storage engine that means corrupt offsets. Use `u32::try_from(x)?`, or `expect` with the invariant. |
| `cast_lossless` | 53 / 18 | Use `u64::from(x)` where a widening cast cannot lose data. Then every remaining `as` stands out. |
| `unwrap_used` | 88 / 46 | `.unwrap()` panics with no explanation. Allow it in tests (`allow-unwrap-in-tests`). In other code, use `.expect("<the invariant that makes this impossible>")` or `?`. |
| `overflow-checks = true` in `[profile.release]` | n/a | Already on in `profile.sim`. Silent wraparound in production is how corruption turns into a feature. |

### Tier 2: not now (too noisy, or the wrong trade)

| Lint | Hits | Why not |
|---|---|---|
| `arithmetic_side_effects` | 304 / 125 | Flags every `+`. Overflow checks plus the simulator catch the real cases more cheaply. |
| `indexing_slicing` | 181 / 73 | Flags every `buf[i]`. An out-of-bounds index already panics, and the simulator finds it. |
| `as_conversions` (all of them) | 213 / 81 | The Tier 1 cast lints cover the dangerous `as` casts. |
| `clippy::pedantic` as a group | hundreds | Its `missing_errors_doc` (208) and `missing_panics_doc` (84) demand *more* comments, the opposite of our rule. Pick individual pedantic lints instead. |
| `panic`, `expect_used` | 11, 53 | Don't ban these. In the deterministic core, a failed invariant should crash. The simulator turns panics into findings (`sim/src/findings.rs` records the message). Make the messages good instead (section 5). |
| `shadow_unrelated`, `too_many_lines` | 23, 3 | Style questions, not bug classes. Add one only if an agent makes the mistake twice. |

### Beyond clippy

- **`rust-toolchain.toml` pinning the toolchain.** `rust-version` says 1.89 but the local toolchain
  is 1.99, and lint sets differ between versions. Pin it so you, CI and every agent get the same
  errors.
- **`[workspace.lints]` as the one place lint levels live**, with `[lints] workspace = true` in every
  crate. An agent who copies a crate's `Cargo.toml` may drop that line, so check it (section 4).
- **`cargo fmt --check`** in the same gate. Formatting should never come up in review.
- **cargo-deny** for licenses, advisories and duplicate versions. Its `wrappers` option only limits
  which crates may depend *directly* on a crate. The determinism boundary needs the full dependency
  closure, so use the check in section 4.

---

## 4. Custom checks: one crate for the rules clippy can't express

Make a `crates/rules` crate (or `xtask`) with integration tests that run under `cargo test`. That
way the local check and CI are the same command. Candidate checks:

1. **Every crate uses the workspace lints.** Parse each `crates/*/Cargo.toml`.
2. **Determinism boundary.** Use `cargo metadata` to compute the dependency closure of `wfe-core`,
   `wfe-lsm`, `wfe-vsr`, `wfe-engine` and `wfe-gateway`. Fail if it contains `tokio`, `axum`,
   `reqwest`, `ureq` or `rand`. This replaces the prose comments in `Cargo.toml`.
3. **Comment ratchet.** Count `//` (non-doc) comments per file. Fail when a file's count goes above
   a checked-in baseline, and lower the baseline whenever it falls. pstack's rule for common
   patterns: "fail only when a change adds more".
4. **`expect` messages state an invariant.** For example, fail on one-word messages such as
   `expect("node")` and `expect("task")`. Today there are many like these.

Each check's failure message should follow the agent error format in section 5. For example:

```
crates/engine/Cargo.toml is missing `[lints] workspace = true`.
Every crate takes its lint levels from [workspace.lints] in the root Cargo.toml.
Fix: add these two lines to crates/engine/Cargo.toml:
    [lints]
    workspace = true
```

---

## 5. Error messages written for agents

An agent reads an error once, acts on it, and has nobody to ask. Each error answers three
questions: **what is wrong (with the actual value), which rule it broke, and the exact thing to do
instead**, naming the file, type or function.

**Clippy bans.** Put the fix in `reason`. Clippy prints it as a note directly under the error:

```
error: use of a disallowed method `std::time::SystemTime::now`
  = note: the deterministic core never reads the wall clock. Take `now: Timestamp` as a parameter; the shell (wfe-server) owns the clock.
```

Avoid `replacement` on `disallowed-types`. In testing, its auto-fix replaced
`use std::collections::HashMap;` with the bare path `std::collections::BTreeMap` and dropped generic
arguments. That is broken code an agent might apply blindly. `replacement` on a *method* rendered
fine.

**Runtime errors.** Some current messages and what they lack:

- `Error::invalid("deployment, build id and task queue are required")`: doesn't say which field was
  missing. Name the field and show the values received.
- `Error::internal(format!("unexpected response: {response:?}"))`: good that it includes the value.
  Also say what was expected.
- `expect("node")`, `panic!("deployment")`: say nothing. State the invariant that makes the failure
  impossible ("a chasm tree always contains its root node"). That sentence is also what lands in
  `findings/dst.jsonl` when the simulator trips it.

`ErrorCode::is_retryable` is the pattern to copy: the error tells the caller what to do *as data*,
not as prose. If messages keep coming out vague, change the `Error::invalid` constructor so the
caller must name the field.

---

## 6. Proposed configuration (not applied)

Root `Cargo.toml`:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
disallowed_methods = "deny"
disallowed_types = "deny"
allow_attributes = "deny"
allow_attributes_without_reason = "deny"
todo = "deny"
unimplemented = "deny"
dbg_macro = "deny"
wildcard_enum_match_arm = "deny"
let_underscore_must_use = "deny"
map_err_ignore = "deny"
unwrap_used = "deny"
cast_possible_truncation = "deny"
cast_possible_wrap = "deny"
cast_sign_loss = "deny"
cast_lossless = "deny"

[profile.release]
debug = "line-tables-only"
overflow-checks = true
```

Root `clippy.toml` (applies to every crate that has no `clippy.toml` of its own):

```toml
allow-unwrap-in-tests = true
allow-expect-in-tests = true
disallowed-types = [
  { path = "std::collections::HashMap", reason = "iteration order differs between runs, which breaks simulation replay. Use std::collections::BTreeMap." },
  { path = "std::collections::HashSet", reason = "iteration order differs between runs, which breaks simulation replay. Use std::collections::BTreeSet." },
  { path = "f32", reason = "no floats in replicated state: results can differ across platforms, so replicas could disagree. Use integers (for example basis points as u32)." },
  { path = "f64", reason = "no floats in replicated state: results can differ across platforms, so replicas could disagree. Use integers (for example basis points as u32)." },
]
disallowed-methods = [
  { path = "std::time::SystemTime::now", reason = "deterministic crates never read the wall clock. Take `now: Timestamp` as a parameter; wfe-server owns the clock." },
  { path = "std::time::Instant::now", reason = "deterministic crates never read a clock. Take time as a parameter so the simulator can control it." },
  { path = "std::thread::spawn", reason = "deterministic crates are single-threaded so a seed replays exactly. Move concurrency to wfe-server." },
  { path = "std::env::var", reason = "configuration comes in as a typed argument, not from the environment, so a simulation run is reproducible." },
]
```

The shell crates (`server`, `sdk`, `cli`, `codec`, `sim`) each get their own `clippy.toml`. A crate's
own file **replaces** the root file rather than merging with it (tested), so the shell crates'
files repeat the `allow-*-in-tests` keys and add their own bans, such as `println!` in `server`
("use `tracing`"). `FileDevice` in `wfe-core` is the one I/O exception. Mark it with
`#[expect(clippy::disallowed_methods, reason = "…")]`.

---

## 7. The two-mistake rule as a process

Keep a table at the top of `CLAUDE.md`/`AGENTS.md`:

| Rule | Enforced by | Added after |
|---|---|---|
| No `HashMap` in deterministic crates | `clippy.toml` disallowed-types | codec.rs prose rule |
| … | … | … |

When an agent makes the same mistake twice:

1. Fix it at the highest level that works: architecture, then a type, then a lint or check whose
   error names the fix, then a test, then prose.
2. Prove the new check fails on the real mistake (revert the fix and run it).
3. Add a row to the table. A row whose "Enforced by" says "this document" is a debt.

---

## 8. Tools and crates

Hit counts are from running each tool on this repo (ast-grep 0.51.3 and typos 1.52.2 run through
`npx`/`uvx` without installing them).

### Tools to adopt

| Tool | Installed | What it enforces |
|---|---|---|
| **ast-grep** | no | Custom rules written in YAML, with a `message` and a `note` aimed at agents, and `ast-grep test` cases showing each rule fires on the real mistake. Covers what clippy can't. A `kind: line_comment` rule that skips doc comments found 261 line comments (48 of them `// ---` banner lines). An `expect-states-invariant` rule found 57 vague messages such as `.expect("shard state")`. This replaces the hand-written grep tests in section 4. Keep section 4's `cargo metadata` checks. |
| **cargo-deny** | no | Licenses, security advisories, duplicate versions, git sources. `bans` with `wrappers` keeps `anyhow` (shells only) and `rand` (we have `Prng`) out of library crates. |
| **cargo-shear** | no | Unused dependencies, including declared but unused `[workspace.dependencies]`. Agents add a dependency, then stop using it. |
| **typos** with `locale = "en-us"` | no | Spelling, including British spellings (tested on a sample file with four of them; all four flagged). Configured in `_typos.toml`. |
| **cargo-nextest** | yes | Test runner with a clear summary of failures and each test in its own process. Doctests still need `cargo test --doc`. |
| **cargo-mutants** | yes | Finds tests that still pass when the code they check is broken. Run it occasionally on `core`/`lsm`/`vsr`, not on every commit (it is slow). |
| **just** | yes | One `just check` that you, agents and CI all run, so the gate is defined in one place. |

### Library crates to adopt

- **strum**: derive the enum ↔ string tables instead of writing them by hand. There are 114
  hand-written `=> "…"` arms, 59 of them in `core/src/api.rs`. `WorkflowStatus` has `as_str`
  *and* `parse` as two separate tables, while serde writes the variant name (`"Running"`). That is
  three spellings of one enum. Derive `IntoStaticStr` + `EnumString` + `VariantArray`, set one
  `serialize_all` casing, and add a test that loops over `VARIANTS` and checks each round-trips.
  *Done (2026-11-24):* `ErrorCode`, `WorkflowStatus`, `EventAttrs`, `ApiRequest` and the VSR
  `Message` derive their names; `crates/core/tests/names.rs` checks every variant round-trips.
  `WorkflowStatus` keeps serde's PascalCase because histories store it.
- **insta**: snapshot tests for agent-facing text: error messages, CLI output, and the JSON shapes on
  the wire. A wording change then shows up as a reviewed diff instead of drifting silently.

### Skip

- **proptest / quickcheck**: the seeded simulator and `Prng` already do this. A second randomness
  framework means two ways to do one thing.
- **miette**: built to render errors in a terminal. Our `Error` is wire data. Reconsider for
  `wfe-cli` only.
- **nutype / validator / garde**: a hand-written validated newtype (for example `TaskQueue` that
  rejects empty strings) is about 15 lines, and Boundary Discipline says to do this at the edge
  anyway.
- **dylint**: real custom compiler lints, but it needs a pinned nightly. ast-grep covers the same
  ground.
- **static_assertions**: `const _: () = assert!(…);` is built in.
- **cargo-udeps**: needs nightly. cargo-shear does the same job.
- **loom / shuttle / turmoil**: for concurrency and network simulation. Our core is single-threaded
  and has its own simulator.

---

## 9. Not verified

- The talk itself: only AI-generated and blog summaries were read. They disagree on whether
  comments are banned outright or "most" are, and on her employer.
- Whether pstack's other skills differ between the Cursor and Claude Code harness copies. Only the
  `claude-code` copies were read.
- The rule `reason` texts above are drafts. Check each against how the crate actually does it
  (for example, the real clock seam in `wfe-server`) before adopting it.
