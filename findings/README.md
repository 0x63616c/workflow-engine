# Simulation findings

`dst.jsonl` is appended to automatically whenever a simulation run fails
(see `crates/sim/src/findings.rs`). One JSON object per line:

| field      | meaning                                                         |
|------------|-----------------------------------------------------------------|
| `suite`, `seed`, `params` | what to rerun                                    |
| `failure`  | the panic message and location                                  |
| `revision`, `dirty`, `source` | the commit, whether the tree differed from it, and a hash of the source as it was |
| `status`, `note` | `open` until understood; then `fixed` with what the bug was |

Records are never removed: every seed here is a regression test.
