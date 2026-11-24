export RUSTUP_TOOLCHAIN := `sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml`

default: check

# Everything that must pass before a change is done.
check: fmt-check lint test

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings
    ast-grep test --skip-snapshot-tests
    ast-grep scan
    typos
    cargo deny check
    cargo shear

test:
    cargo nextest run --workspace
    cargo test --workspace --doc

# Slow: finds tests that still pass when the code they check is broken.
mutants crate:
    cargo mutants -p {{crate}}
