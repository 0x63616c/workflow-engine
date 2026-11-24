use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;
use wfe_rules::{CRATES, FORBIDDEN_IN_REPLICATED, Kind, kind_of};

struct Workspace {
    members: Vec<Member>,
    names: BTreeMap<String, String>,
    runtime_deps: BTreeMap<String, Vec<String>>,
}

struct Member {
    id: String,
    name: String,
    dir: PathBuf,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn load() -> Workspace {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1"])
        .current_dir(workspace_root())
        .output()
        .expect("cargo metadata can run in the workspace that built this test");
    assert!(output.status.success(), "cargo metadata failed:\n{}", String::from_utf8_lossy(&output.stderr));
    let meta: Value = serde_json::from_slice(&output.stdout).expect("cargo metadata always prints valid JSON");

    let names: BTreeMap<String, String> = meta["packages"]
        .as_array()
        .expect("cargo metadata always has a packages array")
        .iter()
        .map(|p| (str_field(p, "id"), str_field(p, "name")))
        .collect();

    let member_ids: BTreeSet<String> = meta["workspace_members"]
        .as_array()
        .expect("cargo metadata always has a workspace_members array")
        .iter()
        .map(|id| id.as_str().expect("workspace member ids are always strings").to_owned())
        .collect();

    let members = meta["packages"]
        .as_array()
        .expect("cargo metadata always has a packages array")
        .iter()
        .filter(|p| member_ids.contains(&str_field(p, "id")))
        .map(|p| Member {
            id: str_field(p, "id"),
            name: str_field(p, "name"),
            dir: PathBuf::from(str_field(p, "manifest_path"))
                .parent()
                .expect("a manifest path always has a parent directory")
                .to_path_buf(),
        })
        .collect();

    let runtime_deps = meta["resolve"]["nodes"]
        .as_array()
        .expect("cargo metadata without --no-deps always resolves nodes")
        .iter()
        .map(|node| {
            let deps = node["deps"]
                .as_array()
                .expect("every resolved node has a deps array")
                .iter()
                .filter(|dep| {
                    dep["dep_kinds"]
                        .as_array()
                        .expect("every resolved dependency lists its kinds")
                        .iter()
                        .any(|k| k["kind"].is_null())
                })
                .map(|dep| str_field(dep, "pkg"))
                .collect();
            (str_field(node, "id"), deps)
        })
        .collect();

    Workspace { members, names, runtime_deps }
}

fn str_field(value: &Value, field: &str) -> String {
    value[field].as_str().unwrap_or_else(|| panic!("cargo metadata field `{field}` is always a string")).to_owned()
}

fn fail_if_any(problems: Vec<String>) {
    assert!(problems.is_empty(), "\n\n{}\n", problems.join("\n\n"));
}

#[test]
fn every_workspace_crate_is_classified() {
    let ws = load();
    let in_workspace: BTreeSet<&str> = ws.members.iter().map(|m| m.name.as_str()).collect();
    let mut problems = Vec::new();
    for member in &ws.members {
        if kind_of(&member.name).is_none() {
            problems.push(format!(
                "{name} ({dir}) is not listed in wfe_rules::CRATES.\n\
                 Every crate declares whether it runs inside a replica (Kind::Replicated), must replay from a seed \
                 (Kind::Deterministic), or does real I/O (Kind::Io); that choice decides which lint bans apply to it.\n\
                 Fix: add (\"{name}\", Kind::...) to CRATES in crates/rules/src/lib.rs. \
                 A Kind::Io crate also needs its own clippy.toml: copy crates/server/clippy.toml.",
                name = member.name,
                dir = member.dir.display(),
            ));
        }
    }
    for (name, _) in CRATES {
        if !in_workspace.contains(name) {
            problems.push(format!(
                "wfe_rules::CRATES lists {name}, which is not a workspace member.\n\
                 Fix: remove it from CRATES in crates/rules/src/lib.rs, or fix the name to match its Cargo.toml."
            ));
        }
    }
    fail_if_any(problems);
}

#[test]
fn every_crate_takes_its_lints_from_the_workspace() {
    let ws = load();
    let mut problems = Vec::new();
    for member in &ws.members {
        let manifest_path = member.dir.join("Cargo.toml");
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("{} listed by cargo metadata is readable: {e}", manifest_path.display()));
        let inherits = manifest
            .split("\n[")
            .any(|section| section.starts_with("lints]") && section.lines().any(|l| l.trim() == "workspace = true"));
        if !inherits {
            problems.push(format!(
                "{} does not inherit the workspace lints.\n\
                 Lint levels live in one place, [workspace.lints] in the root Cargo.toml.\n\
                 Fix: append these two lines to {}:\n    [lints]\n    workspace = true",
                member.name,
                manifest_path.display(),
            ));
        }
    }
    fail_if_any(problems);
}

#[test]
fn only_io_crates_have_their_own_clippy_toml() {
    let ws = load();
    let mut problems = Vec::new();
    for member in &ws.members {
        let Some(kind) = kind_of(&member.name) else { continue };
        let own = member.dir.join("clippy.toml");
        match (kind, own.exists()) {
            (Kind::Io, false) => problems.push(format!(
                "{} is Kind::Io but has no {}.\n\
                 Without it, clippy applies the root clippy.toml determinism bans (no clocks, no threads, no env) \
                 to a crate whose job is I/O.\n\
                 Fix: copy crates/server/clippy.toml to {}.",
                member.name,
                own.display(),
                own.display(),
            )),
            (Kind::Replicated | Kind::Deterministic, true) => problems.push(format!(
                "{} is {kind:?} but has its own {}.\n\
                 A crate's clippy.toml replaces the root one entirely, which switches off the determinism bans \
                 (HashMap, floats, clocks, threads, env) for this crate.\n\
                 Fix: delete {}. To allow one call, put #[expect(clippy::disallowed_methods, reason = \"...\")] on it.",
                member.name,
                own.display(),
                own.display(),
            )),
            (Kind::Io, true) | (Kind::Replicated | Kind::Deterministic, false) => {}
        }
    }
    fail_if_any(problems);
}

#[test]
fn replicated_crates_never_depend_on_io_crates() {
    let ws = load();
    let mut problems = Vec::new();
    for member in ws.members.iter().filter(|m| kind_of(&m.name) == Some(Kind::Replicated)) {
        let mut parent: BTreeMap<&str, &str> = BTreeMap::new();
        let mut queue = VecDeque::from([member.id.as_str()]);
        while let Some(id) = queue.pop_front() {
            for dep in ws.runtime_deps.get(id).into_iter().flatten() {
                if dep != &member.id && !parent.contains_key(dep.as_str()) {
                    parent.insert(dep, id);
                    queue.push_back(dep);
                }
            }
        }
        for &id in parent.keys() {
            let name = &ws.names[id];
            if !FORBIDDEN_IN_REPLICATED.contains(&name.as_str()) {
                continue;
            }
            let mut chain = vec![name.clone()];
            let mut at = id;
            while let Some(&up) = parent.get(at) {
                chain.push(ws.names[up].clone());
                at = up;
            }
            chain.reverse();
            problems.push(format!(
                "{} depends on {name}: {}.\n\
                 {} runs inside a replica, so it must not do I/O, read clocks, or draw OS randomness; \
                 the simulator replays it from a seed.\n\
                 Fix: remove the dependency and move that code into an Io crate (wfe-server, wfe-sdk, ...), \
                 passing the result in as data.",
                member.name,
                chain.join(" -> "),
                member.name,
            ));
        }
    }
    fail_if_any(problems);
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries {
        let path = entry.expect("a directory entry listed by read_dir is readable").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn declared_as_pub_mod(module: &Path) -> bool {
    let name = module.file_stem().and_then(|s| s.to_str()).expect("a Rust file name is UTF-8");
    let dir = module.parent().expect("a module file always has a parent directory");
    let parents = match dir.file_name().and_then(|s| s.to_str()) {
        Some("src") => vec![dir.join("lib.rs"), dir.join("main.rs")],
        Some(parent) => vec![dir.join("mod.rs"), dir.with_file_name(format!("{parent}.rs"))],
        None => Vec::new(),
    };
    parents.iter().filter_map(|p| std::fs::read_to_string(p).ok()).any(|source| {
        source
            .lines()
            .any(|l| l.trim() == format!("pub mod {name};") || l.trim().starts_with(&format!("pub mod {name} {{")))
    })
}

#[test]
fn module_docs_only_on_public_modules() {
    let ws = load();
    let mut problems = Vec::new();
    for member in &ws.members {
        let mut files = Vec::new();
        rust_files(&member.dir.join("src"), &mut files);
        rust_files(&member.dir.join("tests"), &mut files);
        for file in files {
            let source = std::fs::read_to_string(&file)
                .unwrap_or_else(|e| panic!("{} found by read_dir is readable: {e}", file.display()));
            if !source.lines().any(|l| l.trim_start().starts_with("//!")) {
                continue;
            }
            let crate_root = file.ends_with("src/lib.rs") || file.ends_with("src/main.rs");
            let in_src = file.starts_with(member.dir.join("src"));
            if crate_root || (in_src && declared_as_pub_mod(&file)) {
                continue;
            }
            problems.push(format!(
                "{} has `//!` module docs, but it is not a public module.\n\
                 `//!` documents the public API of a crate or a `pub mod`; private modules and test files say what \
                 they do through names, types and asserts.\n\
                 Fix: delete the `//!` lines and move anything that matters into names, an assert! or a test. If the \
                 module is public API, declare it `pub mod` and export it.",
                file.display()
            ));
        }
    }
    fail_if_any(problems);
}
