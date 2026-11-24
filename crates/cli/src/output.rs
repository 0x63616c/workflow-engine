use serde_json::{Value, json};
use wfe_core::Timestamp;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Format {
    /// One JSON document on stdout; an error is one JSON document on stderr with a non-zero exit code.
    Json,
    /// Aligned columns, for people.
    Table,
}

pub(crate) struct Output {
    pub json: Value,
    pub table: Option<Table>,
}

pub(crate) struct Table {
    pub columns: Vec<&'static str>,
    pub rows: Vec<Vec<String>>,
}

impl Output {
    pub(crate) fn json(value: impl serde::Serialize) -> Output {
        Output { json: readable(serde_json::to_value(value).unwrap_or(Value::Null)), table: None }
    }

    pub(crate) fn ok() -> Output {
        Output::json(json!({ "ok": true }))
    }

    pub(crate) fn with_table(mut self, columns: Vec<&'static str>, rows: Vec<Vec<String>>) -> Output {
        self.table = Some(Table { columns, rows });
        self
    }

    pub(crate) fn print(&self, format: Format) {
        match (format, &self.table) {
            (Format::Table, Some(table)) => print!("{}", table.render()),
            (Format::Table, None) => print!("{}", outline(&self.json, 0)),
            (Format::Json, _) => {
                println!("{}", serde_json::to_string_pretty(&self.json).expect("a serde_json::Value always serializes"))
            }
        }
    }
}

impl Table {
    pub(crate) fn render(&self) -> String {
        let mut widths: Vec<usize> = self.columns.iter().map(|c| c.len()).collect();
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        let line = |cells: Vec<&str>| {
            let mut out = String::new();
            for (i, cell) in cells.iter().enumerate() {
                out += cell;
                if i + 1 < cells.len() {
                    out += &" ".repeat(widths[i] - cell.chars().count() + 2);
                }
            }
            out.trim_end().to_string() + "\n"
        };
        let mut out = line(self.columns.to_vec());
        for row in &self.rows {
            out += &line(row.iter().map(String::as_str).collect());
        }
        out
    }
}

fn outline(value: &Value, depth: usize) -> String {
    let pad = "  ".repeat(depth);
    match value {
        Value::Object(map) => map
            .iter()
            .map(|(k, v)| match v {
                Value::Object(m) if !m.is_empty() => format!("{pad}{k}:\n{}", outline(v, depth + 1)),
                Value::Array(a) if !a.is_empty() => format!("{pad}{k}:\n{}", outline(v, depth + 1)),
                Value::Null
                | Value::Bool(_)
                | Value::Number(_)
                | Value::String(_)
                | Value::Array(_)
                | Value::Object(_) => format!("{pad}{k}: {}\n", scalar(v)),
            })
            .collect(),
        Value::Array(items) => items
            .iter()
            .map(|v| match v {
                Value::Object(_) | Value::Array(_) => format!("{pad}-\n{}", outline(v, depth + 1)),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => format!("{pad}- {}\n", scalar(v)),
            })
            .collect(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => format!("{pad}{}\n", scalar(value)),
    }
}

fn scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Object(_) => "{}".into(),
        Value::Array(_) => "[]".into(),
        Value::Null | Value::Bool(_) | Value::Number(_) => value.to_string(),
    }
}

pub(crate) use wfe_core::payload::readable;

pub(crate) fn time(t: Timestamp) -> String {
    t.rfc3339()
}

pub(crate) fn duration_ms(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms}ms"),
        1_000..60_000 => format!("{}.{}s", ms / 1_000, ms % 1_000 / 100),
        60_000..3_600_000 => format!("{}m{}s", ms / 60_000, ms % 60_000 / 1_000),
        _ => format!("{}h{}m", ms / 3_600_000, ms % 3_600_000 / 60_000),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_time() {
        assert_eq!(time(Timestamp::from_millis(0)), "-");
        assert_eq!(time(Timestamp::from_millis(1_795_515_330_000)), "2026-11-24T10:15:30Z");
        assert_eq!(time(Timestamp::from_millis(951_782_400_000)), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn payloads_read_as_json() {
        let p = wfe_core::Payload::json(&json!({"a": [1, 2]})).unwrap();
        let v = readable(
            json!({ "result": serde_json::to_value(&p).unwrap(), "none": serde_json::to_value(wfe_core::Payload::null()).unwrap() }),
        );
        assert_eq!(v, json!({ "result": {"a": [1, 2]}, "none": null }));
    }
}
