use serde::{Deserialize, Serialize};
use wfe_sdk::prelude::*;

fn bad(e: impl std::fmt::Display) -> Failure {
    Failure::new(e.to_string()).kind("bad_input").non_retryable()
}

fn json<T: Serialize>(value: &T) -> Payload {
    Payload::json(value).expect("demo payloads are plain serde types, which always encode as JSON")
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Greet {
    name: String,
}

async fn greet(ctx: WfContext, input: Payload) -> WorkflowResult {
    let input: Greet = input.to_json::<Option<Greet>>().map_err(bad)?.unwrap_or_default();
    let name = if input.name.is_empty() { "world".to_string() } else { input.name };
    Ok(ctx.call("say_hello", json(&name)).await?)
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Order {
    items: Vec<String>,
    needs_approval: bool,
    approval_timeout_ms: u64,
}

#[derive(Serialize)]
struct Receipt {
    reserved: Vec<String>,
    approved_by: Option<String>,
    charge: serde_json::Value,
}

async fn order(ctx: WfContext, input: Payload) -> WorkflowResult {
    let input: Order = input.to_json::<Option<Order>>().map_err(bad)?.unwrap_or_default();
    let reservations = join_all(input.items.iter().map(|item| ctx.call("reserve", json(item))).collect()).await;
    let mut reserved = Vec::new();
    for r in reservations {
        reserved.push(r?.to_json::<String>().map_err(bad)?);
    }
    let mut approved_by = None;
    if input.needs_approval {
        let timeout = if input.approval_timeout_ms == 0 { 600_000 } else { input.approval_timeout_ms };
        match select(ctx.signal("approve"), ctx.timer(timeout)).await {
            Either::Left(who) => approved_by = Some(who.to_json::<String>().unwrap_or_else(|_| "someone".into())),
            Either::Right(_) => return Err(Failure::new("nobody approved the order in time").kind("timeout").into()),
        }
    }
    let charge = ctx.call("charge", json(&reserved.len())).await?.to_json().map_err(bad)?;
    Ok(json(&Receipt { reserved, approved_by, charge }))
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Sleepy {
    ms: u64,
}

async fn sleepy(ctx: WfContext, input: Payload) -> WorkflowResult {
    let input: Sleepy = input.to_json::<Option<Sleepy>>().map_err(bad)?.unwrap_or_default();
    if let Either::Right(_) = select(ctx.timer(input.ms.max(1)), ctx.cancelled()).await {
        return Err(WorkflowError::Canceled);
    }
    Ok(json(&format!("slept {} ms", input.ms.max(1))))
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Flaky {
    fail_times: u32,
}

async fn flaky(ctx: WfContext, input: Payload) -> WorkflowResult {
    let options = ActivityOptions {
        activity_type: "unreliable".into(),
        retry: Some(RetryPolicy {
            initial_interval_ms: 500,
            backoff_percent: 150,
            maximum_interval_ms: 5_000,
            maximum_attempts: 10,
        }),
        ..ActivityOptions::default()
    };
    Ok(ctx.activity(options, input).await?)
}

pub(crate) fn register(worker: &mut Worker, build: &str) {
    worker.workflow("greet", greet).workflow("order", order).workflow("sleepy", sleepy).workflow("flaky", flaky);
    let build = build.to_string();
    worker.activity("say_hello", move |_ctx, input: Payload| {
        let build = build.clone();
        async move {
            let name: String = input.to_json().map_err(bad)?;
            Ok(json(&serde_json::json!({ "greeting": format!("hello, {name}"), "worker_build": build })))
        }
    });
    worker.activity("reserve", |_ctx, input: Payload| async move {
        let item: String = input.to_json().map_err(bad)?;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        Ok(json(&item))
    });
    worker.activity("charge", |_ctx, input: Payload| async move {
        let items: u64 = input.to_json().map_err(bad)?;
        Ok(json(&serde_json::json!({ "items": items, "cents": items * 1_250 })))
    });
    worker.activity("unreliable", |ctx: ActivityContext, input: Payload| async move {
        let input: Flaky = input.to_json::<Option<Flaky>>().map_err(bad)?.unwrap_or_default();
        if ctx.attempt() <= input.fail_times {
            return Err(Failure::new(format!("attempt {} failed on purpose", ctx.attempt())).kind("flaky"));
        }
        Ok(json(&serde_json::json!({ "succeeded_on_attempt": ctx.attempt() })))
    });
}
