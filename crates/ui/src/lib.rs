//! The WFE management UI.
//!
//! A server-rendered site (axum + maud, no build step, no JavaScript
//! framework) over the same HTTP API the CLI and SDK use. It manages any
//! number of cells and shows a simulation fleet's results. One script
//! (`app.js`) turns links and forms into in-place updates; every route
//! still answers with a whole page.
//!
//! There is no authentication yet: bind it to a trusted interface.

mod cells;
mod deployments;
mod dst;
mod view;
mod workflows;

use axum::Router;
use axum::http::header;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use wfe_core::Error;
use wfe_core::api::{ApiRequest, ApiResponse, ApiResult, CellInfo};
use wfe_server::NodeStatus;

#[derive(Clone, Debug)]
pub struct CellConfig {
    /// What the UI calls the cell.
    pub name: String,
    /// HTTP addresses of the cell's nodes.
    pub urls: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct UiConfig {
    pub listen: SocketAddr,
    pub cells: Vec<CellConfig>,
    /// Address of a `wfe dst run --serve`.
    pub dst: Option<String>,
}

pub(crate) struct Cell {
    pub name: String,
    pub urls: Vec<String>,
    http: reqwest::Client,
}

impl Cell {
    pub(crate) async fn api(&self, request: ApiRequest) -> ApiResult {
        let mut last = Error::unavailable("the cell has no addresses");
        for url in &self.urls {
            let url = format!("{}/api", url.trim_end_matches('/'));
            match self.http.post(&url).timeout(Duration::from_secs(12)).json(&request).send().await {
                Ok(response) => match response.json::<ApiResult>().await {
                    Ok(result) => return result,
                    Err(e) => last = Error::unavailable(format!("{url}: {e}")),
                },
                Err(e) => last = Error::unavailable(format!("{url}: {}", root_cause(&e))),
            }
        }
        Err(last)
    }

    pub(crate) async fn describe(&self) -> Result<CellInfo, Error> {
        let response = self.api(ApiRequest::DescribeCell).await?;
        if let ApiResponse::Cell(info) = response { Ok(*info) } else { Err(unexpected(&response)) }
    }

    pub(crate) async fn nodes(&self) -> Vec<(String, Option<NodeStatus>)> {
        let mut out = Vec::new();
        for url in &self.urls {
            let status = async {
                let response = self
                    .http
                    .get(format!("{}/status", url.trim_end_matches('/')))
                    .timeout(Duration::from_secs(3))
                    .send()
                    .await
                    .ok()?;
                response.json::<Option<NodeStatus>>().await.ok()?
            }
            .await;
            out.push((url.clone(), status));
        }
        out
    }

    pub(crate) fn client(&self) -> wfe_sdk::Client {
        wfe_sdk::Client::connect(&self.urls.join(","))
    }
}

fn root_cause(e: &reqwest::Error) -> String {
    let mut source: &dyn std::error::Error = e;
    while let Some(next) = source.source() {
        source = next;
    }
    source.to_string()
}

pub(crate) fn unexpected(response: &ApiResponse) -> Error {
    Error::internal(format!("unexpected response: {response:?}"))
}

pub(crate) struct Ui {
    pub cells: Vec<Arc<Cell>>,
    pub dst: Option<String>,
    pub http: reqwest::Client,
}

pub(crate) type App = Arc<Ui>;

impl Ui {
    pub(crate) fn cell(&self, name: &str) -> Option<Arc<Cell>> {
        self.cells.iter().find(|c| c.name == name).cloned()
    }

    fn cell_names(&self) -> Vec<String> {
        self.cells.iter().map(|c| c.name.clone()).collect()
    }

    pub(crate) fn nav(&self, section: &'static str) -> view::Nav {
        view::Nav { section, cells: self.cell_names(), polls_for_changes: true, ..view::Nav::default() }
    }

    pub(crate) fn cell_nav(&self, cell: &str, tab: &'static str) -> view::Nav {
        view::Nav {
            section: "cells",
            cells: self.cell_names(),
            cell: Some(cell.to_string()),
            tab,
            leaf: None,
            polls_for_changes: true,
        }
    }

    pub(crate) async fn describe_all(&self) -> Vec<(Arc<Cell>, Result<CellInfo, Error>)> {
        let tasks: Vec<_> = self
            .cells
            .iter()
            .map(|cell| {
                let cell = cell.clone();
                tokio::spawn(async move { cell.describe().await })
            })
            .collect();
        let mut out = Vec::new();
        for (cell, task) in self.cells.iter().zip(tasks) {
            out.push((cell.clone(), task.await.unwrap_or_else(|e| Err(Error::internal(e.to_string())))));
        }
        out
    }
}

pub(crate) fn done(back: &str, outcome: Result<String, Error>) -> Response {
    let (key, message) = match outcome {
        Ok(message) => ("ok", message),
        Err(e) => ("err", format!("{} ({})", e.message, e.code.as_str())),
    };
    let join = if back.contains('?') { '&' } else { '?' };
    Redirect::to(&format!("{back}{join}{key}={}", view::enc(&message))).into_response()
}

pub(crate) fn not_found(app: &Ui, what: &str) -> Response {
    let body = maud::html! {
        div.panel {
            (view::empty(view::icons::SEARCH, "Not found", maud::html! {
                span { (what) }
                a.btn href="/" { "Back to overview" }
            }))
        }
    };
    let nav = app.nav("").polling(false).leaf("Not found");
    (axum::http::StatusCode::NOT_FOUND, view::page("Not found", &nav, &view::Flash::default(), body)).into_response()
}

pub fn router(config: UiConfig) -> Router {
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .build()
        .expect("a client with only a connect timeout set always builds");
    let cells = config
        .cells
        .iter()
        .map(|c| Arc::new(Cell { name: c.name.clone(), urls: c.urls.clone(), http: http.clone() }))
        .collect();
    let app: App = Arc::new(Ui { cells, dst: config.dst.clone().map(|u| u.trim_end_matches('/').to_string()), http });
    Router::new()
        .route("/", get(cells::overview))
        .route("/replicate", post(cells::replicate))
        .route("/failover", post(cells::failover))
        .route("/cells/{cell}", get(cells::detail))
        .route("/cells/{cell}/replication", post(cells::replication))
        .route("/cells/{cell}/role", post(cells::role))
        .route("/cells/{cell}/workflows", get(workflows::list))
        .route("/cells/{cell}/workflows/start", post(workflows::start))
        .route("/cells/{cell}/workflow", get(workflows::detail))
        .route("/cells/{cell}/workflow/act", post(workflows::act))
        .route("/cells/{cell}/deployments", get(deployments::list))
        .route("/cells/{cell}/deployments/act", post(deployments::act))
        .route("/dst", get(dst::dashboard))
        .route("/assets/style.css", get(|| async { ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], view::CSS) }))
        .route("/assets/app.js", get(|| async { ([(header::CONTENT_TYPE, "text/javascript; charset=utf-8")], view::JS) }))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

/// Serve the UI until the process exits.
pub async fn serve(config: UiConfig) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    tracing::info!(url = %format!("http://{}", listener.local_addr()?), cells = config.cells.len(), dst = ?config.dst, "management UI");
    axum::serve(listener, router(config)).await?;
    Ok(())
}
