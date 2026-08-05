// SPDX-License-Identifier: Apache-2.0

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use nodedb_graph::params::GraphAlgorithm;
use nodedb_lite::{Encryption, LiteConfig, NodeDbLite};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

const OPERATION_TIMEOUT_SECONDS: f64 = 300.0;
const DURABLE_PAGE_SIZE: usize = 64 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let dataset = PathBuf::from(args.next().ok_or("missing dataset directory")?);
    let output = PathBuf::from(args.next().ok_or("missing output directory")?);
    let database = PathBuf::from(args.next().ok_or("missing database path")?);
    if args.next().is_some() {
        return Err("usage: nodedb-lite-graphalytics DATASET OUTPUT DATABASE".into());
    }
    let dataset_name = dataset
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("invalid dataset directory")?;
    fs::create_dir_all(&output)?;
    if database.is_dir() {
        fs::remove_dir_all(&database)?;
    } else if database.exists() {
        fs::remove_file(&database)?;
    }

    let diagnostics_path = std::env::var_os("NODEDB_GRAPHALYTICS_DIAGNOSTICS").map(PathBuf::from);

    let db = NodeDbLite::open_at_path_with_config_and_page_size(
        &database,
        Encryption::Plaintext,
        LiteConfig::default(),
        DURABLE_PAGE_SIZE,
    )
    .await?;
    let (metrics, mut diagnostics) = db
        .graphalytics_import_with_diagnostics(
            &dataset.join(format!("{dataset_name}.v")),
            &dataset.join(format!("{dataset_name}.e")),
            diagnostics_path.is_some(),
        )
        .await?;
    enforce_timeout("load", metrics.load_seconds)?;
    enforce_timeout("prepare", metrics.prepare_seconds)?;

    let mut timings = Vec::new();
    for (name, algorithm) in [
        ("PR", Some(GraphAlgorithm::PageRank)),
        ("WCC", Some(GraphAlgorithm::Wcc)),
        ("BFS", None),
        ("LCC", Some(GraphAlgorithm::Lcc)),
        ("SSSP", Some(GraphAlgorithm::Sssp)),
        ("CDLP", Some(GraphAlgorithm::LabelPropagation)),
    ] {
        let (result, elapsed) = match algorithm {
            Some(algorithm) => {
                let start = Instant::now();
                let result = db.graphalytics_run(algorithm, "6")?;
                (result, start.elapsed().as_secs_f64())
            }
            None => {
                let start = Instant::now();
                let distances = db.graphalytics_bfs_distances("6")?;
                let elapsed = start.elapsed().as_secs_f64();
                (db.graphalytics_bfs_result(distances)?, elapsed)
            }
        };
        enforce_timeout(name, elapsed)?;
        write_result(&output.join(format!("{dataset_name}-{name}")), &result)?;
        timings.push((name, elapsed));
        println!("[NodeDB Lite] {name:<4} {elapsed:.6}s");
    }
    db.flush().await?;

    let mut summary = BufWriter::new(File::create(output.join("summary.json"))?);
    writeln!(summary, "{{")?;
    writeln!(summary, "  \"system\": \"nodedb-lite\",")?;
    writeln!(summary, "  \"dataset\": \"{dataset_name}\",")?;
    writeln!(summary, "  \"vertices\": {},", metrics.vertices)?;
    writeln!(summary, "  \"edges\": {},", metrics.edges)?;
    writeln!(summary, "  \"load_seconds\": {},", metrics.load_seconds)?;
    writeln!(
        summary,
        "  \"prepare_seconds\": {},",
        metrics.prepare_seconds
    )?;
    writeln!(summary, "  \"algorithms\": {{")?;
    for (index, (name, elapsed)) in timings.iter().enumerate() {
        let comma = if index + 1 == timings.len() { "" } else { "," };
        writeln!(summary, "    \"{name}\": {elapsed}{comma}")?;
    }
    writeln!(summary, "  }}")?;
    writeln!(summary, "}}")?;
    drop(summary);
    if let (Some(path), Some(diagnostics)) = (diagnostics_path, diagnostics.as_mut()) {
        diagnostics.set_database_bytes(directory_size_bytes(&database).ok());
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, diagnostics.to_json(dataset_name)?)?;
    }
    Ok(())
}

fn directory_size_bytes(path: &Path) -> std::io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    fs::read_dir(path)?.try_fold(0u64, |total, entry| {
        let entry = entry?;
        total
            .checked_add(directory_size_bytes(&entry.path())?)
            .ok_or_else(|| std::io::Error::other("PageDB directory size overflow"))
    })
}

fn enforce_timeout(operation: &str, seconds: f64) -> Result<(), Box<dyn std::error::Error>> {
    if seconds > OPERATION_TIMEOUT_SECONDS {
        return Err(format!(
            "{operation} took {seconds:.3}s and exceeded the {OPERATION_TIMEOUT_SECONDS:.0}-second operation timeout"
        )
        .into());
    }
    Ok(())
}

fn write_result(path: &Path, result: &QueryResult) -> Result<(), Box<dyn std::error::Error>> {
    let mut output = BufWriter::with_capacity(1 << 20, File::create(path)?);
    for row in &result.rows {
        if row.len() < 2 {
            return Err(format!("algorithm returned a row with {} columns", row.len()).into());
        }
        write!(output, "{} ", render_value(&row[0])?)?;
        writeln!(output, "{}", render_value(&row[1])?)?;
    }
    Ok(())
}

fn render_value(value: &Value) -> Result<String, Box<dyn std::error::Error>> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Integer(value) => Ok(value.to_string()),
        Value::Float(value) if value.is_infinite() && value.is_sign_positive() => {
            Ok("Infinity".to_string())
        }
        Value::Float(value) if value.is_infinite() => Ok("-Infinity".to_string()),
        Value::Float(value) => Ok(value.to_string()),
        _ => Err(format!("unsupported Graphalytics result value: {value:?}").into()),
    }
}
