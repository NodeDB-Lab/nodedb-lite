// SPDX-License-Identifier: Apache-2.0

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use nodedb_graph::params::GraphAlgorithm;
use nodedb_lite::{Encryption, NodeDbLite};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

const OPERATION_TIMEOUT_SECONDS: f64 = 300.0;

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

    let db = NodeDbLite::open_at_path(&database, Encryption::Plaintext).await?;
    let metrics = db
        .graphalytics_import(
            &dataset.join(format!("{dataset_name}.v")),
            &dataset.join(format!("{dataset_name}.e")),
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
        let start = Instant::now();
        let result = match algorithm {
            Some(algorithm) => db.graphalytics_run(algorithm, "6")?,
            None => db.graphalytics_bfs("6")?,
        };
        let elapsed = start.elapsed().as_secs_f64();
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
    writeln!(summary, "  \"prepare_seconds\": {},", metrics.prepare_seconds)?;
    writeln!(summary, "  \"algorithms\": {{")?;
    for (index, (name, elapsed)) in timings.iter().enumerate() {
        let comma = if index + 1 == timings.len() { "" } else { "," };
        writeln!(summary, "    \"{name}\": {elapsed}{comma}")?;
    }
    writeln!(summary, "  }}")?;
    writeln!(summary, "}}")?;
    Ok(())
}

fn enforce_timeout(operation: &str, seconds: f64) -> Result<(), Box<dyn std::error::Error>> {
    if seconds > OPERATION_TIMEOUT_SECONDS {
        return Err(format!(
            "{operation} exceeded the {OPERATION_TIMEOUT_SECONDS:.0}-second operation timeout"
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
