//! CLI wrapper around the mcap2dora library.
//!
//!   convert — decode an mcap and write one Arrow IPC file per topic
//!   drain   — decode an mcap fully in memory (measures pure conversion speed)
//!   verify  — read back .arrow files and print row counts

use anyhow::{Context, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use clap::{Parser, Subcommand};
use mcap2dora::{map_file, McapArrowReader, Mode, ReaderOptions, Stats};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Parser)]
#[command(about = "Convert MCAP rosbags to Arrow for dora-rs shared memory")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Convert one mcap file into per-topic Arrow IPC files
    Convert {
        /// raw | decoded
        #[arg(long)]
        mode: Mode,
        /// output directory (created if missing)
        #[arg(long)]
        out: PathBuf,
        input: PathBuf,
    },
    /// Decode one mcap fully in memory, writing nothing (throughput measure)
    Drain {
        /// raw | decoded
        #[arg(long)]
        mode: Mode,
        input: PathBuf,
    },
    /// Read back every .arrow file in a directory and print row counts
    Verify { dir: PathBuf },
}

fn sanitize_topic(topic: &str) -> String {
    let s = topic.trim_start_matches('/').replace('/', "__");
    if s.is_empty() {
        "_root".to_string()
    } else {
        s
    }
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn print_stats(
    input: &Path,
    mode: Mode,
    sink: &str,
    input_bytes: u64,
    out_bytes: u64,
    wall: f64,
    stats: &Stats,
) {
    let mode = if mode == Mode::Raw { "raw" } else { "decoded" };
    for (t, reason) in &stats.fallback_topics {
        eprintln!("[fallback->raw] {t}: {reason}");
    }
    for (t, err) in &stats.failed_topics {
        eprintln!("[decode failed] {t}: {err} — topic excluded from output");
    }
    eprintln!(
        "{} [{}/{}]: {:.1} MB, {} msgs in {:.2}s — {:.0} MB/s, {:.0} kmsg/s, out {:.1} MB",
        input.display(),
        mode,
        sink,
        input_bytes as f64 / 1e6,
        stats.messages,
        wall,
        input_bytes as f64 / 1e6 / wall,
        stats.messages as f64 / wall / 1e3,
        out_bytes as f64 / 1e6
    );
    let failed: Vec<String> = stats
        .failed_topics
        .iter()
        .map(|(t, e)| format!("\"{}\"", json_escape(&format!("{t}: {e}"))))
        .collect();
    println!(
        "{{\"file\":\"{}\",\"mode\":\"{}\",\"sink\":\"{}\",\"input_bytes\":{},\"msgs\":{},\"skipped\":{},\"wall_s\":{:.3},\"mb_per_s\":{:.1},\"kmsg_per_s\":{:.1},\"out_bytes\":{},\"topics\":{},\"fallback_topics\":{},\"failed\":[{}]}}",
        json_escape(&input.display().to_string()),
        mode,
        sink,
        input_bytes,
        stats.messages,
        stats.skipped,
        wall,
        input_bytes as f64 / 1e6 / wall,
        stats.messages as f64 / wall / 1e3,
        out_bytes,
        stats.topics,
        stats.fallback_topics.len(),
        failed.join(",")
    );
}

fn convert(mode: Mode, out: &Path, input: &Path) -> Result<()> {
    std::fs::create_dir_all(out)?;
    let mapped = map_file(input)?;
    let input_bytes = mapped.len() as u64;

    let t0 = Instant::now();
    let mut reader = McapArrowReader::new(
        &mapped,
        ReaderOptions {
            mode,
            ..Default::default()
        },
    )?;
    let mut writers: HashMap<String, (PathBuf, FileWriter<BufWriter<File>>)> = HashMap::new();
    while let Some(tb) = reader.next_batch()? {
        if !writers.contains_key(&tb.topic) {
            let path = out.join(format!("{}.arrow", sanitize_topic(&tb.topic)));
            let f = File::create(&path).with_context(|| format!("create {}", path.display()))?;
            let schema = tb.batch.schema();
            let w = FileWriter::try_new(BufWriter::with_capacity(4 << 20, f), schema.as_ref())?;
            writers.insert(tb.topic.clone(), (path, w));
        }
        writers.get_mut(&tb.topic).unwrap().1.write(&tb.batch)?;
    }

    let failed: Vec<&String> = reader
        .stats()
        .failed_topics
        .iter()
        .map(|(t, _)| t)
        .collect();
    let mut out_bytes: u64 = 0;
    for (topic, (path, mut w)) in writers {
        if failed.contains(&&topic) {
            drop(w);
            let _ = std::fs::remove_file(&path);
        } else {
            w.finish()?;
            out_bytes += std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        }
    }
    let wall = t0.elapsed().as_secs_f64();
    print_stats(input, mode, "file", input_bytes, out_bytes, wall, reader.stats());
    Ok(())
}

fn drain(mode: Mode, input: &Path) -> Result<()> {
    let mapped = map_file(input)?;
    let input_bytes = mapped.len() as u64;

    let t0 = Instant::now();
    let mut reader = McapArrowReader::new(
        &mapped,
        ReaderOptions {
            mode,
            ..Default::default()
        },
    )?;
    let mut batches: u64 = 0;
    let mut rows: u64 = 0;
    while let Some(tb) = reader.next_batch()? {
        batches += 1;
        rows += tb.batch.num_rows() as u64;
    }
    let wall = t0.elapsed().as_secs_f64();
    eprintln!("drained {batches} batches / {rows} rows (nothing written)");
    print_stats(input, mode, "memory", input_bytes, 0, wall, reader.stats());
    Ok(())
}

fn verify(dir: &Path) -> Result<()> {
    let mut total_rows: u64 = 0;
    let mut files = 0;
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "arrow"))
        .collect();
    entries.sort();
    for path in entries {
        let f = File::open(&path)?;
        let reader = FileReader::try_new(BufReader::new(f), None)?;
        let schema = reader.schema();
        let topic = schema
            .metadata()
            .get("mcap2dora:topic")
            .cloned()
            .unwrap_or_default();
        let mtype = schema
            .metadata()
            .get("mcap2dora:type")
            .cloned()
            .unwrap_or_default();
        let mut rows: u64 = 0;
        let mut batches: u64 = 0;
        for batch in reader {
            let batch = batch?;
            rows += batch.num_rows() as u64;
            batches += 1;
        }
        println!(
            "{:60} rows={:8} batches={:3} cols={:3} topic={} type={}",
            path.file_name().unwrap().to_string_lossy(),
            rows,
            batches,
            schema.fields().len(),
            topic,
            mtype
        );
        total_rows += rows;
        files += 1;
    }
    println!("TOTAL: {files} files, {total_rows} rows");
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Convert { mode, out, input } => convert(mode, &out, &input),
        Cmd::Drain { mode, input } => drain(mode, &input),
        Cmd::Verify { dir } => verify(&dir),
    }
}
