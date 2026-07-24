//! mcap2dora — convert ROS 1/2 MCAP rosbags into Arrow IPC files that dora-rs
//! nodes can memory-map and publish zero-copy over dora's shared memory.
//!
//! Modes:
//!   raw     — per topic: log_time, publish_time, data (raw CDR bytes as LargeBinary)
//!   decoded — per topic: log_time, publish_time + every message field expanded
//!             into typed Arrow columns (uint8[] payloads become LargeBinary)

mod decode;
mod rosmsg;

use anyhow::{bail, Context, Result};
use arrow::array::{ArrayRef, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use clap::{Parser, Subcommand};
use decode::{append, finish, new_builder, ColB, Reader};
use memmap2::Mmap;
use rosmsg::{timestamp_dt, FieldType, TypeReg};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser)]
#[command(about = "Convert MCAP rosbags to Arrow IPC for dora-rs shared memory")]
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
        mode: String,
        /// output directory (created if missing)
        #[arg(long)]
        out: PathBuf,
        input: PathBuf,
    },
    /// Read back every .arrow file in a directory and print row counts
    Verify { dir: PathBuf },
}

const FLUSH_BYTES: usize = 64 << 20;
const FLUSH_ROWS: usize = 65536;

enum Kind {
    Raw {
        b: ColB,
    },
    Decoded {
        reg: TypeReg,
        ros1: bool,
        cols: Vec<ColB>,
    },
}

struct TopicState {
    path: PathBuf,
    writer: Option<FileWriter<BufWriter<File>>>,
    schema: Arc<Schema>,
    kind: Kind,
    ts_log: Vec<i64>,
    ts_pub: Vec<i64>,
    pending_bytes: usize,
    rows: u64,
    failed: Option<String>,
    fallback: bool,
    msg_type: String,
}

fn sanitize_topic(topic: &str) -> String {
    let s = topic.trim_start_matches('/').replace('/', "__");
    if s.is_empty() {
        "_root".to_string()
    } else {
        s
    }
}

fn top_fields(reg: &TypeReg) -> &[(String, FieldType)] {
    &reg.defs[reg.top].fields
}

impl TopicState {
    fn new(ch: &mcap::Channel, mode: &str, out: &Path) -> Result<TopicState> {
        let topic = ch.topic.clone();
        let msg_type = ch
            .schema
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_default();
        let mut fallback = false;

        let kind = if mode == "decoded" {
            match Self::try_decoded(ch) {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("[fallback->raw] {topic} ({msg_type}): {e:#}");
                    fallback = true;
                    Kind::Raw {
                        b: new_raw_builder(),
                    }
                }
            }
        } else {
            Kind::Raw {
                b: new_raw_builder(),
            }
        };

        let mut fields = vec![
            Field::new("log_time", timestamp_dt(), false),
            Field::new("publish_time", timestamp_dt(), false),
        ];
        match &kind {
            Kind::Raw { .. } => fields.push(Field::new("data", DataType::LargeBinary, false)),
            Kind::Decoded { reg, .. } => {
                for (name, ft) in top_fields(reg) {
                    fields.push(Field::new(
                        name.clone(),
                        rosmsg::arrow_type(ft, &reg.defs),
                        false,
                    ));
                }
            }
        }
        let mut meta = HashMap::new();
        meta.insert("mcap2dora:topic".to_string(), topic.clone());
        meta.insert("mcap2dora:type".to_string(), msg_type.clone());
        meta.insert(
            "mcap2dora:mode".to_string(),
            if matches!(kind, Kind::Raw { .. }) {
                "raw".to_string()
            } else {
                "decoded".to_string()
            },
        );
        meta.insert(
            "mcap2dora:message_encoding".to_string(),
            ch.message_encoding.clone(),
        );
        let schema = Arc::new(Schema::new_with_metadata(fields, meta));

        let path = out.join(format!("{}.arrow", sanitize_topic(&topic)));
        let f = File::create(&path).with_context(|| format!("create {}", path.display()))?;
        let writer = FileWriter::try_new(BufWriter::with_capacity(4 << 20, f), &schema)?;

        Ok(TopicState {
            path,
            writer: Some(writer),
            schema,
            kind,
            ts_log: Vec::new(),
            ts_pub: Vec::new(),
            pending_bytes: 0,
            rows: 0,
            failed: None,
            fallback,
            msg_type,
        })
    }

    fn try_decoded(ch: &mcap::Channel) -> Result<Kind> {
        let schema = match &ch.schema {
            Some(s) => s,
            None => bail!("no schema record"),
        };
        let text = String::from_utf8_lossy(&schema.data);
        let ros1 = match (schema.encoding.as_str(), ch.message_encoding.as_str()) {
            ("ros2msg", "cdr") => false,
            ("ros1msg", "ros1") => true,
            (se, me) => bail!("unsupported schema/message encoding: {se}/{me}"),
        };
        let reg = rosmsg::parse(&schema.name, &text, ros1)?;
        let cols = top_fields(&reg)
            .iter()
            .map(|(_, ft)| new_builder(ft, &reg.defs))
            .collect();
        Ok(Kind::Decoded { reg, ros1, cols })
    }

    fn pending_rows(&self) -> usize {
        self.ts_log.len()
    }

    fn push(&mut self, data: &[u8], log_time: u64, publish_time: u64) -> Result<()> {
        match &mut self.kind {
            Kind::Raw { b } => {
                if let ColB::Bin { offsets, data: d } = b {
                    d.extend_from_slice(data);
                    offsets.push(d.len() as i64);
                } else {
                    unreachable!()
                }
            }
            Kind::Decoded { reg, ros1, cols } => {
                let mut r = if *ros1 {
                    Reader::ros1(data)
                } else {
                    Reader::cdr(data)?
                };
                for ((_, ft), cb) in top_fields(reg).iter().zip(cols.iter_mut()) {
                    append(ft, cb, &mut r, &reg.defs)?;
                }
            }
        }
        self.ts_log.push(log_time as i64);
        self.ts_pub.push(publish_time as i64);
        self.pending_bytes += data.len();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let n = self.pending_rows();
        if n == 0 {
            return Ok(());
        }
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        arrays.push(Arc::new(TimestampNanosecondArray::from(std::mem::take(
            &mut self.ts_log,
        ))));
        arrays.push(Arc::new(TimestampNanosecondArray::from(std::mem::take(
            &mut self.ts_pub,
        ))));
        match &mut self.kind {
            Kind::Raw { b } => {
                let old = std::mem::replace(b, new_raw_builder());
                if let ColB::Bin { offsets, data } = old {
                    arrays.push(Arc::new(arrow::array::LargeBinaryArray::new(
                        arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(
                            offsets,
                        )),
                        arrow::buffer::Buffer::from_vec(data),
                        None,
                    )));
                } else {
                    unreachable!()
                }
            }
            Kind::Decoded { reg, cols, .. } => {
                let fields = top_fields(reg);
                let old: Vec<ColB> = fields
                    .iter()
                    .enumerate()
                    .map(|(i, (_, ft))| {
                        std::mem::replace(&mut cols[i], new_builder(ft, &reg.defs))
                    })
                    .collect();
                for (cb, (_, ft)) in old.into_iter().zip(fields.iter()) {
                    arrays.push(finish(cb, ft, &reg.defs));
                }
            }
        }
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.writer
            .as_mut()
            .expect("writer already closed")
            .write(&batch)?;
        self.rows += n as u64;
        self.pending_bytes = 0;
        Ok(())
    }

    /// Decode failed: drop the writer and delete the partial output file.
    fn abort(&mut self, err: String) {
        self.writer = None;
        let _ = std::fs::remove_file(&self.path);
        self.ts_log.clear();
        self.ts_pub.clear();
        self.pending_bytes = 0;
        self.failed = Some(err);
    }

    fn close(&mut self) -> Result<u64> {
        if self.failed.is_some() {
            return Ok(0);
        }
        self.flush()?;
        if let Some(mut w) = self.writer.take() {
            w.finish()?;
        }
        Ok(std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0))
    }
}

fn new_raw_builder() -> ColB {
    ColB::Bin {
        offsets: vec![0],
        data: Vec::new(),
    }
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn convert(mode: &str, out: &Path, input: &Path) -> Result<()> {
    if mode != "raw" && mode != "decoded" {
        bail!("mode must be raw or decoded");
    }
    std::fs::create_dir_all(out)?;
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mapped = unsafe { Mmap::map(&file)? };
    let input_bytes = mapped.len() as u64;

    let t0 = Instant::now();
    let mut topics: HashMap<String, TopicState> = HashMap::new();
    let mut msgs: u64 = 0;
    let mut skipped: u64 = 0;

    let stream = mcap::MessageStream::new(&mapped)?;
    for message in stream {
        let m = message?;
        if !topics.contains_key(&m.channel.topic) {
            let st = TopicState::new(&m.channel, mode, out)?;
            topics.insert(m.channel.topic.clone(), st);
        }
        let st = topics.get_mut(&m.channel.topic).unwrap();
        if st.failed.is_some() {
            skipped += 1;
            continue;
        }
        if let Err(e) = st.push(&m.data, m.log_time, m.publish_time) {
            eprintln!(
                "[decode failed] {} ({}): {e:#} — topic excluded from output",
                m.channel.topic, st.msg_type
            );
            st.abort(format!("{e:#}"));
            skipped += 1;
            continue;
        }
        msgs += 1;
        if st.pending_bytes >= FLUSH_BYTES || st.pending_rows() >= FLUSH_ROWS {
            st.flush()?;
        }
    }

    let mut out_bytes: u64 = 0;
    for st in topics.values_mut() {
        out_bytes += st.close()?;
    }
    let wall = t0.elapsed().as_secs_f64();

    let fallback: Vec<&String> = topics
        .iter()
        .filter(|(_, s)| s.fallback && s.failed.is_none())
        .map(|(t, _)| t)
        .collect();
    let failed: Vec<String> = topics
        .iter()
        .filter_map(|(t, s)| s.failed.as_ref().map(|e| format!("{t}: {e}")))
        .collect();

    // human-readable summary on stderr, machine-readable JSON on stdout
    eprintln!(
        "{} [{}]: {:.1} MB, {} msgs in {:.2}s — {:.0} MB/s, {:.0} kmsg/s, out {:.1} MB",
        input.display(),
        mode,
        input_bytes as f64 / 1e6,
        msgs,
        wall,
        input_bytes as f64 / 1e6 / wall,
        msgs as f64 / wall / 1e3,
        out_bytes as f64 / 1e6
    );
    println!(
        "{{\"file\":\"{}\",\"mode\":\"{}\",\"input_bytes\":{},\"msgs\":{},\"skipped\":{},\"wall_s\":{:.3},\"mb_per_s\":{:.1},\"kmsg_per_s\":{:.1},\"out_bytes\":{},\"topics\":{},\"fallback_topics\":{},\"failed\":[{}]}}",
        json_escape(&input.display().to_string()),
        mode,
        input_bytes,
        msgs,
        skipped,
        wall,
        input_bytes as f64 / 1e6 / wall,
        msgs as f64 / wall / 1e3,
        out_bytes,
        topics.len(),
        fallback.len(),
        failed
            .iter()
            .map(|f| format!("\"{}\"", json_escape(f)))
            .collect::<Vec<_>>()
            .join(",")
    );
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
        Cmd::Convert { mode, out, input } => convert(&mode, &out, &input),
        Cmd::Verify { dir } => verify(&dir),
    }
}
