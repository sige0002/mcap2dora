//! Streaming MCAP → Arrow RecordBatch reader.
//!
//! This is the in-memory core: it decodes an MCAP rosbag into per-topic
//! Arrow record batches without writing anything to disk, so the batches can
//! be handed straight to dora-rs (`send_output`) or any Arrow consumer.

use crate::decode::{append, new_builder, ColB, Reader};
use crate::rosmsg::{self, timestamp_dt, FieldType, TypeReg};
use anyhow::{bail, Context, Result};
use arrow::array::{ArrayRef, LargeBinaryArray, RecordBatch, TimestampNanosecondArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use memmap2::Mmap;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    /// log_time / publish_time / raw serialized bytes per message
    Raw,
    /// every message field expanded into typed Arrow columns
    Decoded,
}

impl std::str::FromStr for Mode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "raw" => Ok(Mode::Raw),
            "decoded" => Ok(Mode::Decoded),
            other => bail!("mode must be raw or decoded, got {other}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReaderOptions {
    pub mode: Mode,
    /// a topic's pending rows are emitted as a batch once either limit is hit
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        ReaderOptions {
            mode: Mode::Decoded,
            max_batch_rows: 65536,
            max_batch_bytes: 64 << 20,
        }
    }
}

/// One Arrow batch for one topic. Batches of a topic are yielded in message
/// order; batches of different topics are interleaved in flush order.
pub struct TopicBatch {
    pub topic: String,
    pub msg_type: String,
    pub batch: RecordBatch,
}

#[derive(Default, Debug)]
pub struct Stats {
    pub messages: u64,
    pub skipped: u64,
    pub topics: usize,
    /// decoded mode requested but schema unusable → raw columns (topic, reason)
    pub fallback_topics: Vec<(String, String)>,
    /// decode error mid-stream; earlier batches of the topic may already have
    /// been yielded (topic, error)
    pub failed_topics: Vec<(String, String)>,
}

/// mmap a file for [`McapArrowReader::new`].
pub fn map_file(path: &Path) -> Result<Mmap> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(unsafe { Mmap::map(&f)? })
}

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

fn new_raw_builder() -> ColB {
    ColB::Bin {
        offsets: vec![0],
        data: Vec::new(),
    }
}

fn top_fields(reg: &TypeReg) -> &[(String, FieldType)] {
    &reg.defs[reg.top].fields
}

struct TopicState {
    schema: SchemaRef,
    kind: Kind,
    ts_log: Vec<i64>,
    ts_pub: Vec<i64>,
    pending_bytes: usize,
    failed: bool,
    fallback_reason: Option<String>,
    msg_type: String,
}

impl TopicState {
    fn new(ch: &mcap::Channel, mode: Mode) -> Result<TopicState> {
        let msg_type = ch
            .schema
            .as_ref()
            .map(|s| s.name.clone())
            .unwrap_or_default();
        let mut fallback_reason = None;

        let kind = if mode == Mode::Decoded {
            match Self::try_decoded(ch) {
                Ok(k) => k,
                Err(e) => {
                    fallback_reason = Some(format!("{e:#}"));
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
        meta.insert("mcap2dora:topic".to_string(), ch.topic.clone());
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

        Ok(TopicState {
            schema,
            kind,
            ts_log: Vec::new(),
            ts_pub: Vec::new(),
            pending_bytes: 0,
            failed: false,
            fallback_reason,
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

    fn flush(&mut self) -> Result<RecordBatch> {
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
                    arrays.push(Arc::new(LargeBinaryArray::new(
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        Buffer::from_vec(data),
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
                    .map(|(i, (_, ft))| std::mem::replace(&mut cols[i], new_builder(ft, &reg.defs)))
                    .collect();
                for (cb, (_, ft)) in old.into_iter().zip(fields.iter()) {
                    arrays.push(crate::decode::finish(cb, ft, &reg.defs));
                }
            }
        }
        self.pending_bytes = 0;
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }

    /// Decode failed: drop pending data and free the builders.
    fn mark_failed(&mut self) {
        self.failed = true;
        self.ts_log = Vec::new();
        self.ts_pub = Vec::new();
        self.pending_bytes = 0;
        self.kind = Kind::Raw {
            b: new_raw_builder(),
        };
    }
}

/// Streams per-topic Arrow batches out of an mmap'd MCAP buffer.
///
/// ```no_run
/// use mcap2dora::{map_file, McapArrowReader, ReaderOptions};
/// # fn main() -> anyhow::Result<()> {
/// let mapped = map_file(std::path::Path::new("bag_0.mcap"))?;
/// let mut reader = McapArrowReader::new(&mapped, ReaderOptions::default())?;
/// while let Some(tb) = reader.next_batch()? {
///     // tb.batch is an arrow RecordBatch — hand it to dora send_output etc.
///     println!("{}: {} rows", tb.topic, tb.batch.num_rows());
/// }
/// println!("{:?}", reader.stats());
/// # Ok(())
/// # }
/// ```
pub struct McapArrowReader<'a> {
    stream: Option<mcap::MessageStream<'a>>,
    topics: HashMap<String, TopicState>,
    opts: ReaderOptions,
    drain: Vec<String>,
    stats: Stats,
}

impl<'a> McapArrowReader<'a> {
    pub fn new(mapped: &'a [u8], opts: ReaderOptions) -> Result<Self> {
        Ok(McapArrowReader {
            stream: Some(mcap::MessageStream::new(mapped)?),
            topics: HashMap::new(),
            opts,
            drain: Vec::new(),
            stats: Stats::default(),
        })
    }

    /// Next batch, or Ok(None) once the bag is exhausted.
    pub fn next_batch(&mut self) -> Result<Option<TopicBatch>> {
        while let Some(stream) = self.stream.as_mut() {
            let Some(message) = stream.next() else {
                // end of stream: queue non-empty topics for the final flush
                self.stream = None;
                self.drain = self
                    .topics
                    .iter()
                    .filter(|(_, s)| !s.failed && s.pending_rows() > 0)
                    .map(|(t, _)| t.clone())
                    .collect();
                self.drain.sort_unstable_by(|a, b| b.cmp(a)); // pop() yields ascending
                break;
            };
            let m = message?;
            if !self.topics.contains_key(&m.channel.topic) {
                let st = TopicState::new(&m.channel, self.opts.mode)?;
                if let Some(reason) = &st.fallback_reason {
                    self.stats
                        .fallback_topics
                        .push((m.channel.topic.clone(), reason.clone()));
                }
                self.stats.topics += 1;
                self.topics.insert(m.channel.topic.clone(), st);
            }
            let st = self.topics.get_mut(&m.channel.topic).unwrap();
            if st.failed {
                self.stats.skipped += 1;
                continue;
            }
            if let Err(e) = st.push(&m.data, m.log_time, m.publish_time) {
                st.mark_failed();
                self.stats
                    .failed_topics
                    .push((m.channel.topic.clone(), format!("{e:#}")));
                self.stats.skipped += 1;
                continue;
            }
            self.stats.messages += 1;
            if st.pending_bytes >= self.opts.max_batch_bytes
                || st.pending_rows() >= self.opts.max_batch_rows
            {
                let batch = st.flush()?;
                return Ok(Some(TopicBatch {
                    topic: m.channel.topic.clone(),
                    msg_type: st.msg_type.clone(),
                    batch,
                }));
            }
        }
        while let Some(topic) = self.drain.pop() {
            let st = self.topics.get_mut(&topic).unwrap();
            if st.pending_rows() == 0 {
                continue;
            }
            let batch = st.flush()?;
            let msg_type = st.msg_type.clone();
            return Ok(Some(TopicBatch {
                topic,
                msg_type,
                batch,
            }));
        }
        Ok(None)
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Arrow schema of a topic seen so far (available after its first message).
    pub fn topic_schema(&self, topic: &str) -> Option<SchemaRef> {
        self.topics.get(topic).map(|s| s.schema.clone())
    }
}

impl<'a> Iterator for McapArrowReader<'a> {
    type Item = Result<TopicBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}
