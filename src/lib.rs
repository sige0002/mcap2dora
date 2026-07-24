//! mcap2dora — stream ROS 1/2 MCAP rosbags as Arrow record batches.
//!
//! The core API is [`McapArrowReader`]: it decodes an MCAP file into
//! per-topic Arrow [`arrow::record_batch::RecordBatch`]es fully in memory,
//! ready to be handed to dora-rs `send_output` (dora's shared-memory
//! transport carries Arrow data zero-copy to every subscriber).
//!
//! The `mcap2dora` CLI (see `src/main.rs`) is a thin wrapper that writes the
//! same batches to Arrow IPC files for caching / offline use.

pub mod decode;
pub mod rosmsg;
mod reader;

pub use memmap2::Mmap;
pub use reader::{map_file, McapArrowReader, Mode, ReaderOptions, Stats, TopicBatch};
