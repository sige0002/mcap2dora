#!/usr/bin/env bash
# Runs inside the rust container. Converts every mcap under /rosbags in both
# modes, appending one JSON line per run to /app/results/bench_results.jsonl.
# Outputs are written to /bench_tmp and deleted after each measurement.
set -u
BIN=/app/target/release/mcap2dora
RESULTS=/app/results/bench_results.jsonl
LOG=/app/results/bench_stderr.log
mkdir -p /app/results
: > "$RESULTS"
: > "$LOG"

mapfile -t FILES < <(find -L /rosbags -name '*.mcap' | sort)
echo "benchmarking ${#FILES[@]} mcap files" >&2

for f in "${FILES[@]}"; do
    rel=${f#/rosbags/}
    id=$(dirname "$rel" | tr '/' '__')
    # warm the page cache once so raw/decoded see identical read conditions
    cat "$f" > /dev/null
    for mode in raw decoded; do
        # in-memory conversion (library path, nothing written)
        echo ">>> $rel [$mode/memory]" | tee -a "$LOG" >&2
        if ! "$BIN" drain --mode "$mode" "$f" >> "$RESULTS" 2>> "$LOG"; then
            echo "{\"file\":\"$rel\",\"mode\":\"$mode\",\"sink\":\"memory\",\"error\":true}" >> "$RESULTS"
            echo "!!! FAILED: $rel [$mode/memory]" | tee -a "$LOG" >&2
        fi
        # conversion to Arrow IPC files
        outdir=/bench_tmp/$id/$mode
        rm -rf "$outdir"
        mkdir -p "$outdir"
        echo ">>> $rel [$mode/file]" | tee -a "$LOG" >&2
        if ! "$BIN" convert --mode "$mode" --out "$outdir" "$f" >> "$RESULTS" 2>> "$LOG"; then
            echo "{\"file\":\"$rel\",\"mode\":\"$mode\",\"sink\":\"file\",\"error\":true}" >> "$RESULTS"
            echo "!!! FAILED: $rel [$mode/file]" | tee -a "$LOG" >&2
        fi
        rm -rf "$outdir"
    done
done
echo "bench done" >&2
