# Shopping Sessions: native C++ pipeline

One C++20 source file (`src/main.cpp`) compiled into a static binary. The
image is `FROM scratch` and holds only that binary, so there is no interpreter
or dynamic loader to start. The binary reads `/data`, writes Parquet to
`/output/user_daily/` and `/output/campaign_daily/`, and exits 0.

## Results

| Check | Result |
| --- | --- |
| 36 edge-case fixtures (`make fixtures`) | 36/36 pass, amd64 and arm64 images |
| 1M-event dev set vs `dev-answer/` | exact match |
| Independent reference on 9 more seeds, 10M events, a Feb-29 window and IDs over 15 bytes | exact match |
| Dev set, 8 threads, Linux | about 31 ms inside the process, about 0.03 s wall |
| 10M events (917 MB of CSV), 8 threads, Linux | about 0.3 s wall |

The timings were measured in a Linux VM on Apple silicon (arm64). The scorer's
Xeon vCPUs will give different numbers.

## How it works

All phases run on `NCPU` threads with dynamic work distribution.

1. **Phase 0: products.** The threads split `products.csv` and insert into one
   lock-free table. Product IDs are unique, so a slot is claimed with a single
   CAS. Category 0 is `unknown`.
2. **Phase 1: parse.**
   - Input files are opened in parallel, and each one gets `POSIX_FADV_WILLNEED`
     so the kernel starts reading everything at once. Each thread reads 1 MB
     chunks with `pread` into its own reusable buffer. There is no `mmap`, so
     page faults can't serialize I/O on network or FUSE storage.
   - Phase 1 runs 2 × `NCPU` threads so that reads overlap parsing. The later
     phases use `NCPU` threads, and all barriers sleep in the kernel after a
     short spin, so a waiting thread never takes CPU from a working one.
   - A SIMD scanner (AVX2 on x86, NEON on ARM) builds a 64-bit mask of `,`,
     `"` and `\n` for each 64 bytes. The tokenizer walks those bits and checks
     the rules' quoting and field-count rules exactly. A missing newline at
     the end of a file counts as a line end, and `\r` stays ordinary data.
   - Timestamps use a per-thread cache of the last valid `YYYY-MM-DD`, so most
     F1/F2 values skip calendar validation and conversion.
   - Event copies are radix-scattered by an `event_id` hash into 48-byte
     records. Each record holds the user key and category, which are worked
     out while the line is still in cache.
   - Clicks are parsed first, then events.
   - While the others parse, thread 0 builds the union-find over `links/`,
     naming each person by the byte-wise smallest ID.
3. **Phase 2: dedup.** Each `event_id` partition is deduplicated in an
   L2-sized table that keeps the copy with the smallest `ingest_ts`. A copy is
   validated only if it wins, so dedup comes before validation as the rules
   require. Winners and clicks are mapped user → person through a compact
   prefetched map, then scattered by person hash.
4. **Phase 3: sessions.**
   - Each person partition groups events by person and sorts them by `ts`.
     A new session starts after a gap of more than 30 minutes.
   - Clicks are sorted by `(ts, click_id desc)`. A sliding 7-day window gives
     the crediting click: the latest one, with the smallest `click_id` on a
     tie. Per-campaign counts in the same window give the assist set, so each
     session costs O(1) plus the number of distinct campaigns it has.
   - Duplicate clicks are identical, so they cannot change the result and
     are never deduplicated.
5. **Output.** A minimal Parquet writer (PLAIN encoding, uncompressed, `DATE`
   logical type, nullable `top_category`). Each thread writes its own
   `user_daily` part. Thread 0 merges the per-thread campaign × day counters
   and writes `campaign_daily` at the same time.

IDs are compared through a 128-bit key. IDs of up to 15 bytes are packed into
the key exactly, as their bytes plus their length, so equality is exact and the
name can be decoded from the key. Longer IDs are hashed into a separate key
space and keep a pointer to their bytes.

## Run and test

```sh
make build fixtures                 # amd64 image, all 36 fixtures (needs Docker buildx)
make build fixtures PLATFORM=linux/arm64   # native build on Apple silicon
make dev run-dev check-dev          # 1M-event dev set (needs `generate` on PATH)
make local-fixtures                 # no Docker: build with the host compiler
```

`SS_TIMING=1` prints per-phase timings to stderr.
