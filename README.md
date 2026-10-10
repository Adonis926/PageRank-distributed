# CXL-DE PageRank

This repository contains the distributed PageRank workload used to evaluate
CXL-DE. It runs one PageRank partition in each of two KVM guests, restores the
worker-side graph state with CXLfork, and exchanges cross-partition
contributions through HydraRPC shared-memory queues.

The artifact entry point is `pagerank_hydra`. The old WordCount/MapReduce
prototype is not part of the paper evaluation.

## Recommended checkout

Use the [CXL-DE superproject](https://github.com/Adonis926/CXL-DE) so that this
repository, HydraRPC, CXLfork, its guest kernel, cxlporter, and QEMU are checked
out at the exact versions validated together:

```bash
git clone git@github.com:Adonis926/CXL-DE.git
cd CXL-DE
./scripts/checkout-components.sh
```

On the Lux evaluation host, reuse the preserved VM images and validated build
artifacts, then verify the complete source and runtime closure:

```bash
./scripts/bootstrap-lux.sh
./scripts/status.sh
```

## Build PageRank

From the CXL-DE superproject:

```bash
./scripts/build.sh pagerank
```

The resulting executable is:

```text
pagerank/build-pagerank/pagerank_hydra
```

For a standalone source tree, provide HydraRPC and the installed CXLfork UAPI
headers explicitly and build only the PageRank target:

```bash
cmake -S . -B build-pagerank \
  -DHYDRARPC_ROOT=/path/to/HydraRPC \
  -DMR_ENABLE_CXLFORK=ON \
  -DCXLFORK_UAPI_INCLUDE_DIR=/path/to/cxlfork/headers
cmake --build build-pagerank --target pagerank_hydra -j"$(nproc)"
```

`MR_ENABLE_CXLFORK` is a historical CMake option name retained for source
compatibility. It enables the CXLfork UAPI used by PageRank; it does not select
a MapReduce workload.

## Run the CXL-DE experiment

Run these commands from the CXL-DE superproject on Lux. The scale is the total
number of PageRank compute workers and guest vCPUs across both guests:

```bash
# Validate paths, backend layout, CPU allocation, and QEMU parameters only.
./scripts/run-pagerank.sh 8 --check

# Run one warmup and five measured executions.
./scripts/run-pagerank.sh 8
```

Supported scales are `8`, `16`, `32`, `64`, and `128`. Each guest receives
half of the selected vCPUs and 4 GiB of DRAM.

The runner selects the shared-memory backend automatically:

- `reserved-dram`: the host was booted with the validated 64 GiB `memmap`
  reservation, exposed as `/dev/dax0.0`;
- `real-cxl`: `/dev/dax0.0` is backed by the physical CXL device.

The backend can also be selected explicitly:

```bash
./scripts/run-pagerank.sh 32 --backend reserved-dram
./scripts/run-pagerank.sh 32 --backend real-cxl
```

Do not run multiple scales concurrently. The launcher refuses to disturb an
unrelated QEMU process or an in-use shared-memory device and only clears the
locked device-relative range used by CXL-DE.

## Evaluated workload

The default experiment uses:

- 524,288 vertices in total, split equally between the two guests;
- 16 outgoing edges per vertex, including 2 remote edges;
- 100 PageRank iterations;
- direct HydraRPC receive mode with no relay threads;
- 1 correctness/warmup run followed by 5 measured runs;
- a fixed aggregate memory budget of 8 GiB.

VM0 acts as the coordinator and owns the first graph partition. VM1 restores
the second graph partition from the CXLfork template. During every iteration,
workers directly exchange remote contributions and barrier messages through
HydraRPC. A run is accepted only if restore metadata, graph checksums, final
rank checksum, residual, transport counters, and CXLfork reader cleanup all
pass validation.

## Results

Every invocation creates a new timestamped directory under:

```text
CXL-DE/results/pagerank-cxlfork-hydrarpc/
```

Important outputs include:

- `summary.csv`: wall-clock summary statistics;
- `runs.csv`: the five measured samples;
- `breakdown-summary.csv`: activation, CXLfork, and HydraRPC breakdowns;
- `cxlfork.csv`: restore, shared-leaf attachment, and CoW evidence;
- `transport.csv`: message and payload counters;
- `manifest/config.txt`: the effective workload and resource allocation;
- `manifest/qemu-vcpu-affinity.txt`: QEMU arguments and actual vCPU pinning;
- `runs/*.coordinator.log` and `runs/*.worker.log`: raw execution logs.

`elapsed_ms` is the comparable PageRank execution interval. `job_e2e_ms` also
includes CXLfork restore, thread creation, START activation, and final result
delivery. Thread-accumulated transport timings must not be added to either
wall-clock value.

## Source layout

```text
pagerank/                         PageRank coordinator, worker, graph, CXLfork glue
config/pagerank-*.env             locked experiment profiles
scripts/run-pagerank-manual.sh    safe top-level experiment driver
scripts/run-pagerank-cxlfork.sh   build, launch, validation, and collection
scripts/cxlde-shared-backend.sh   real-CXL/reserved-DRAM backend resolution
scripts/analyze-pagerank.py       raw-log to CSV analysis
docs/pagerank-*.md                detailed design and measurement notes
```

The repository retains some historical `mr_*` implementation files and an
`mr_hydra` target because the PageRank port reuses audited CXLfork wrappers and
common utilities from the earlier prototype. They are not invoked by the
CXL-DE PageRank runner and should not be used for reproducing the paper.
