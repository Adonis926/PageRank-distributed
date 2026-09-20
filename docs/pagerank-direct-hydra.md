# PageRank direct-HydraRPC mode

This branch preserves the existing CXLfork lifecycle while providing a second
steady-state receive path that does not create per-worker relay threads.

## Lifecycle boundary

The direct path does not bypass or replace CXLfork:

1. VM0 publishes the initialized PageRank graph and rank arrays with CXLfork.
2. VM1 opens a restore context and executes `RESUME_V2`.
3. The restored graph checksum, shared-leaf metadata, MoW data-page CoW, and
   optional PFN/leaf validation remain mandatory.
4. Only after restore and validation do the PageRank compute workers start.
5. During PageRank iterations, each compute worker sends, receives, waits, and
   releases DATA/BARRIER messages directly through its HydraRPC queue.

The older `relay` mode remains available and unchanged. In that mode a
per-worker relay receives from HydraRPC and performs a process-local futex
handoff. Select the mode with `--receive-mode=relay|direct` or
`PR_RECEIVE_MODE=relay|direct`.

## 103 scaling profile

Use `config/pagerank-real-cxl-103-direct-scaling.env` for the direct path. In
this profile `PR_SCALE` is both the total compute-worker count and total guest
vCPU count across the two VMs. Each VM receives half of the vCPUs and 4 GiB of
memory. No relay vCPUs or relay threads are created.

For example, `PR_SCALE=16` means two 8-vCPU/4-GiB guests, eight compute workers
per guest, 16 compute workers in total, and 8 GiB aggregate guest memory.

Successful direct-mode logs must contain:

```text
receive_mode=direct signal_path=hydra-busy-direct
relay_threads_per_node=0 relay_cpu_policy=none
local_futex_handoff_calls=0 restore_policy=mow verify=ok
```

The result analyzer rejects a run if the direct path creates relay threads,
uses the relay futex handoff, bypasses MoW restore, fails checksum/residual
validation, or produces incomplete CXLfork correctness evidence.
