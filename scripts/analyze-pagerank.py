#!/usr/bin/env python3
"""Turn raw pagerank_hydra key=value logs into non-overlapping CSV views."""

from __future__ import annotations

import csv
import math
import re
import statistics
import sys
from pathlib import Path


TOKEN = re.compile(r"(?:^|\s)([A-Za-z0-9_]+)=([^\s]+)")


def parse_line(path: Path) -> dict[str, str]:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("pr_run_v1 "):
            values = dict(TOKEN.findall(line))
            values["source_log"] = str(path)
            return values
    raise RuntimeError(f"missing pr_run_v1 line in {path}")


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return ordered[low]
    return ordered[low] * (high - index) + ordered[high] * (index - low)


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} RESULT_DIR", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    paths = sorted((root / "runs").glob("run-*.coordinator.log"))
    if not paths:
        raise RuntimeError(f"no measured coordinator logs below {root / 'runs'}")

    rows: list[dict[str, object]] = []
    for path in paths:
        row: dict[str, object] = parse_line(path)
        row["sample"] = int(path.name.split("-")[1].split(".")[0])
        if row.get("status") != "ok" or row.get("verify") != "ok":
            raise RuntimeError(f"failed run in {path}")
        if abs(float(row["checksum"]) - 1.0) > 1e-9:
            raise RuntimeError(f"PageRank checksum is not one in {path}")
        rows.append(row)

    common = [
        "sample", "state", "run_id", "vertices", "degree",
        "remote_degree", "iterations", "chunks", "threads_per_node",
        "total_threads", "elapsed_ms", "medges_per_sec", "job_e2e_ms",
        "residual", "checksum", "verify", "source_log",
    ]
    write_csv(root / "runs.csv", rows, common)

    activation = [
        "sample", "run_id", "job_e2e_ms", "arm_ms", "graph_build_ms",
        "local_thread_create_ms", "start_publish_ms", "elapsed_ms",
        "source_log",
    ]
    write_csv(root / "activation.csv", rows, activation)

    transport = [
        "sample", "run_id", "elapsed_ms", "hydra_send_cpu_sum_ms",
        "hydra_recv_wait_sum_ms", "hydra_release_sum_ms", "messages",
        "payload_bytes", "source_log",
    ]
    write_csv(root / "transport.csv", rows, transport)

    cxlfork = [
        "sample", "run_id", "restore_wrapper_ms", "restore_ioctl_ms",
        "restore_kernel_ms", "restore_leaf_attach_ms",
        "restore_fallback_ms", "shared_leaf_count", "shared_data_pages",
        "fallback_pages", "node1_graph_checksum", "source_log",
    ]
    write_csv(root / "cxlfork.csv", rows, cxlfork)

    summary_rows: list[dict[str, object]] = []
    for metric in ("elapsed_ms", "job_e2e_ms"):
        values = [float(row[metric]) for row in rows]
        summary_rows.append(
            {
                "state": rows[0]["state"],
                "metric": metric,
                "samples": len(values),
                "mean": statistics.fmean(values),
                "p50": statistics.median(values),
                "p95": percentile(values, 0.95),
                "min": min(values),
                "max": max(values),
                "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
            }
        )
    write_csv(
        root / "summary.csv",
        summary_rows,
        ["state", "metric", "samples", "mean", "p50", "p95", "min", "max", "stddev"],
    )
    print(f"analyzed={len(rows)} result_dir={root} verify=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
