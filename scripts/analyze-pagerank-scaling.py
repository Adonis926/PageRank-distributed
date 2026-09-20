#!/usr/bin/env python3
"""Combine comparable PageRank scale result directories into one CSV."""

from __future__ import annotations

import csv
import statistics
import sys
from pathlib import Path


def key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def metric(path: Path, wanted: str, statistic: str = "mean") -> float:
    with path.open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            if row["metric"] == wanted:
                return float(row[statistic])
    raise RuntimeError(f"missing {wanted} in {path}")


def main() -> int:
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} OUTPUT.csv RESULT_DIR...", file=sys.stderr)
        return 2

    output = Path(sys.argv[1])
    combined: list[dict[str, object]] = []
    for argument in sys.argv[2:]:
        root = Path(argument).resolve()
        config = key_values(root / "manifest/config.txt")
        with (root / "runs.csv").open(encoding="utf-8", newline="") as stream:
            runs = list(csv.DictReader(stream))
        if not runs or any(row["verify"] != "ok" for row in runs):
            raise RuntimeError(f"invalid formal samples in {root}")

        elapsed = [float(row["elapsed_ms"]) for row in runs]
        throughput = [float(row["medges_per_sec"]) for row in runs]
        combined.append(
            {
                "compute_workers_total": int(config["compute_workers_total"]),
                "compute_workers_per_vm": int(config["threads_per_node"]),
                "relay_threads_total": int(config["relay_threads_total"]),
                "guest_vcpus_per_vm": int(config["guest_vcpus"]),
                "total_guest_vcpus": 2 * int(config["guest_vcpus"]),
                "total_guest_memory_gib": 8,
                "host_cpu_overlap": int(config["host_cpu_overlap"]),
                "samples": len(runs),
                "elapsed_ms_mean": metric(root / "summary.csv", "elapsed_ms"),
                "elapsed_ms_p50": statistics.median(elapsed),
                "elapsed_ms_p95": metric(root / "summary.csv", "elapsed_ms", "p95"),
                "elapsed_ms_stddev": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
                "job_e2e_ms_mean": metric(root / "summary.csv", "job_e2e_ms"),
                "throughput_medges_s_mean": statistics.fmean(throughput),
                "data_cow_pages_mean": metric(root / "cow-summary.csv", "data_cow_pages"),
                "leaf_cow_pages_mean": metric(root / "cow-summary.csv", "leaf_cow_pages"),
                "messages_mean": metric(root / "communication-summary.csv", "messages"),
                "verify": "ok",
                "result_dir": str(root),
            }
        )

    combined.sort(key=lambda row: int(row["compute_workers_total"]))
    fields = list(combined[0])
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(combined)
    print(f"scales={len(combined)} output={output} verify=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
