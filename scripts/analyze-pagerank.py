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


def parse_prefix(path: Path, prefix: str) -> dict[str, str]:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(prefix + " "):
            return dict(TOKEN.findall(line))
    raise RuntimeError(f"missing {prefix} line in {path}")


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
        host_path = path.with_name(path.name.replace(".coordinator.log", ".host.log"))
        if host_path.exists():
            host = parse_prefix(host_path, "pr_host_v1")
            row["host_orchestration_e2e_ms"] = (
                float(host["orchestration_e2e_ns"]) / 1_000_000.0
            )
        else:
            row["host_orchestration_e2e_ms"] = "NA"
        row["activation_overhead_ms"] = (
            float(row["job_e2e_ms"]) - float(row["elapsed_ms"])
        )
        if row.get("status") != "ok" or row.get("verify") != "ok":
            raise RuntimeError(f"failed run in {path}")
        if abs(float(row["checksum"]) - 1.0) > 1e-9:
            raise RuntimeError(f"PageRank checksum is not one in {path}")
        if row.get("residual_mode") != "final-only":
            raise RuntimeError(f"unexpected residual policy in {path}")
        if row.get("signal_path") != "hydra-busy-local-futex":
            raise RuntimeError(f"unexpected cross-OS signal path in {path}")
        if row.get("notify") != "busy" or row.get("effective_notify") != "busy":
            raise RuntimeError(f"HydraRPC busy-poll path is not effective in {path}")
        if row.get("relay_cpu_policy") != "dedicated":
            raise RuntimeError(f"relay CPUs are not dedicated in {path}")
        if int(row["relay_threads_per_node"]) != int(row["threads_per_node"]):
            raise RuntimeError(f"relay/worker count mismatch in {path}")
        if int(row["local_futex_wait_calls"]) == 0:
            raise RuntimeError(f"local futex wait path was not exercised in {path}")
        if int(row["local_futex_wake_calls"]) == 0:
            raise RuntimeError(f"local futex wake path was not exercised in {path}")
        if int(row["local_futex_handoff_calls"]) == 0:
            raise RuntimeError(f"local futex handoff was not observed in {path}")
        if row.get("restore_policy") != "mow":
            raise RuntimeError(f"CXLfork restore is not MoW in {path}")
        if int(row["rank_pfn_pages"]) != 0:
            if int(row["rank_pfn_pages"]) != int(row["expected_rank_cow_pages"]):
                raise RuntimeError(f"rank PFN inspection is incomplete in {path}")
            if int(row["rank_pfn_changed_pages"]) != int(row["rank_pfn_pages"]):
                raise RuntimeError(f"not all PageRank rank pages performed CoW in {path}")
        if int(row["residual_syncs"]) != 1:
            raise RuntimeError(f"residual synchronization count mismatch in {path}")
        local_sum = (
            float(row["node0_local_residual"])
            + float(row["node1_local_residual"])
        )
        global_residual = float(row["residual"])
        for field in ("node0_global_residual", "node1_global_residual"):
            if not math.isclose(
                float(row[field]), global_residual, rel_tol=1e-12, abs_tol=1e-12
            ):
                raise RuntimeError(f"{field} disagrees with global residual in {path}")
        if not math.isclose(
            local_sum, global_residual, rel_tol=1e-12, abs_tol=1e-12
        ):
            raise RuntimeError(f"local residual sum mismatch in {path}")
        rows.append(row)

    common = [
        "sample", "state", "backend", "notify", "effective_notify",
        "run_id", "vertices", "degree",
        "remote_degree", "iterations", "chunks", "threads_per_node",
        "total_threads", "compute_threads_total", "relay_threads_total",
        "runtime_threads_total", "relay_threads_per_node", "signal_path",
        "relay_cpu_policy",
        "elapsed_ms", "medges_per_sec", "job_e2e_ms",
        "activation_overhead_ms", "host_orchestration_e2e_ms",
        "residual_mode", "residual_syncs", "residual",
        "node0_local_residual", "node1_local_residual",
        "node0_global_residual", "node1_global_residual",
        "checksum", "verify", "source_log",
    ]
    write_csv(root / "runs.csv", rows, common)

    activation = [
        "sample", "run_id", "job_e2e_ms", "arm_ms", "graph_build_ms",
        "local_thread_create_ms", "start_publish_ms", "elapsed_ms",
        "activation_overhead_ms", "host_orchestration_e2e_ms",
        "source_log",
    ]
    write_csv(root / "activation.csv", rows, activation)

    transport = [
        "sample", "run_id", "notify", "effective_notify", "signal_path",
        "relay_threads_per_node", "elapsed_ms",
        "hydra_send_cpu_sum_ms", "hydra_recv_wait_sum_ms",
        "hydra_release_sum_ms", "hydra_wait_cpu_sum_ms",
        "hydra_wait_calls", "local_futex_wait_sum_ms",
        "local_futex_wake_sum_ms", "local_futex_wait_calls",
        "local_futex_wake_calls", "local_futex_handoff_sum_ms",
        "local_futex_handoff_calls", "local_futex_handoff_mean_us",
        "messages", "payload_bytes", "source_log",
    ]
    write_csv(root / "transport.csv", rows, transport)

    residual = [
        "sample", "run_id", "iterations", "residual_mode",
        "residual_syncs", "node0_local_residual", "node1_local_residual",
        "node0_global_residual", "node1_global_residual", "residual",
        "source_log",
    ]
    write_csv(root / "residual.csv", rows, residual)

    cxlfork = [
        "sample", "run_id", "restore_wrapper_ms", "restore_ioctl_ms",
        "restore_kernel_ms", "restore_leaf_attach_ms",
        "restore_fallback_ms", "shared_leaf_count", "shared_data_pages",
        "fallback_pages", "restore_policy", "expected_rank_cow_pages",
        "cxlfork_faults",
        "cxlfork_local_copy_pages", "cxlfork_leaf_copy_pages",
        "cxlfork_data_cow_ms", "cxlfork_leaf_materialize_ms",
        "cxlfork_cxl_to_local_copy_ms", "rank_pfn_pages",
        "rank_pfn_changed_pages", "rank_leaf_cow_count",
        "node1_graph_checksum", "source_log",
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

    breakdown_metrics = [
        ("job_e2e_ms", "parent_wall", "no"),
        ("elapsed_ms", "execution_wall_inside_job", "yes"),
        ("activation_overhead_ms", "job_minus_execution", "yes"),
        ("arm_ms", "nested_in_activation_overhead", "no"),
        ("local_thread_create_ms", "nested_in_activation_overhead", "no"),
        ("start_publish_ms", "nested_in_activation_overhead", "no"),
        ("restore_wrapper_ms", "nested_in_arm", "no"),
        ("restore_ioctl_ms", "nested_in_restore_wrapper", "no"),
        ("restore_kernel_ms", "nested_in_restore_ioctl", "no"),
        ("restore_leaf_attach_ms", "nested_in_restore_kernel", "no"),
        ("restore_fallback_ms", "nested_in_restore_kernel", "no"),
        ("graph_build_ms", "before_job_window", "no"),
        ("hydra_send_cpu_sum_ms", "sum_across_threads_inside_execution", "no"),
        ("hydra_recv_wait_sum_ms", "sum_across_threads_inside_execution", "no"),
        ("hydra_release_sum_ms", "sum_across_threads_inside_execution", "no"),
        ("hydra_wait_cpu_sum_ms", "nested_in_hydra_recv_wait_sum", "no"),
        ("local_futex_wait_sum_ms", "sum_across_workers_and_relays_inside_execution", "no"),
        ("local_futex_wake_sum_ms", "sum_across_workers_and_relays_inside_execution", "no"),
        ("local_futex_handoff_sum_ms", "sum_of_relay_publish_to_worker_resume_intervals", "no"),
        ("host_orchestration_e2e_ms", "host_ssh_exec_to_both_exit", "no"),
    ]
    breakdown_rows: list[dict[str, object]] = []
    for metric, scope, additive in breakdown_metrics:
        values = [
            float(row[metric]) for row in rows
            if row.get(metric) not in (None, "NA")
        ]
        if not values:
            continue
        breakdown_rows.append(
            {
                "metric": metric,
                "scope": scope,
                "additive_to_job_e2e": additive,
                "samples": len(values),
                "mean": statistics.fmean(values),
                "p50": statistics.median(values),
                "p95": percentile(values, 0.95),
                "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
            }
        )
    write_csv(
        root / "breakdown-summary.csv",
        breakdown_rows,
        ["metric", "scope", "additive_to_job_e2e", "samples", "mean",
         "p50", "p95", "stddev"],
    )
    print(f"analyzed={len(rows)} result_dir={root} verify=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
