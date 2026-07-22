# Motivation benchmark 快速使用指南

本目录的 benchmark 用于测量 HydraRPC + porter + CXLfork + application-state bind
组成的远端 worker 激活路径。完整的研究边界、阶段定义、正式结果和限制见测试环境中的：

`/home/xtl/cxlfork/cxlfork-repro/docs/cohort-motivation-benchmark-guide-zh.md`

## 脚本

- `scripts/run-motivation-bench.sh`：build、部署 VM、发布/释放 template、执行 probe 和
  WordCount、收集 raw 日志、调用分析器和 checker；
- `scripts/analyze-motivation.py`：解析 `task_result_v3`、`hydra_task_v1`、
  `hydra_response_v1` 和 `master_job_v1`，生成逐 task/job CSV 和 summary；
- `scripts/check-motivation-results.py`：检查样本数、事件顺序、应用状态消费、policy 路径、
  WordCount output、slot release 和 dmesg；
- `scripts/test-state-binding.sh`：独立的 application-state correctness acceptance；
- `scripts/run-bench.sh`：旧通用 runner，已经兼容 V3 envelope，但不替代 Motivation
  runner 的排他统计和时钟校验。

## 最小 smoke

```bash
cd /home/xtl/cxlfork/mr-distributed
git switch cohort-motivation-control-plane-measurement

scripts/run-motivation-bench.sh \
  --output-dir=/tmp/motivation-smoke \
  --smoke
```

runner 会先让 master 初始化 Hydra shared layout，再启动 agent；两个 agent 都写出 ready
证据后，runner 才发布 start barrier。不要手工改回“agent 先 attach、master 后 init”，
否则 agent 可能附着上一轮的旧 connection layout。

## 1000 个 activation 样本

```bash
RESULT=/home/xtl/cxlfork/mr-static-results/motivation-control-plane/manual-$(date -u +%Y%m%dT%H%M%SZ)

scripts/run-motivation-bench.sh \
  --output-dir="$RESULT" \
  --samples=1000 --warmups=5 --activation-blocks=10 \
  --workers=4 \
  --modes=persistent,localfork,local-prefork,cxlfork-preopen,cxlfork-per-request \
  --template-bytes-list=4194304
```

10 个 block 轮换 mode 顺序，每个 mode 最终有 1000 个有效样本。每个 block 的 warmup
带 `MR_TRACE_F_WARMUP`，不会进入 summary。

## 100 个 WordCount job

```bash
scripts/run-motivation-bench.sh \
  --output-dir="$RESULT" --skip-activation --workers=4 \
  --template-bytes-list=4194304 \
  --wordcount-input=/home/xtl/cxlfork/mr-file-sim-smoke/input/input_0.txt \
  --wordcount-expected=/home/xtl/cxlfork/mr-file-sim-smoke/expected.txt \
  --wordcount-modes=localfork,cxlfork-preopen \
  --wordcount-repeats=100
```

两种 mode 以固定正序/逆序 block 交错。`job-summary.csv` 的独立重复单位是 job，不能把
同一 job 的 map/reduce task 当成多个 job 样本。

## 结果目录

```text
RESULT/
  metadata.txt
  run-manifest.csv
  tasks.csv
  summary.csv
  jobs.csv
  job-summary.csv
  validation.json
  system/
  raw/<scenario>/
```

只有当最后输出 `result check passed` 时结果才完整。若分析规则修正，可以从 raw 重跑：

```bash
python3 scripts/analyze-motivation.py "$RESULT"
python3 scripts/check-motivation-results.py "$RESULT"
```

## 重要计时规则

- 不得直接相减 VM0 和 VM1 的绝对 `CLOCK_MONOTONIC`；
- target activation 只用 VM1 同钟域事件；RTT 只用 VM0 同钟域事件；
- kernel V3 bucket 互斥且必须精确求和；
- kernel restore、resume ioctl 和 restore wrapper 是嵌套关系，不能相加；
- reap/refill 位于业务完成后，会影响完整响应 RTT，但不属于 worker 开始执行前；
- 四时间点 one-way 只有带 `±delay/2` 的辅助估计意义；
- 少于 500 个样本时分析器不输出 P99；100-job 只看 mean/P50/P95/stddev。

10.0.16.91 使用 QEMU host-backed 文件模拟共享 CXL 区域。结果适合软件路径和相对
breakdown，不得当作真实 CXL fabric 的绝对时延或带宽。
