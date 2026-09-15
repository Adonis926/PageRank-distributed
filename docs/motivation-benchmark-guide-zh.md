# Motivation benchmark 快速使用指南

本目录的 benchmark 用于测量 HydraRPC + porter + CXLfork + application-state bind
组成的远端 worker 激活路径。完整的研究边界、阶段定义、正式结果和限制见测试环境中的：

`/home/xtl/cxlfork/cxlfork-repro/docs/cohort-motivation-benchmark-guide-zh.md`

## 脚本

- `scripts/run-motivation-bench.sh`：build、部署 VM、发布/释放 template、执行 probe 和
  WordCount、收集 raw 日志、调用分析器和 checker；
- `scripts/analyze-motivation.py`：解析 `task_result_v4`、`hydra_task_v1`、
  `hydra_response_v1` 和 `master_job_v1`，生成逐 task/job CSV 和 summary；
- `scripts/check-motivation-results.py`：检查样本数、事件顺序、应用状态消费、policy 路径、
  WordCount output、slot release 和 dmesg；
- `scripts/test-state-binding.sh`：独立的 application-state correctness acceptance；
- `scripts/run-bench.sh`：通用 MR 端到端 runner。当前版本支持 master/agent ready
  barrier、VM0 同钟域作业计时、自动发布/释放 reducer CXLfork template，以及
  input/map/reduce/output 排他阶段统计；
- `scripts/prepare-aligned-wordcount.sh`：生成与单机 Motivation MR 完全相同的确定性
  WordCount token stream、expected output 和 checksum manifest。

## 与单机 MR 对齐的端到端实验

103 上的权威双 VM profile 是：

`/home/xtl/cxlfork/cxlfork-repro/config/real-cxl-103-mr-e2e.env`

它使用 48+48 vCPU、20+20 GiB 私有 DRAM，VM0/VM1 分别绑定 NUMA node 0/1，
并保持经过验证的 DAX 分区不变。正式 workload 固定为 1 GiB、512 文件、
65,536 个词、48 mapper worker、48 reducer worker、128 partitions。

先生成正式输入（输入生成不在计时区间）：

```bash
cd /home/xtl/cxlfork/mr-distributed

scripts/prepare-aligned-wordcount.sh \
  --output-dir=/home/xtl/start/hpca-cxlfork/workloads/wordcount-1g-aligned \
  --input-mib=1024 --files=512 --vocab=65536 --line-words=16 --jobs=48
```

正式运行前必须先用 64 MiB 和 256 MiB 通过容量 gate。正式 runner 的关键参数是：

```text
mode=ssh
schedule=role-static
VM0: role=map, launch=localfork, workers=48
VM1: role=reduce, launch=cxlfork, workers=48
reducers=128
map-chunk=2097152
payload-max=4194304
shuffle-connections=48
queue-policy=shared_pair
cxlfork-policy=mow
validation=fast
warmup=2
repeat=5
```

`run-bench.sh` 会让 master 先初始化 HydraRPC layout 并停在 start barrier，等待两个
agent 连同 porter/worker pool 都发布 ready，随后再开始正式作业。主要指标 `run_ms`
来自 VM0 的 `master_job_v2`：从 start barrier 释放后开始，覆盖输入读取、Map、跨 VM
shuffle、CXLfork reducer 执行和输出写入。它不混入 SSH 启动和 agent 冷准备。

以下字段必须分开理解：

- `job_run_ns`：论文端到端对比采用的作业时间；
- `cold_wrapper_ns`：master 进程启动、Hydra layout、agent/porter/worker 冷准备、作业和
  master teardown 的完整 wrapper 时间；
- `orchestrator_ms`：宿主机观察到的控制过程，还会包含 SSH 开销，只作诊断；
- `stages.csv`：`input_setup + map_stage + reduce_stage + output_write + other = total`；
- `tasks.csv`：逐 task 的 HydraRPC、porter、CXLfork restore 和业务执行细分。并行 task
  的 latency 不能相加后当作 job wall time。

作业级只有 5 个正式样本，因此报告 mean、median、min/max 和标准差，不报告 P99。
Task 级样本数足够时可以另外报告分位数，但必须标注为 task latency。

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
