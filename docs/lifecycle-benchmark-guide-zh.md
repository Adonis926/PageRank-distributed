# HydraRPC + CXLfork + WordCount 单路径生命周期测试

## 1. 测试目的

这套测试不再比较 persistent、localfork、prefork、MoA、Hybrid 等多种策略，而是
固定一条能够体现当前系统能力的主路径，回答三个问题：

1. 第一个远端 reducer worker 从不存在到真正开始执行需要多久；
2. worker pool 建好以后，正常任务的时间花在哪些阶段；
3. template 发布、worker 冷启动、正常任务、进程回收和最终释放能否组成一个
   完整且可验证的生命周期。

固定路径为：

```text
VM0 mapper: localfork（固定 workload 生产端，不作为实验变量）
VM1 reducer: CXLfork + lazy porter + preopen + MoW + fast validation
4 MiB reducer template + 4-worker 静态 pool
```

## 2. 冷启动边界

计时开始前，两台 VM、HydraRPC queue、CXLfork 驱动以及 reducer agent 已经就绪。
reducer agent 此时只负责接收 HydraRPC 请求，尚未创建：

- `mr_porter` 的 supervisor 子进程；
- 4 个 reducer worker；
- worker 内的 `/dev/cxlfork` context。

第一个真实 WordCount reduce 请求到达后，agent 才懒创建 porter 和 worker pool。
因此冷启动覆盖：

```text
request → agent parse → porter → supervisor → worker fork
        → /dev/cxlfork preopen → RESUME_V2 → state bind → business
```

QEMU 启动、Linux 开机、DAX mode 切换、SSH、编译和部署属于实验基础设施，不混入
worker 冷启动。

## 3. 一个 cycle 做什么

每个 cycle 都从没有 template、porter 和 worker 的状态开始：

1. VM0 构造 4 MiB reducer template 并执行 `PREPARE_V2`；
2. 启动 VM0 mapper agent；
3. 启动 VM1 transport-only reducer agent；
4. 运行一次固定 WordCount job；
5. 第一个 reducer task 触发 lazy porter，标记为 `cold`；
6. 后续 reducer task 使用已经建立的 porter/pool，标记为 `steady`；
7. master 发送 shutdown；
8. agent 停止 supervisor 和所有待命 worker；
9. 确认 slot reader 已清零，执行 `RELEASE_V2`；
10. 验证 WordCount 输出和 release 后不可 inspect。

默认每个 cycle 使用 11 个串行 reducer partition，因此自然产生：

- 1 个 cold reducer task；
- 10 个 steady reducer task。

串行调度避免后续 reducer 请求在冷启动期间排队，从而把 steady task 错当成正常
稳定路径。

## 4. 运行命令

前提是 103 上的 CXLfork 双 VM 已经独占运行，SSH 端口为 2200/2201。

最小 smoke：

```bash
cd /home/xtl/cxlfork/mr-distributed
git switch cxlfork-lifecycle-breakdown-103

RESULT=/home/xtl/start/hpca-cxlfork/results/lifecycle-smoke-$(date -u +%Y%m%dT%H%M%SZ)
scripts/run-lifecycle-bench.sh --output-dir="$RESULT" --smoke
```

正式测试：

```bash
RESULT=/home/xtl/start/hpca-cxlfork/results/lifecycle-real-dax-$(date -u +%Y%m%dT%H%M%SZ)

scripts/run-lifecycle-bench.sh \
  --output-dir="$RESULT" \
  --cycles=100 \
  --steady-tasks=10 \
  --workers=4 \
  --template-bytes=4194304
```

输出目录必须不存在，runner 拒绝覆盖旧结果。

## 5. 最重要的计时字段

### Template prepare

- `template_build_ns`：构造 WordCount worker state；
- `prepare_ioctl_ns`：完整 `PREPARE_V2` ioctl；
- `prepare_pte_walk_ns`、`prepare_gup_ns`、`prepare_page_copy_ns`：内核
  checkpoint 数据面；
- `prepare_metadata_ns`、`prepare_checksum_ns`、`prepare_publish_ns`：metadata
  和发布协议；
- `wrapper_total_ns`：用户态从打开设备到 template 可见。

### Cold task

- `porter_start_total_ns`：创建 porter 到初始 pool 全部 ready；
- `porter_shared_map_ns`、`porter_socketpair_ns`：VM1 本地 porter 控制结构；
- `porter_supervisor_fork_ns`：创建 supervisor；
- `initial_pool_spawn_ns`：创建并 preopen 初始 worker pool；
- `first_worker_ready_ns`：从开始建 pool 到第一个 worker ready；
- `target_to_business_ns`：agent 收到请求到 callback 开始；
- `target_to_first_state_access_ns`：到业务第一次读取恢复状态；
- `roundtrip_ns`：VM0 观察到的完整任务往返。

### Cold/steady 公共阶段

- `restore_wrapper_ns` 包含 `resume_ioctl_ns`；
- `resume_ioctl_ns` 包含 `kernel_total_ns`；
- `target_bind_ns`：恢复地址空间与 WordCount state 的绑定；
- `target_business_ns`：实际 WordCount reducer callback；
- `target_reap_ns`：退出并回收本次一次性 worker；
- `target_refill_ns`：创建并 preopen 替代 worker；
- `business_to_pool_ready_ns`：业务完成到 pool 恢复容量。

上述包含关系不能重复相加。

### Teardown

- `porter_stop_total_ns`：agent 开始停止 porter 到本地资源释放；
- `workers_stop_total_ns`：通知并等待剩余待命 worker 退出；
- `supervisor_wait_total_ns`：porter reply 后等待 supervisor 完全退出。

## 6. 结果文件

```text
RESULT/
├── lifecycle.csv      # 一行一个 cycle：prepare、cold 数量和 teardown
├── tasks.csv          # 一行一个 map/reduce task，reduce 分为 cold/steady
├── workers.csv        # worker PID、出生、preopen、执行和 replacement 关联
├── kernel.csv         # 每次 reducer restore 的互斥内核 bucket
├── jobs.csv           # WordCount job 端到端时间
├── summary.csv        # prepare/cold/steady/teardown 的统计摘要
├── validation.json    # 机器可读的验收结论
├── metadata.txt       # Git、配置、样本和时钟规则
├── run-manifest.csv   # 每个 cycle 的期望样本数
├── raw/               # master/agent/template/release 原始证据
└── system/            # build、capability 和前后 dmesg
```

正式分析优先看：

```bash
column -s, -t < "$RESULT/summary.csv" | less -S
column -s, -t < "$RESULT/lifecycle.csv" | less -S
column -s, -t < "$RESULT/tasks.csv" | less -S
```

## 7. 统计规则

- cold/template/teardown 默认 100 个独立 cycle，报告 mean、P50、P95 和标准差；
- steady 默认 1000 个 task，可以额外报告 P99；
- 少于 500 个样本时分析器不输出 P99；
- VM0 和 VM1 的绝对 `CLOCK_MONOTONIC` 不直接相减；
- `cross_vm_delay_estimate_ns` 只是诊断值，不和顶层 RTT bucket 相加；
- kernel exclusive bucket 必须精确等于 `kernel_total_ns`。

## 8. 使用结果的前提

只有以下条件全部满足，数据才可进入论文分析：

- 每个 cycle 恰好 1 个 cold 和配置数量的 steady reducer task；
- WordCount 输出与 expected 完全一致；
- 每个 reducer task 的 binding flags 为 15，且确实读取 worker state；
- `restore_id != 0`、`shared_leaf_count > 0`、`fallback_pages = 0`；
- kernel 互斥 bucket 无未归属时间；
- teardown 后 reader 为 0，release 后 slot 不可 inspect；
- `validation_errors=[]`；
- 两台 VM 的 dmesg 没有 CXLfork error、BUG 或 Oops。

当前恢复对象仍是受限匿名 worker template，不是任意 Linux 进程从保存 IP/SP 透明
继续执行。该边界不影响本测试研究 worker 数据面优化后剩余的跨 OS 控制路径。
