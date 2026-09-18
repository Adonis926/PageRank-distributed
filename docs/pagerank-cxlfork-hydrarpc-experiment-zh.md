# PageRank + CXLfork + HydraRPC 实验说明

## 1. 这套实验回答什么问题

这套程序把单机 PageRank workload 拆到两个独立 guest OS 中，同时保留单机实验的图规模、迭代次数和总 CPU/内存预算。它用于分别观察：

- CXLfork 将 VM1 的只读图模板恢复为可执行 worker 状态需要多久；
- HydraRPC 在 100 轮 PageRank 中传递跨分区 contribution 和同步消息的实际路径；
- worker 已经准备好之后，纯 PageRank 计算时间是多少；
- 从 VM0 发出 `ARM` 到收到 VM1 最终结果，额外的跨 OS 激活开销是多少。

它不是透明恢复任意 PageRank 进程。CXLfork 保存和恢复的是与业务强绑定的 VM1 图模板；程序代码、任意 IP/SP、socket 和正在运行到一半的栈不在 capsule 中。

## 2. 系统结构

正式配置为两个 KVM VM：

| 角色 | 资源 | host CPU | 工作 |
|---|---:|---:|---|
| VM0 / coordinator | 16 vCPU、4 GiB | 0–15 | 本地构造 node0 图，发送控制请求，参与 PageRank，收集结果 |
| VM1 / worker | 16 vCPU、4 GiB | 48–63 | 接收 `ARM`，通过 CXLfork 恢复 node1 图，参与 PageRank |

两台 VM 合计 32 vCPU、8 GiB，与单机 PageRank 的 32 vCPU、8 GiB 总预算一致。每个 QEMU vCPU 都一对一固定到对应 host CPU。

真实 `/dev/dax0.0` 使用互不重叠的三段：

| host DAX 范围 | 用途 |
|---|---|
| 4–8 GiB | CXLfork NVDIMM backend |
| 8–10 GiB | HydraRPC endpoint 0 |
| 10–12 GiB | HydraRPC endpoint 1 |

4 GiB guest 下，QEMU 实际把 NVDIMM 放在 guest physical `0x180000000–0x27effffff`。CXLfork 保留前 64 MiB 后使用 `0x184000000`，长度为 `0xfb000000`。不能继续沿用 16 GiB guest 的 `0x484000000`，否则驱动会映射到不存在的 guest physical hole，`PREPARE_V2` 会返回 `EILSEQ`。

## 3. Workload 对齐

默认参数与单机 `pagerank_bsp` 相同：

- 总顶点数：524,288（每个 node 262,144）；
- 每顶点 16 条边，其中 14 条本地边、2 条远端边；
- 100 次迭代；
- 每个 node 16 个 worker thread，总计 32 个；
- 每个 thread 每轮拆成 32 个 pipeline chunk；
- 一次预热/正确性 gate，随后 5 次正式样本；
- 每次正式样本使用新 coordinator/worker 进程。

图生成公式、CSR 排序和 PageRank 数学计算从单机版本移植。小规模测试中，单机原程序和双节点程序得到相同 residual 和 checksum；正式运行的最终 rank checksum 均为 1。

## 4. 一次运行发生了什么

1. VM0 预先构造 node1 的 CSR 图，把 64 MiB 匿名 VMA 交给 `PREPARE_V2`。生成的 slot 记录 key、generation、页面 checksum 和业务图 checksum。
2. 每次样本启动新的 coordinator 和 worker 进程，并打开两组 HydraRPC endpoint。
3. coordinator 构造 node0 本地图，然后通过 HydraRPC control queue 发送 `ARM`。
4. worker 收到 `ARM` 后调用 `RESUME_V2`，把 node1 图映射到固定地址，检查 generation/checksum，再创建 16 个线程。
5. coordinator 发送 `START`。两个 node 同时进入计时计算区间。
6. 每轮中，每个线程把本 chunk 的远端 contribution 作为 HydraRPC payload 发送；对端直接消费收到的 payload，之后 release message。每轮末再交换 barrier message。
7. worker 通过 HydraRPC 返回 elapsed、residual、rank checksum、transport 和 restore 统计。
8. 进程退出后 CXLfork reader 归零。整套测试最后执行 `INSPECT`，确认 checksum 和 `readers=0`，然后 `RELEASE_V2`。

每个正式样本实际发送 105,600 条消息和 844,774,400 bytes payload。这些不是伪造计数，而是两端逐线程统计后汇总的结果。

## 5. 计时边界

### 可以直接与单机 `elapsed_ms` 对齐的指标

`elapsed_ms` 从两端 worker thread 都通过 START gate 后开始，到 100 轮迭代和最终 checksum 完成为止。它包含 PageRank 计算、HydraRPC contribution 传输和每轮同步；不包含图构造、CXLfork restore、线程创建和进程启动。

它复用了单机程序“线程初始化完成后开始、最后 checksum 完成时结束”的边界，因此是当前最接近单机 `elapsed_ms` 的指标。

### 跨 OS 激活指标

`job_e2e_ms` 从 VM0 发布 `ARM` 前开始，到 VM0 收到 VM1 最终 result 为止。它包含：

- control queue 往返；
- VM1 CXLfork restore；
- 两端线程创建和 START 发布；
- 完整 `elapsed_ms`；
- 最终 result 回传。

`activation_overhead_ms = job_e2e_ms - elapsed_ms`。正式分析时，`elapsed_ms` 和 `activation_overhead_ms` 是唯一可以相加得到 `job_e2e_ms` 的两个顶层部分。

### 不能重复相加的嵌套指标

- `restore_wrapper_ms` 包含 `restore_ioctl_ms`；
- `restore_ioctl_ms` 包含 `restore_kernel_ms`；
- `restore_leaf_attach_ms` 和 fallback 位于 kernel restore 内；
- `arm_ms` 已包含 worker restore 和 worker thread creation；
- Hydra send/receive/release 是 32 个线程的时间总和，彼此并行，不能与 wall-clock `elapsed_ms` 相加；
- `graph_build_ms` 位于 `job_e2e_ms` 之前；
- `host_orchestration_e2e_ms` 从 host 发起两条 SSH/exec 到两个进程退出，包含 SSH、进程启动、图构造、测试和清理，只是辅助诊断值，不能拿它与单机 `elapsed_ms` 直接比较。

## 6. 代码和脚本

开发分支为 `motivation-pagerank-cxlfork-hydrarpc-103`。

- `pagerank/pagerank_hydra.c`：coordinator/worker、HydraRPC 数据与控制消息、计时点。
- `pagerank/pr_graph.c`：与单机版本一致的图构造、CXL 模板格式和 checksum。
- `pagerank/pr_cxlfork.c`：`PREPARE_V2`、`RESUME_V2`、`INSPECT`、`RELEASE_V2` 封装。
- `config/pagerank-real-cxl-103.env`：唯一正式参数文件。
- `scripts/run-pagerank-cxlfork.sh`：build、vCPU pin、prepare、run、完整 bench 都由这个脚本完成。
- `scripts/analyze-pagerank.py`：从每次原始 key-value log 生成 CSV 和统计摘要。

没有为每个动作创建独立脚本，手工调试时只需要上面一个 runner。

## 7. 手工复现命令

下列命令均在 103 host 上执行。DAX 必须已经处于 devdax 模式，且必须确认没有其他实验占用。清零只能使用仓库现有的安全工具处理 4–12 GiB，禁止运行全盘 `dax_zero`。

加载当前 native-capable KVM：

```bash
sudo /home/xtl/start/hpca/load-native-kvm.sh
```

用现有 launcher 启动两个 VM：

```bash
sudo env \
  WS=/home/xtl/cxlfork/cxlfork-repro \
  QEMU=/home/xtl/qemu-9.0.90-clean/build/qemu-system-x86_64 \
  MACHINE=pc-q35-8.2,kernel-irqchip=split,mem-merge=off \
  KERNEL=/home/xtl/cxlfork/cxlfork-repro/kernel/build-6.6.32/arch/x86/boot/bzImage \
  GUEST_APPEND_EXTRA=nox2apic \
  CXL_FILE=/dev/dax0.0 CXL_BACKEND_SIZE=4G CXL_BACKEND_OFFSET=4G CXL_BACKEND_ALIGN=2M \
  CXL_PHYS=0x184000000 CXL_SIZE=0xfb000000 \
  HYDRA_SHM_FILE=/dev/dax0.0 HYDRA_SHM_SIZE=4G HYDRA_BACKEND_OFFSET=8G \
  HYDRA_ENDPOINT_SIZE=2G HYDRA_ENDPOINT1_OFFSET=2G HYDRA_BACKEND_ALIGN=2M \
  BACKEND_RESET=none MEM=4G SMP=16 VM_MAXMEM=20G \
  VM0_HOST_CPUS=0-15 VM1_HOST_CPUS=48-63 \
  VM0_MEMORY_NODE=0 VM1_MEMORY_NODE=1 \
  HYDRA0_PCI_ADDR=0x2 HYDRA1_PCI_ADDR=0x3 \
  /home/xtl/cxlfork/cxlfork-repro/scripts/start-cxlfork-vms.sh cxl
```

固定 vCPU，并执行正式测试：

```bash
cd /home/xtl/cxlfork/mr-pagerank
sudo ./scripts/run-pagerank-cxlfork.sh pin-vcpus
PR_STAMP=manual-$(date +%Y%m%d-%H%M%S) \
  ./scripts/run-pagerank-cxlfork.sh bench
```

停止时只停止该工作区记录的两个 VM：

```bash
sudo /home/xtl/cxlfork/cxlfork-repro/scripts/stop-cxlfork-vms.sh cxl
```

## 8. 结果文件怎么看

103 上正式结果目录：

```text
/home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/final-20260918-085902/
```

最重要的文件：

- `runs.csv`：5 次正式样本的顶层结果；
- `summary.csv`：`elapsed_ms` 与 `job_e2e_ms` 的 mean/P50/P95/min/max/stddev；
- `breakdown-summary.csv`：各 breakdown 的均值和层级，明确标记能否作为顶层相加；
- `activation.csv`：ARM、thread creation、START 等控制路径；
- `cxlfork.csv`：每次 restore、leaf attach 和 fallback；
- `transport.csv`：HydraRPC 消息、bytes 和线程累计时间；
- `runs/*.coordinator.log`、`runs/*.worker.log`、`runs/*.host.log`：不可丢失的原始记录；
- `template.log`：CXLfork prepare 与模板 checksum；
- `final-inspect.log`、`release.log`：reader/checksum/release 证据；
- `manifest/qemu-vcpu-affinity.txt`：QEMU 完整命令和每个 vCPU 的实际 affinity；
- `manifest/vm*-dmesg.txt`：测试结束后的完整 guest dmesg。

## 9. 本次正式结果

5 次正式 fast-path 样本：

| 指标 | mean | P50 | P95 | stddev |
|---|---:|---:|---:|---:|
| `elapsed_ms` | 148.990 ms | 148.634 ms | 150.073 ms | 0.967 ms |
| `job_e2e_ms` | 155.804 ms | 155.918 ms | 157.323 ms | 1.497 ms |
| `activation_overhead_ms` | 6.813 ms | 6.302 ms | 8.678 ms | 1.370 ms |

平均 breakdown：

- ARM：2.780 ms；
- VM0 thread creation：2.591 ms；
- START publish：0.222 ms；
- CXLfork restore wrapper：0.0308 ms；
- kernel restore：0.0202 ms；
- shared-leaf attach：0.0030 ms；
- fallback reconstruction：0；
- shared leaf：32 个；shared data page：16,384 个。

因此，在已经预建 capsule 的稳态 fast path 中，kernel restore 只占 `job_e2e_ms` 的约 0.013%，主要可见的非计算 wall time是线程创建、control request/response 和调度尾部，而不是页表恢复。

正确性/真实性证据：5 次 checksum 全为 1，residual 一致；每次都是 32 个 shared leaf、16,384 个 shared data page、0 个 fallback page；最终 page checksum 一致、reader 为 0、release 成功；两台 guest dmesg 未发现 BUG/Oops/general-protection/panic。

## 10. 如何解释与现有单机结果的关系

本次 `elapsed_ms` 约 149 ms，比已有 native-DRAM 单机结果约 230 ms 更短，并没有出现原先预期的“分布式路径明显更慢”。不能为了得到预期曲线而人为加延迟或挑选异常样本。

当前最合理的解释是通信机制不同：单机参考程序的分区同步使用 futex sleep/wake；本实现的 HydraRPC worker 使用专用 core 忙轮询共享 queue。忙轮询减少了同步唤醒开销，且两台 QEMU 分别固定在两个 socket 的 16 个 core 上。它足以抵消跨 OS 路径的稳态开销。第一轮全量验证 gate 的 `job_e2e_ms` 约 398 ms，但它刻意逐页校验，不是默认 fast path，不能拿来冒充正式性能。

此外，单机使用 6.12 guest kernel，本 CXLfork 原型基于 6.6.32。虽然 workload 参数、CPU/内存总量和 vCPU 绑定已经对齐，kernel 与通信原语仍是系统级变量。因此这批数据可以作为“真实路径已跑通”和 breakdown 数据，但暂时不能支撑“CXLfork+HydraRPC 一定比单机慢”的论文结论。若论文必须回答这一点，应增加同一通信等待策略的 ablation，或同时比较两边完整的进程 wall time；不能混用 `host_orchestration_e2e_ms` 与单机 `elapsed_ms`。

