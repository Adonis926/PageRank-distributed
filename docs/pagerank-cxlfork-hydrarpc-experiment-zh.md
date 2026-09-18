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

本轮还把虚拟平台参数与 `/home/xtl/start/hpca/start-native-vm.sh 32 8 0`
对齐：两边都使用 QEMU 9.0.90、`pc-i440fx-9.1`、split irqchip、关闭
memory merge 和 `qemu64` CPU model。单机是 `2 socket × 16 core × 1 thread`；
分布式侧将它拆成两台各 `1 socket × 16 core × 1 thread`、单 guest NUMA 的 VM。
分布式 CPU model 只额外开放 `+waitpkg`，这是 HydraRPC 纯共享内存
`UMONITOR/UMWAIT` 唤醒的必要条件。runner 会在每轮正式测试前核查实际 QEMU
cmdline、guest 拓扑、WAITPKG 和 32 个 vCPU 的逐线程 affinity，不接受仅配置文件
“看起来一致”但运行时没有生效的情况。

真实 `/dev/dax0.0` 使用互不重叠的三段：

| host DAX 范围 | 用途 |
|---|---|
| 4–8 GiB | CXLfork NVDIMM backend |
| 8–10 GiB | HydraRPC endpoint 0 |
| 10–12 GiB | HydraRPC endpoint 1 |

在 `pc-i440fx-9.1` 和 4 GiB guest DRAM 下，QEMU HMP
`info memory-devices` 实测 NVDIMM 起点为 `0x140000000`、可见长度为
`0xff000000`。CXLfork 保留前 64 MiB 后使用 `0x144000000`，长度为
`0xfb000000`。HydraRPC 的两个 ivshmem BAR 固定为 `00:03.0` 和 `00:04.0`；
`00:02.0` 是 i440fx 默认 VGA，不能沿用旧 Q35 配置。物理地址或 BDF 配错时，
不得继续跑 benchmark。

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
6. 每轮中，每个线程把本 chunk 的远端 contribution 作为 HydraRPC payload 发送；对端直接消费收到的 payload，之后 release message。每个 VM 先用本地 `pthread_barrier` 汇总16个线程的 local residual，再由 thread 0 把它搭载在原有 HydraRPC barrier 中。两端在进入下一轮前都计算 `global residual = node0 local + node1 local`。
7. worker 通过 HydraRPC 返回 elapsed、residual、rank checksum、transport 和 restore 统计。
8. 进程退出后 CXLfork reader 归零。整套测试最后执行 `INSPECT`，确认 checksum 和 `readers=0`，然后 `RELEASE_V2`。

每个正式样本实际发送 105,600 条消息和 844,774,400 bytes payload。这些不是伪造计数，而是两端逐线程统计后汇总的结果。

正式路径不使用 TCP、socket sidecar 或跨 VM futex。接收方在 HydraRPC 共享队列的 head/tail cache line 上执行 `UMONITOR/UMWAIT`；发送方通过 `hydra_send_*()` release-publish 队列时，对该 cache line 的一致性写入就是唤醒事件。醒来后接收方重新检查队列条件，因此它具有 futex-like 的“等待条件变化、唤醒后复查”语义，但实现完全位于 HydraRPC 共享内存数据面。runner 会同时检查两个 guest 的 `waitpkg` CPU flag；程序输出必须是 `notify=umwait effective_notify=umwait`，不允许静默退化为 busy polling。

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
- `hydra_wait_cpu_sum_ms` 是所有线程 UMWAIT 调用的累计时间，已嵌套在 receive wait 中，也不能再次相加；
- `graph_build_ms` 位于 `job_e2e_ms` 之前；
- `host_orchestration_e2e_ms` 从 host 发起两条 SSH/exec 到两个进程退出，包含 SSH、进程启动、图构造、测试和清理，只是辅助诊断值，不能拿它与单机 `elapsed_ms` 直接比较。

## 6. 代码和脚本

当前加入逐轮全局 residual 汇总后的开发分支为
`motivation-pagerank-global-residual-103`：

- CXLfork launcher commit：`690fe42`；
- PageRank/HydraRPC 实现 commit：`8320fa2`；后续文档提交以当前分支 HEAD
  为准，结果运行版本仍记录在 manifest 的 `mr_git` 中。

- `pagerank/pagerank_hydra.c`：coordinator/worker、HydraRPC 数据与控制消息、计时点。
- `pagerank/pr_graph.c`：与单机版本一致的图构造、CXL 模板格式和 checksum。
- `pagerank/pr_cxlfork.c`：`PREPARE_V2`、`RESUME_V2`、`INSPECT`、`RELEASE_V2` 封装。
- `config/pagerank-real-cxl-103.env`：唯一正式参数文件。
- `scripts/run-pagerank-cxlfork.sh`：build、vCPU pin、prepare、run、完整 bench 都由这个脚本完成。
- `scripts/analyze-pagerank.py`：从每次原始 key-value log 生成 CSV 和统计摘要。

没有为每个动作创建独立脚本，手工调试时只需要上面一个 runner。

## 7. 手工复现命令

下列命令均在 103 host 上执行。DAX 必须已经处于 devdax 模式，且必须确认没有其他实验占用。清零只能使用仓库现有的安全工具处理 4–12 GiB，禁止运行全盘 `dax_zero`。

先关闭可能残留的 Native/GVM VM，再加载当前 native-capable KVM：

```bash
sudo /home/xtl/start/hpca/kill-gvm.sh
sudo /home/xtl/start/hpca/load-native-kvm.sh
```

仅清零本实验独占的 DAX 4–12 GiB，不能运行全盘 `dax_zero`：

```bash
sudo /home/xtl/start/hpca-cxlfork/bin/dax_range_zero \
  /dev/dax0.0 4294967296 8589934592 ZERO-CXLFORK-MR-4G-12G
```

用现有 launcher 启动两个 VM：

```bash
sudo env \
  WS=/home/xtl/cxlfork/cxlfork-repro \
  QEMU=/home/xtl/qemu-9.0.90-clean/build/qemu-system-x86_64 \
  MACHINE=pc-i440fx-9.1,kernel-irqchip=split,mem-merge=off \
  CPU_MODEL=qemu64,+waitpkg,kvmclock-stable-bit=off \
  HYDRA_PCI_BUS=pci.0 VM_PIN_VCPUS=1 \
  KERNEL=/home/xtl/cxlfork/cxlfork-repro/kernel/build-6.6.32/arch/x86/boot/bzImage \
  GUEST_APPEND_EXTRA='nox2apic clocksource=kvm-clock nmi_watchdog=0 irqaffinity=0' \
  CXL_FILE=/dev/dax0.0 CXL_BACKEND_SIZE=4G CXL_BACKEND_OFFSET=4G CXL_BACKEND_ALIGN=2M \
  CXL_PHYS=0x144000000 CXL_SIZE=0xfb000000 \
  HYDRA_SHM_FILE=/dev/dax0.0 HYDRA_SHM_SIZE=4G HYDRA_BACKEND_OFFSET=8G \
  HYDRA_ENDPOINT_SIZE=2G HYDRA_ENDPOINT1_OFFSET=2G HYDRA_BACKEND_ALIGN=2M \
  BACKEND_RESET=none MEM=4G SMP=16 VM_MAXMEM=20G \
  VM0_HOST_CPUS=0-15 VM1_HOST_CPUS=48-63 \
  VM0_MEMORY_NODE=0 VM1_MEMORY_NODE=1 \
  HYDRA0_PCI_ADDR=0x3 HYDRA1_PCI_ADDR=0x4 \
  /home/xtl/cxlfork/cxlfork-repro/scripts/start-cxlfork-vms.sh cxl
```

启动器已逐 vCPU 绑核。执行正式测试时，runner 会再次验证，而不是再次修改：

```bash
cd /home/xtl/cxlfork/mr-pagerank
PR_STAMP=manual-$(date +%Y%m%d-%H%M%S) \
  PR_LOCAL_DIAGNOSTIC=0 ./scripts/run-pagerank-cxlfork.sh bench
```

停止时只停止该工作区记录的两个 VM：

```bash
sudo /home/xtl/cxlfork/cxlfork-repro/scripts/stop-cxlfork-vms.sh cxl
```

## 8. 结果文件怎么看

103 上正式结果目录：

```text
/home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/paper-main-global-residual-20260918/
```

跳板机上的逐文件 SHA256 一致备份为：

```text
/home/adonis/cxlfork/test/pagerank/cxlfork-hydrarpc-results/paper-main-global-residual-20260918/
```

最重要的文件：

- `runs.csv`：5 次正式样本的顶层结果；
- `summary.csv`：`elapsed_ms` 与 `job_e2e_ms` 的 mean/P50/P95/min/max/stddev；
- `breakdown-summary.csv`：各 breakdown 的均值和层级，明确标记能否作为顶层相加；
- `activation.csv`：ARM、thread creation、START 等控制路径；
- `cxlfork.csv`：每次 restore、leaf attach 和 fallback；
- `transport.csv`：HydraRPC 消息、bytes 和线程累计时间；
- `residual.csv`：每次样本两端的 local/global residual 以及逐轮同步次数；
- `runs/*.coordinator.log`、`runs/*.worker.log`、`runs/*.host.log`：不可丢失的原始记录；
- `template.log`：CXLfork prepare 与模板 checksum；
- `final-inspect.log`、`release.log`：reader/checksum/release 证据；
- `manifest/qemu-vcpu-affinity.txt`：QEMU 完整命令和每个 vCPU 的实际 affinity；
- `manifest/hmp-memory-devices.txt`：两台 VM 的实际 NVDIMM guest physical 起点和大小；
- `manifest/vm*-dmesg.txt`：测试结束后的完整 guest dmesg。

## 9. 本次正式结果

5 次正式 fast-path 样本：

| 指标 | mean | P50 | P95 | stddev |
|---|---:|---:|---:|---:|
| `elapsed_ms` | 179.864 ms | 180.721 ms | 181.321 ms | 1.854 ms |
| `job_e2e_ms` | 187.627 ms | 188.179 ms | 190.127 ms | 2.364 ms |
| `activation_overhead_ms` | 7.763 ms | 7.249 ms | 9.132 ms | 1.236 ms |
| throughput | 4664.263 MEdges/s | — | — | — |

平均 breakdown：

- ARM：3.625 ms；
- VM0 thread creation：2.733 ms；
- START publish：0.217 ms；
- CXLfork restore wrapper：0.0526 ms；
- kernel restore：0.0326 ms；
- shared-leaf attach：0.0040 ms；
- fallback reconstruction：0；
- HydraRPC receive wait（线程累计）：361.831 ms；
- 其中纯 HydraRPC UMWAIT（线程累计）：72.631 ms；
- shared leaf：32 个；shared data page：16,384 个。

因此，在已经预建 capsule 的稳态 fast path 中，kernel restore 只占 `job_e2e_ms`
的约 0.017%，主要可见的非计算 wall time 是线程创建、control
request/response 和调度尾部，而不是页表恢复。`graph_build_ms` 均值约
866.165 ms，但它发生在正式 job 计时窗口之前；host 从两条 SSH exec 到进程退出的
辅助时间均值约 1416.833 ms，二者都不能加到 `elapsed_ms` 中冒充同一层级 breakdown。

正确性/真实性证据：5 次 checksum 全为 1；每个样本两端都报告
`residual_mode=global-per-iteration` 和 `residual_syncs=100`，两端 global residual
均为 `3.93870320473e-20`，且等于两个 local residual 之和。每次都是32个
shared leaf、16,384个 shared data page、0个 fallback page；最终 page checksum
一致、reader为0、release成功；两台 guest dmesg 未发现
BUG/Oops/general-protection/panic。

逐轮 global residual 复用了原来56字节 barrier 消息的 reserved 空间：只有 thread 0
携带本 VM 的 local residual，其余线程携带0。因此没有增加跨 VM 往返、消息条数或
payload 大小；仍为105,600条消息和844,774,400 bytes。与修改前同平台的
175.799 ms 相比，新 `elapsed_ms` 增加约2.3%。这是一组各5次样本的初步差值，
可说明语义增强没有数量级代价，但不应把它当作高置信微基准结论。

## 10. 如何解释与正式 32u 单机数据的关系

以下是本轮指定的唯一单机基线；旧的约 230 ms 数字不再使用：

| 系统 | elapsed mean | stddev | throughput |
|---|---:|---:|---:|
| vm-dram | 183.855 ms | 0.659 ms | 4562.693 MEdges/s |
| vm-cxl | 409.808 ms | 9.914 ms | 2048.613 MEdges/s |
| starfish | 448.275 ms | 1.378 ms | 1871.326 MEdges/s |
| cohort-cxl | 415.141 ms | 3.752 ms | 2020.826 MEdges/s |
| cohort | 195.575 ms | 3.763 ms | 4290.757 MEdges/s |

CXLfork+HydraRPC 的对齐后 `elapsed_ms` 为179.864 ms，比 vm-dram 的
183.855 ms 快约2.2%；若把跨 OS ARM、restore、线程创建和结果
返回计入，`job_e2e_ms` 为187.627 ms，比 vm-dram 的执行区间慢约2.1%。新数据不再具有旧版19.3%的巨大优势，
说明 machine/CPU/topology/绑核确实显著影响结论；但它仍不能支撑“分布式路径一定
更慢”，更不能按预期人为调慢或筛选样本。

这一次已经排除了“分布式侧忙轮询、单机侧 futex”这一明显差异：单机程序保持原
futex 不变，分布式侧为纯 HydraRPC UMWAIT，五次样本都确认
`effective_notify=umwait`。machine、基础 CPU model、socket/core/thread 总拓扑、
host CPU 和内存 NUMA 放置也已对齐。仍无法消除的结构差异是：单机是一个
32-vCPU guest 的两个 guest NUMA 节点，分布式是两个各 16-vCPU、单 guest NUMA
的独立 OS；图和 rank 数组也随之天然分区。这部分局部性收益可能覆盖 HydraRPC
同步成本，是目前最符合结果的解释。

另外，单机与 CXLfork guest kernel 版本仍不同。故这批结果可用于证明真实 CXLfork restore、纯 HydraRPC 共享内存唤醒及其 breakdown，但若论文要做严格的系统优劣结论，还需要把 guest kernel、NUMA/CPU 放置和内存带宽预算进一步统一。`host_orchestration_e2e_ms` 包含 SSH 和进程启动，绝不能拿来与单机 `elapsed_ms` 比较。
