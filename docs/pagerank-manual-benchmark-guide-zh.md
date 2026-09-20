# PageRank 手工性能测试教程

本文只讲性能测试主路径，不展开正确性测试。目标是让使用者只记住两个入口：

- 单机 VM 内：`./example.sh`
- CXLfork + HydraRPC 的 103 host 上：`./scripts/run-pagerank-manual.sh 32`

## 1. 两套实验分别在哪里运行

单机版本只有一个 guest。VM 的类型、CPU 数和内存由已有的 host 启动脚本决定，
PageRank 测试脚本不负责改变 VM 配置。进入 guest 后运行同一条命令即可。

分布式版本有两个 guest。CXLfork、HydraRPC、DAX 区间、双 VM 启停和 vCPU 绑定相互
关联，所以由一个 host 侧总入口统一处理。不要进入 guest 分别手工启动 coordinator
和 worker。

## 2. 单机版本：进入 guest 后怎么测

### 2.1 正式测试

在已经启动好的单机 guest 中，以 root 登录，然后执行：

```bash
cd /root/hpca/pagerank
./example.sh
```

这就是完整的正式测试命令，不需要附加 `env` 或长参数。脚本会：

1. 根据当前 guest 的 NUMA CPU 列表自动选择 worker CPU；
2. 编译 `pagerank_bsp`；
3. 用默认 workload 预热 1 次；
4. 正式运行 5 次；
5. 计算平均时间、median、min、max、stddev 和平均吞吐量。

默认 workload 是：每个逻辑 node 262,144 个顶点、每个顶点 16 条边、其中 2 条是
跨 node 边、100 轮迭代、每个 worker 每轮 32 个同步 chunk。程序的进程创建点位于
CSR 图、两个 rank 数组和 PageRank 配置初始化之后。

### 2.2 查看结果

```bash
cat /root/hpca-results/pagerank/result.txt
cat /root/hpca-results/pagerank/runs.csv
```

- `result.txt` 是汇总结果，汇报时最常看这一行；
- `runs.csv` 保存 5 次正式样本，画图和重新统计时使用；
- `config.txt` 记录本次 workload 与实际 CPU 列表；
- `warmup-0.log` 是预热结果，不计入 5 次正式统计。

`example.sh` 默认复用 `/root/hpca-results/pagerank`，再次运行会覆盖其中的结果。需要
保留本轮数据时，在下次测试前执行：

```bash
cp -a /root/hpca-results/pagerank \
  /root/hpca-results/pagerank-$(date +%Y%m%d-%H%M%S)
```

### 2.3 切换 native-dram、native-cxl 等系统

先退出 guest，用已有 host 启动脚本切换系统及 CPU/内存配置；进入新 guest 后仍然只
执行：

```bash
cd /root/hpca/pagerank
./example.sh
```

因此，不同单机系统之间真正需要保持一致的是 host 启动参数和 guest 内同一份
workload，而不是给 `example.sh` 传不同参数。

## 3. 分布式版本：一条命令完成测试

以下命令都在 103 host（`xtl@10.0.16.103`，hostname 为 `Lux`）上执行，而不是在
任一 guest 内执行。

进入仓库：

```bash
cd /home/xtl/cxlfork/mr-pagerank-direct-hydra-103
```

例如，测两台 VM 合计 32 个 PageRank 计算 worker：

```bash
./scripts/run-pagerank-manual.sh 32
```

不要在前面加 `sudo`。脚本会在需要访问 KVM 和 DAX 时自行请求一次 sudo 密码。

它自动完成：

1. 验证 103、`/dev/dax0.0` devdax 模式和所需文件；
2. 只停止本 CXLfork 工作区残留的 VM；若发现其他 QEMU 或 DAX 使用者则安全退出；
3. 加载 native KVM 模块；
4. 只清零 DAX 的 4--12 GiB，不会清空整个 64 GiB 设备；
5. 按所选规模启动 VM0 和 VM1，各 4 GiB 内存；
6. 逐 vCPU 绑核并等待两个 guest SSH ready；
7. 构建程序、创建 CXLfork 模板、预热 1 次并正式运行 5 次；
8. 分析数据、释放 slot、打印汇总结果；
9. 无论成功或失败都停止本次双 VM。

### 3.1 核数参数到底表示什么

这个 direct-HydraRPC 分支中，命令后的数字同时表示“两台 VM 合计的 PageRank
计算 worker 数”和“两台 VM 合计的 guest vCPU 数”。compute worker 自己直接
轮询 HydraRPC 队列，不创建额外 relay 线程。

| 命令参数 | 每 VM 计算 worker | 每 VM relay | 每 VM vCPU | 两 VM 总 vCPU | 总内存 |
|---:|---:|---:|---:|---:|---:|
| 8 | 4 | 0 | 4 | 8 | 8 GiB |
| 16 | 8 | 0 | 8 | 16 | 8 GiB |
| 32 | 16 | 0 | 16 | 32 | 8 GiB |
| 64 | 32 | 0 | 32 | 64 | 8 GiB |
| 128 | 64 | 0 | 64 | 128 | 8 GiB |

正式主序列逐个执行：

```bash
./scripts/run-pagerank-manual.sh 8
./scripts/run-pagerank-manual.sh 16
./scripts/run-pagerank-manual.sh 32
./scripts/run-pagerank-manual.sh 64
./scripts/run-pagerank-manual.sh 128
```

每条命令都是一次独立实验：重新清理指定 DAX 区间、重新启动双 VM、运行 1 次预热和
5 次正式样本，然后停止 VM。不要并行执行这些命令。direct 模式下 128u 只需要
128 个 guest vCPU，不再因为一对一 relay 扩大到 256 个 vCPU。

### 3.2 运行前只检查配置，不启动 VM

想先确认某个规模会怎样分配 CPU，可执行：

```bash
./scripts/run-pagerank-manual.sh 32 --check
```

该命令不清零 DAX、不启动 VM、也不运行 workload。

## 4. 分布式结果放在哪里

每次运行都会新建带时间戳的目录，不会覆盖旧数据：

```text
/home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/
  manual-direct-<规模>u-8g-<年月日-时分秒>/
```

最重要的文件是：

- `summary.csv`：`elapsed_ms`、`job_e2e_ms` 的均值/P50/P95/min/max/stddev；
- `runs.csv`：5 次正式样本的顶层时间与吞吐；
- `breakdown-summary.csv`：激活、CXLfork、HydraRPC 等 breakdown 的汇总；
- `cxlfork.csv`：restore、shared-leaf attach、数据页 CoW 和 leaf CoW；
- `transport.csv`：HydraRPC 消息数、字节数和直接等待时间；direct 模式要求本地
  futex handoff 计数为 0；
- `manifest/config.txt`：实际 worker、relay、vCPU、内存和 workload；
- `manifest/qemu-vcpu-affinity.txt`：QEMU 参数与实际绑核证据；
- `runs/*.coordinator.log`、`runs/*.worker.log`：不可丢失的原始日志。

测试结束时脚本会直接打印 `result_dir=...` 和 `summary.csv`。之后也可以执行：

```bash
cat /home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/<结果目录>/summary.csv
```

## 5. 单机和分布式比较时必须使用同一口径

- 两边都使用 524,288 个总顶点、16 条边、2 条跨分区边、100 轮和 5 个正式样本；
- 单机 `elapsed_ms` 与分布式 `elapsed_ms` 比较 PageRank 主执行区间；
- 分布式 `job_e2e_ms` 还包含 CXLfork restore、线程创建和 START 激活，更适合解释
  “状态已经共享后，到 worker 真正执行仍有什么控制路径”；
- `host_orchestration_e2e_ms` 包含 SSH 和 host 进程编排，不能与单机 `elapsed_ms`
  直接比较；
- `cxlfork.csv` 和 `transport.csv` 中许多字段是所有并发线程的累计时间，不能直接
  相加成 wall-clock 时间。

最稳妥的人工记录方式是同时保存：单机的 `result.txt`、`runs.csv`、`config.txt`，
以及分布式的整个时间戳结果目录。
