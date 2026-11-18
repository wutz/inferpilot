# `llm-d` 深度编排与训推/RL 联动机制

本文档详细说明 InferPilot 控制器如何将高层 CRD 编译为底层 CNCF `llm-d` 与 Kubernetes 分布式原语，以及如何与 `ModelDock` 和 `TrainPilot` 形成完整的 SFT/RL 训推闭环。

---

## 1. `InferenceService` → `llm-d` 资源编译映射

一个 `InferenceService` 对象在经过 `InferenceServiceReconciler` 调谐后，会在目标 Namespace 下受控生成一套完整的 `llm-d` 运行栈（通过 `ownerReferences` 绑定生命周期自动回收）：

```text
InferenceService (inferpilot.wutz.dev/v1alpha1)
   │
   ├── 1. 确保模型缓存就绪 ──▶ 查询或自动创建 ModelDock ModelCache (等待 Ready 获取只读 subPath)
   │
   ├── 2. 渲染网关选点面   ──▶ InferencePool (inference.networking.k8s.io/v1)
   │                        ──▶ EndpointPicker (EPP) Deployment + Service + Scorer ConfigMap
   │
   ├── 3. 渲染计算工作负载 ──▶ [SingleNode]    apps/v1 Deployment
   │                        ──▶ [MultiNode]     leaderworkerset.x-k8s.io/v1 LeaderWorkerSet
   │                        ──▶ [Disaggregated] Prefill LWS/Deployment + Decode LWS/Deployment
   │
   └── 4. 渲染弹性伸缩面   ──▶ keda.sh/v1alpha1 ScaledObject (采集 EPP Prometheus 饱和度指标)
```

### 1.1 EPP（Endpoint Picker）评分流水线自动化配置

InferPilot 根据 `InferenceService.spec` 自动装配 `llm-d` EPP 的 *Filter-Score-Pick* 插件权重：

- **常规公共池（`PublicMaaS`）**：`PrecisePrefixCacheScorer (权重 0.5) + QueueDepthScorer (权重 0.3) + KVCacheUtilizationScorer (权重 0.2)`。同时在 vLLM Pod 启动参数注入 ZMQ KV 事件发布端，使 EPP 实时感知每个 Pod 显存与 CPU Offload 层缓存了哪些 Token Block。
- **共享多适配器池（`SharedMultiLoRA`）**：额外激活 `LoRAAffinityScorer`：优先把携带 `model: "acme/finance-copilot:v1.2"` 的请求路由到 GPU 显存已驻留该 LoRA 的副本；若那些副本队列已满，再路由至空闲副本触发主机内存 → 显存毫秒级换入。

### 1.2 多机大模型（`MultiNode`）与 RoCE 网络对齐规范

当部署诸如 `DeepSeek-R1-671B` 或超长上下文 `Qwen3-235B` 等跨机 TP/PP/EP 模型时，普通 Deployment 单 Pod 重启会导致整个跨节点 Ray/NCCL 通信环死锁。InferPilot 编译生成 `LeaderWorkerSet`：
- **成组生命周期**：同一副本组内的 `1 Leader + (N-1) Workers` 作为原子调度与重启单元，仅 Leader Pod 注册进 `InferencePool` 接收 EPP 转发请求。
- **自动高速网络注入（对齐 `TrainPilot` 约定）**：当跨节点且整机 8 卡分配时，自动开启 `hostNetwork: true`、`dnsPolicy: ClusterFirstWithHostNet`、请求 `rdma/hca: 1` 资源并挂载 `/dev/shm`（内存占比默认 `0.5`），确保跨机 AllReduce / AllToAll 走无损 RoCEv2/IB 通道。

### 1.3 Prefill / Decode 分离（`Disaggregated`）编排守则

PD 分离并非银弹，InferPilot 在 Webhook 与控制器层设置明确护栏：
- **准入检查**：若 `topology.mode == Disaggregated`，强制校验目标 GPU 节点池具备 RDMA HCA 或 NVLink 跨机互联能力；纯万兆/25G TCP 节点拒绝启用跨机 PD 分离（防止传输数百 MB KV Cache 的网络延迟超过省下的计算收益）。
- **双角色协同**：为 Prefill Worker 与 Decode Worker 注入匹配的 `NixlConnector` / `Mooncake` 环境变量，`llm-d` Router 在单次请求生命周期内同时选定最优 Prefill 端点与 Decode 端点完成零拷贝 KV 交接。

---

## 2. 动态 Multi-LoRA 热加载状态机（`LoRABinding`）

当租户基于 `TrainPilot` 完成微调并通过 `ModelDock` 导出新的 `LoRAAdapter` 后，上线流程完全无需触碰 GPU Pod 生命周期：

```text
租户创建 LoRABinding (指向 ModelArtifact: finance-qwen3-lora-v1.2)
   │
   ├─ 1. 准入校验：核对 Base Model Digest 一致性、LoRA Rank <= maxLoRARank、跨租户可见性
   ├─ 2. 缓存校验：确认对应 ModelCache 已将该 LoRA 解包至共享只读卷 /lora-cas/sha256/<digest>
   ├─ 3. 实例同步：LoRABindingReconciler 监听目标基座池所有 Ready Pod（含后续扩容出的新 Pod！）
   │      └─ 调用 Pod 内部受控管理端口 POST /v1/load_lora_adapter 注册名称与本地只读路径
   └─ 4. 路由开放：更新 EPP 配置映射与北向网关路由表，状态置为 Active
```

- **扩容自愈**：当共享基座池因流量高峰从 2 副本扩容至 5 副本时，`LoRABindingReconciler` Watch 到新增 Pod 进入 Ready 前，自动为其补齐活跃的 LoRA 注册表，确保新副本上线即可承接所有租户适配器流量。

---

## 3. 强化学习（RL）在线 Rollout 权重热更新协议（`RolloutChannel`）

在 GRPO（如 DeepSeek-R1 训练范式）与多轮 Tool-use Agentic RL 中，训练集群需要针对每个 Prompt 采样 `G`（如 8～64）条长轨迹。接入 `llm-d` 的核心收益是**同一 Prompt 的 `G` 次展开与多轮对话历史可 100% 命中 Prefix Cache**；核心风险则是**策略权重更新时的在途请求竞争与陈旧 KV Cache 污染**。

`RolloutChannel` 通过部署轻量协调器（`inferpilot-rl-coord`）严格执行四步原子栅栏协议：

```text
TrainPilot (RL Actor 完成梯度更新 step N -> N+1)
   │
   │ 1. 发起 PrepareSync(targetVersion="step-1201")
   ▼
inferpilot-rl-coord
   ├──▶ [阶段 A: Draining]
   │     通知 EPP 挂起新采样请求入队；等待所有 vLLM 副本 in_flight_requests 归零
   │     （若超 drainTimeoutSeconds，主动取消超长尾轨迹以防拖慢全局训练吞吐）
   │
   ├──▶ [阶段 B: SyncingWeights]
   │     向 Actor 返回 ReadyForBroadcast ──▶ Actor 与所有 vLLM Worker 通过预建的
   │     NCCL / RDMA 通信组直接显存对显存广播新权重张量（32B 模型耗时约 1~2 秒，零落盘）
   │
   ├──▶ [阶段 C: FlushingCache (防 Off-policy 脏数据生命线)]
   │     1. 并发调用所有 vLLM 副本 reset_prefix_cache 清空 GPU HBM 与 CPU DRAM 中的旧激活缓存
   │     2. 同步向 llm-d EPP 发送 PurgeIndex 指令清空全局 KV Block 路由树
   │
   └──▶ [阶段 D: Serving]
         原子更新池版本标记 activePolicyVersion="step-1201"，恢复 EPP 放行队列；
         后续每条采样响应均附带 x-inferpilot-policy-version: step-1201 供训练器做一致性断言
```

当 RL 训练发现某个 Step（如 `step-1500`）评测奖励创新高、决定对外提供服务时，再由 `TrainPilot` 走**慢路径**触发 `ModelDock` 的 `ModelExport` 固化入库 Harbor，无缝切换为正式线上版本。
