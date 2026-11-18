# 架构总览与双模场景设计

本文档阐述 InferPilot 如何在 `llm-d` 分布式推理底座之上，以一套统一的 Kubernetes Operator 架构同时支撑**「租户自选模型部署 + 训推/RL 联动」**与**「平台公共 MaaS + 按用户 Token 计量限速」**两大核心场景，并给出关键工程决策（D1–D12）。

---

## 1. 为什么在 `llm-d` 之上还需要构建 InferPilot？

CNCF `llm-d` 提供了业界领先的 **南向（Southbound）模型池内调度与分布式执行能力**（基于 Gateway API Inference Extension 的 `InferencePool`、Endpoint Picker `EPP`、ZMQ 精确 KV Cache 索引、`LeaderWorkerSet` 多机编排与 NIXL PD 分离）。但若直接面向企业多租户与训推一体平台交付，仍存在四块明显的拼图缺失：

1. **原语过于底层，不适合直接暴露给租户**：部署一个生产级模型需要手工拼装 `InferencePool`、`EPP` Deployment/ConfigMap、`HTTPRoute`、`LeaderWorkerSet`、`KEDA ScaledObject` 等近十种资源，租户极易配错参数。
2. **缺乏与模型仓库及 RL 训练热通道的原生闭环**：`llm-d` 默认假设模型通过静态路径或 `HF_TOKEN` 拉取，缺乏与 Harbor OCI 预热缓存（`ModelDock`）的自动绑定，也未内置强化学习每步在线更新权重时的 **“流量排空 + 权重刷写 + 全局 KV Cache 清空”** 原子协调机制。
3. **缺乏北向商业多租户治理与 Token 账本**：`llm-d` 只关心后端 Pod 饱和度，不处理用户 API Key 鉴权、预付费余额熔断、流式 Output Token 后扣减限速以及防客户端中途断流逃单的计量审计。
4. **缺乏与平台算力计费体系对齐**：对于租户独占部署的 GPU 推理池，需要与 `TrainPilot` / `DevPilot` 完全对齐的按运行段落计费（`segments[]`）与抢占式（`Spot`）生命周期管理。

因此，InferPilot 定位为 **`llm-d` 与 Envoy AI Gateway 之上的声明式平台控制面与商业治理层**。

---

## 2. 双模业务场景融合拓扑

InferPilot 坚持 **“单层 Envoy 数据面、双模后端池编排”** 原则——绝不在 Envoy AI Gateway 后面再串接一层独立的 `llm-d` Standalone Envoy，避免双倍代理序列化与长连接内存开销：

```text
                 ┌─────────────────────────────────────────────────────┐
                 │           外部用户 / 租户应用 / RL Rollout 采样器   │
                 └──────────────────────────┬──────────────────────────┘
                                            │ OpenAI 兼容 HTTPS / SSE 流
                                            ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                     统一网关数据面 (Envoy Proxy: AI Gateway + GAIE)                    │
│                                                                                       │
│  [1. 认证与预检滤镜] 校验 API Key/JWT ──▶ 查询 Redis 校验 RPM / 并发流 / TPM 余额 > 0 │
│  [2. 请求规范化改写] 剥离伪造内部头；注入用户 PriorityTier 与 Tenant-Salt；            │
│                      stream=true 时强制注入 stream_options.include_usage=true         │
│  [3. 南向选点调度]   通过 ext-proc 调用对应模型池的 llm-d EPP 获取最优目标 Pod IP     │
│  [4. 流式计量后处理] 实时统计 SSE Chunk；正常结束取 usage 帧、异常断流取网关累计值    │
│                      ──▶ 异步扣减 Redis TPM 桶 + 投递完整计量事件至 Kafka             │
└──────────────┬────────────────────────────┬────────────────────────────┬──────────────┘
               │                            │                            │
               ▼                            ▼                            ▼
  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐
  │ 形态 1: 平台公共 MaaS 池│  │ 形态 2: 共享 Multi-LoRA │  │ 形态 3: 租户独占 / RL 池│
  │ Namespace: infer-public │  │ Namespace: infer-shared │  │ Namespace: forge-<ten>  │
  │                         │  │                         │  │                         │
  │ - DeepSeek-R1 / Qwen3   │  │ - 平台托管热门基座池    │  │ - 租户全量私有权重模型  │
  │ - 开启精确 KV Events    │  │ - 租户通过 LoRABinding  │  │ - 按 GPU 运行段落计费   │
  │ - 大规模下开启 PD 分离  │  │   秒级热挂载百个适配器  │  │ - 可绑定 RolloutChannel │
  │ - 按 Token 消耗向用户计费│  │ - 按调优版 Token 费率计费│  │   承接 TrainPilot RL 采样│
  └─────────────────────────┘  └─────────────────────────┘  └─────────────────────────┘
```

---

## 3. 关键工程决策（D1–D12）

### D1：单层 Envoy 集成模式（Gateway Integration Mode），拒绝双层网关级联
`llm-d` 支持 Standalone（自带独立 Envoy）与 Gateway Integration 两种模式。InferPilot 生产架构统一采用 **Gateway Integration 模式**：集群入口 Envoy（由 Envoy AI Gateway / kgateway 控制面管理）直接加载 `ext-proc` 过滤器与后端每个 `InferencePool` 绑定的 `EPP` 通信。一次 SSE 流式请求只经过一层 L7 代理直达 `vLLM`/`SGLang` Pod，将网关附加首字延迟控制在 `< 2ms`。

### D2：自适应三档工作负载拓扑编译器（SingleNode / MultiNode / Disaggregated）
租户在 `InferenceService` 中只需声明模型引用与并行策略（或使用 `Auto` 由 Operator 读取 `ModelArtifact.spec.servingProfile` 自动推导），InferPilot 负责将其编译为三档底层工作负载之一：
1. **`SingleNode`**：单机 1/2/4/8 卡可容纳的模型，渲染标准 `Deployment`（支持平滑滚动更新与 KEDA 快速弹性）。
2. **`MultiNode`**：单机显存不足的超大规模模型（如 671B MoE），渲染 K8s SIG **`LeaderWorkerSet (LWS)`**，保证 Leader 与 Worker 成组创建、成组重启，并自动开启与 `TrainPilot` 一致的 `hostNetwork` + `rdma/hca` RoCE 网络配置。
3. **`Disaggregated`（PD 分离）**：仅在集群具备 RDMA/NIXL 高速网络且并发吞吐达到阈值时启用，将 Prefill 池（高算力比）与 Decode 池（高显存带宽比）拆分为独立扩缩容组，由 `llm-d` Router 协调跨池 KV 传输。

### D3：优先引导租户使用共享基座 `LoRABinding`（10 倍级降本）
针对场景一“租户微调/RL 产出的模型上线”，若租户每个微调版本都独占启动一组 32B/70B GPU Pod，在低频调用时段会造成严重的显存空转浪费。InferPilot 将 **`LoRABinding` 设为独立一等公民 CRD**：
- 平台常驻运行高并发共享基座池（开启 `--enable-lora` 与预设 `--max-lora-rank`）；
- 租户提交 `LoRABinding` 绑定自己的 `ModelArtifact`（`LoRAAdapter`），控制器通过热加载 API 将适配器注册进运行中的副本，并通知 `llm-d` EPP 开启 **LoRA-aware 亲和打分**（优先将请求路由至显存中已驻留该 LoRA 的 Pod），实现零新增 GPU 成本、15 秒内极速上线。

### D4：与 `ModelDock` 强契约集成，杜绝在线拉取 HuggingFace
InferPilot 管理的生产 Pod **默认禁用容器启动时访问公网拉取权重**。所有 `InferenceService` 与 `LoRABinding` 均引用 `ModelDock` 的 `ModelArtifact`：
- 若对应 `ModelCache` 尚未就绪，InferPilot 自动请求创建 `ModelCache` 并将 `InferenceService` 置于 `WarmingCache` 阶段（此时**不分配 GPU Pod、不产生 GPU 计费**！）；
- 待纯 CPU/IO 的缓存预热完成后，再调度 GPU Pod 直接挂载只读 CAS 目录秒级加载显存，彻底消除“占着昂贵 GPU 卡干等网络下载”的资源浪费。

### D5：强化学习（RL）在线联动采用原子版本栅栏（Drain → Sync → Flush KV → Resume）
在 GRPO / Agentic RL 训练中，推理引擎生成的采样轨迹必须严格对应当前 `policy_version`。InferPilot 设计专门的 `RolloutChannel` 控制器：
1. **排空（Drain）**：收到 `TrainPilot` Actor 发起的新一轮权重同步请求时，通知 EPP 暂停向该池派发新请求，等待在途生成完成（或按超时中断）；
2. **显存直传（RDMA/NCCL Weight Sync）**：触发 vLLM Worker Extension 通过高性能网络从训练 Actor 直接覆写 GPU 权重参数（耗时 1～3 秒）；
3. **强制清空 KV Cache（Critical!）**：权重变化后，GPU 显存与主机内存中缓存的所有旧 Prefix KV 激活张量全部失效，控制器**强制调用引擎 `reset_prefix_cache` 并清空 `llm-d` EPP 内的 KV Block 索引表**，防止产生 Off-policy 脏激活数据；
4. **版本盖戳放行（Resume）**：更新池状态 `activePolicyVersion = step-N`，恢复流量并在响应元数据中回传该版本号供训练侧校验。

### D6：双阶段令牌桶（Pre-check + Post-deduct）解决未知输出长度限速难题
LLM 请求在进入网关时只能预估或读取输入长度，无法预知最终生成的 Output Token 数量。InferPilot 采用双阶段限速：
- **入口预检（Pre-check）**：原子校验 `并发活跃流数 < maxConcurrentStreams`、`RPM 未超限` 且 `当前分钟 TPM 剩余令牌 > 0`；同时强制检查请求体的 `max_tokens`（若缺失或超过策略上限则截断/改写），防止单个恶意请求无限生成。
- **流结束实扣（Post-deduct）**：流式响应结束拿到真实总消耗 Token 后，从 Redis 令牌桶中做后置扣减（允许当前已放行的单个长请求将桶余额扣为微负值，随后的新请求将在入口被精准拦截直到令牌自然恢复）。

### D7：流式强制 `include_usage` + 客户端断流（Abort）网关侧兜底计费
针对计费最棘手的“客户端收到 99% 内容后故意断开 TCP 连接以逃避末尾 `usage` 统计帧”漏洞：
- 网关对所有流式请求自动注入 `"stream_options": {"include_usage": true}`；
- 计量组件在转发每个 SSE `data:` 帧时，同步累加已发出的增量字符数与 Chunk 计数；
- 若流正常结束且包含引擎权威 `usage` 帧，以引擎 `usage` 为准出账；
- 若连接因客户端取消（HTTP Context Canceled）、超时或后端崩溃提前中断导致缺失 `usage` 帧，**立即触发兜底逻辑**：对输入 Prompt + 已成功写回客户端缓冲区的增量文本执行本地快速 Tokenizer 计数，生成标记为 `completion_state="aborted"` 的补偿账单，确保“算力一经消耗即准确入账”。

### D8：计量颗粒度原生对齐 Prefix Cache 与深度思考模型
账单事件结构原生拆分四个核心计费字段：`prompt_tokens`（未命中缓存的新算输入）、`cached_prompt_tokens`（命中 `llm-d` Prefix Cache 的输入，支持平台向客户推出缓存命中折扣价）、`completion_tokens`（常规输出）与 `reasoning_tokens`（思维链输出），并通过唯一 `request_id` 写入 Kafka，由下游消费端幂等入库 ClickHouse / `kubemeter`。

### D9：共享池强制注入租户 Prefix Cache Salt（防御 TTFT 侧信道攻击）
在平台公共 MaaS 池和共享基座 Multi-LoRA 池中，若开启全局 Prefix-Cache 复用且不加隔离，攻击者可通过构造候选敏感前缀并测量 `TTFT`（首字延迟是否骤降）反推其他租户刚刚提交过的私有 System Prompt 或合同片段。
- InferPilot 在网关层验明租户身份后，自动向后端请求注入内部不可伪造的 `cache_salt: <tenant_id>`（仅对平台显式声明的公共开源知识库前缀豁免），使得 `vLLM` 与 `llm-d` EPP 计算 KV Block Hash 时天然混入租户盐值，**实现租户内极致缓存命中、跨租户零缓存穿透**。

### D10：摒弃 GPU 利用率 HPA，全面转向 KEDA + `llm-d` EPP 饱和度弹性
vLLM 启动即预分配 90% 显存，且 Decode 阶段受限于显存带宽而非算力利用率，传统 CPU/GPU HPA 完全失真。InferPilot 自动为开启弹性的 `InferenceService` 生成 `KEDA ScaledObject`，基于 EPP 汇总的 **池级排队请求数（Queue Depth）、平均 KV Cache 占用率、以及预测 TTFT 违约率** 进行扩缩容。

### D11：独占推理池商业计费对齐 `TrainPilot` / `DevPilot`
当租户选择独占 GPU 部署专属 `InferenceService` 时，不再按单次调用 Token 计费，而是按**实际占用的 GPU 实例时长**计费：
- 复用与 `TrainPilot` 一致的定点金额字符串 `pricePerHour`、`billingMode`（`OnDemand` | `Spot`）与 `status.segments[]` 计费段模型；
- 计费起点严格定义为“副本组 Ready 可承接流量时刻”（**不含前期 `ModelCache` 预热阶段**），扩缩容发生时自动闭合旧副本数计费段并开启新副本数计费段，账目清晰可核对。

### D12：用户等级与 EPP Flow Control 优先级端到端打通
北向 `TokenRateLimitPolicy` 中定义的等级（`Critical` / `Standard` / `BatchBackground`）由网关校验后写入内部受保护 Header `x-inferpilot-fairness-class` 与 `x-inferpilot-priority`，直接映射至南向 `llm-d` EPP 的 **Flow Control 公平队列与优先级调度器**：在算力打满的高峰期，高优付费用户的在线请求优先出队，异步批处理流量自动退避排队，杜绝低优大请求饿死在线交互用户。
