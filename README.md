# InferPilot

面向训推一体平台的 Kubernetes 分布式大模型推理与 MaaS 计量限速 Operator（API Group：`inferpilot.wutz.dev/v1alpha1`）。

InferPilot 以 CNCF **`llm-d`（Gateway API Inference Extension）** 与 **Envoy AI Gateway** 为执行和数据面底座，向上对接 [`ModelDock`](https://github.com/wutz/modeldock)（模型注册与预热中心）与 [`TrainPilot`](https://github.com/wutz/trainpilot)（训练与强化学习平台），在同一套云原生架构中同时支撑两大核心业务场景：

1. **租户自选模型部署与训推/强化学习（RL）闭环联动**：租户无需理解 `InferencePool`、`EPP`、`LeaderWorkerSet` 或 PD 分离细节，即可一键将 `ModelDock` 中的基座或微调产物部署为**独占推理池**或挂载至**共享基座动态 Multi-LoRA 池**；同时提供面向强化学习（GRPO / PPO / Agentic RL）在线采样的 **RDMA/NIXL 秒级权重热同步与 KV Cache 原子刷新通道**。
2. **平台公共 MaaS 模型服务与按用户 Token 精准计量限速**：通过与 `llm-d` 共用单层 Envoy 数据面的统一北向网关，对外提供兼容 OpenAI 规范的模型服务，支持用户级 **RPM / TPM / 最大并发流双阶段限速**、优先级映射（联动 `llm-d` EPP 公平队列）、流式 `include_usage` 强制注入与**客户端中途断流兜底计费**。

> 状态：**M0 架构规划与 CRD 规范设计阶段**（当前仓库仅包含架构设计与接口契约文档，尚未进入编码阶段）。

## 核心架构

```text
                        客户端调用 (平台公共 MaaS API  /  租户专属模型端点)
                                                │
                                                ▼
┌─────────────────── 统一单层数据面: Envoy + AI Gateway + llm-d Router ───────────────────┐
│ 北向策略层 (ModelRoute / TokenRateLimitPolicy):                                         │
│  ├─ API Key / JWT 租户与用户身份认证，剥离外部伪造内部 Header                           │
│  ├─ Redis 双阶段限速：入口拦截 RPM/并发流/透支余额 + 流结束扣减真实 Input/Output Tokens │
│  └─ 流式计量审计：强制注入 include_usage + 断流 Chunk 兜底统计 ──▶ Kafka ──▶ ClickHouse │
│ 南向调度层 (Gateway API Inference Extension + Endpoint Picker EPP):                     │
│  ├─ 按用户付费等级映射 EPP Flow Control 优先级队列与延迟预测过载保护                    │
│  └─ Filter-Score-Pick 管线：精确 KV Block 缓存亲和 + LoRA 已加载亲和 + 队列饱和度打分   │
└───────────────────┬───────────────────────────┬───────────────────────────┬─────────────┘
                    │                           │                           │
        (场景二: 公共 MaaS 大模型池)    (场景一 A: 共享基座 Multi-LoRA)  (场景一 B: 租户独占 / RL 采样池)
                    ▼                           ▼                           ▼
         ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────────┐
         │ InferenceService    │     │ InferenceService    │     │ InferenceService        │
         │ (PD分离 / 多机 LWS) │     │ + LoRABinding × N   │     │ + RolloutChannel (可选) │
         │ 精确 Prefix Cache   │     │ 零新增 GPU 秒级上线 │     │ RDMA 刷写权重 + 清空 KV │
         └──────────┬──────────┘     └──────────┬──────────┘     └────────────┬────────────┘
                    └───────────────────────────┼─────────────────────────────┘
                                                │
                      ┌─────────────────────────┴─────────────────────────┐
                      ▼ 慢路径制品挂载 (秒级只读)                         ▼ 快路径显存直传 (秒级热刷)
        ModelDock (ModelArtifact + ModelCache)              TrainPilot (RL Actor RDMA / NIXL 广播)
```

## 核心特性与生态对齐

| 能力维度 | 设计机制 | 关联生态契约 |
| --- | --- | --- |
| 屏蔽 `llm-d` 底层复杂度 | `InferenceService` 按模型规格自动编译为 `Deployment`、多机 `LeaderWorkerSet` 或 Prefill/Decode 分离池，配套生成 `InferencePool` 与 `EPP` | `llm-d` v1.0、`gateway-api-inference-extension` |
| 模型零拷贝极速冷启动 | 直接绑定 `ModelDock` 的 `ModelArtifact`，自动挂载就绪的 `ModelCache` 只读 CAS `subPath`，多副本扩容零重复下载 | `modeldock.wutz.dev/v1alpha1` (`ModelArtifact`, `ModelCache`) |
| 租户微调零边际成本上线 | `LoRABinding` 将租户 SFT/RL 产出的 LoRA 动态热加载至共享基座池，同步注册至 EPP LoRA-aware 路由器 | vLLM Dynamic LoRA API、`llm-d` EPP LoRA Scorer |
| 强化学习在线采样联动 | `RolloutChannel` 编排「网关排空在途请求 → NCCL/RDMA/NIXL 覆写显存权重 → 强制清空 vLLM 与 EPP KV 索引 → 版本栅栏放行」 | `trainpilot.wutz.dev/v1alpha1` (`TrainTask`)、verl / OpenRLHF |
| 多机 RoCE 网络与实例计费 | 多机全卡实例自动注入 `hostNetwork` + `rdma/hca`；租户独占实例支持 `OnDemand`/`Spot` 与 `status.segments[]` 按段落计费 | 对齐 `TrainPilot` / `DevPilot` 网络与 Money 计费段契约 |
| 用户级 Token 限速与防漏账 | Pre-check + Post-deduct 双阶段 TPM 令牌桶；强制注入 `include_usage` + 中途断流 Chunk 累计兜底账单；拆分命中缓存 Token | Envoy AI Gateway、Redis、Kafka / ClickHouse / `kubemeter` |
| SLO 感知弹性扩缩容 | 自动渲染 `KEDA ScaledObject`，采集 `llm-d` EPP 排队深度、KV Cache 使用率与 TTFT 饱和度驱动副本伸缩（支持缩容至 0） | KEDA、Prometheus |
| 多租户 KV 侧信道防御 | 共享模型池自动按租户注入 Prefix Cache Namespace Salt，严防跨租户通过首字延迟（TTFT）探测私有 Prompt | vLLM Cache Salt、Kubernetes `NetworkPolicy` |

## 文档目录

- [架构总览与双模场景设计](docs/architecture.md) — 统一单层网关设计、双业务维度融合拓扑、关键工程决策（D1–D12）
- [Operator 与 CRD 规范](docs/operator-and-crd.md) — `InferenceService`、`LoRABinding`、`ModelRoute`、`TokenRateLimitPolicy`、`RolloutChannel` 完整定义与状态机
- [`llm-d` 深度编排与训推/RL 联动机制](docs/llmd-and-rl-synergy.md) — EPP 精确 KV Events 路由、分级缓存、多机 LWS/PD 分离编译规则与 RL 原子权重刷新协议
- [北向网关、Token 精准计量与限速架构](docs/gateway-metering-and-ratelimit.md) — 双阶段 TPM/RPM 限速算法、流式断流补偿计费、多维账单拆分与多租户安全红线
- [演进路线图](docs/roadmap.md) — M0–M4 里程碑划分与端到端性能/计费验收标准

## 规划交付物（后续编码阶段）

项目进入编码阶段后，将采用 Kubebuilder v4 构建并发布以下多架构组件与 Chart：

- `ghcr.io/wutz/inferpilot` — Kubernetes Operator 控制器管理器、拓扑编译器与 Admission Webhook
- `ghcr.io/wutz/inferpilot-meter` — Envoy 流式访问日志 / `ext-proc` 计量采集与 Redis TPM 异步扣减守护组件
- `ghcr.io/wutz/inferpilot-rl-coord` — 面向 RL 在线采样的 Drain/WeightSync/KVCacheFlush 协调器组件
- `oci://ghcr.io/wutz/charts/inferpilot` — 部署 Helm Chart

## 许可

Apache License 2.0
