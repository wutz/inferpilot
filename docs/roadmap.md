# 演进路线图

每个里程碑均以「自动化单测 / `envtest` 通过 + 在搭载真实 GPU 与 `llm-d` 的开发集群完成压测与对账验收」作为关闭条件。

---

## M0 架构规划与 CRD 规范设计（当前阶段）

- [x] 确立「单层 Envoy AI Gateway + CNCF `llm-d` (GAIE) 南向调度」一体化底座架构
- [x] 完成面向「租户自选部署 + 训推/RL 联动」与「公共 MaaS + 按用户 Token 计量限速」的双模融合拓扑设计
- [x] 定义五大核心 CRD（`InferenceService`、`LoRABinding`、`RolloutChannel`、`ModelRoute`、`TokenRateLimitPolicy`）完整字段与状态机
- [x] 制定双阶段 TPM 限速、流式断流兜底计费、租户 KV Cache Salt 隔离以及 RL 原子权重刷新栅栏协议
- [ ] 完成与 `ModelDock`、`TrainPilot`、`kubemeter` 的跨项目契约评审，冻结 `v1alpha1` API 定义

---

## M1 核心底座打通：`InferenceService` 单机编排、`llm-d` Baseline 与 `ModelDock` 挂载

目标：跑通「在 `ModelDock` 选择就绪模型 → 创建 `InferenceService` → 自动等待 `ModelCache` 预热就绪 → 自动拉起 vLLM + `InferencePool` + `EPP` → 对比普通 Service 轮询验证 Prefix/Load 感知路由收益 + 独占实例按段落计费」。

- [ ] 使用 Kubebuilder v4 初始化 Go 工程（`github.com/wutz/inferpilot`，API Group `inferpilot.wutz.dev/v1alpha1`）与 Helm Chart（`charts/inferpilot`）
- [ ] 实现 `InferenceServiceReconciler`（`SingleNode` 模式）：
  - [ ] 对接 `ModelDock` API：自动校验 `ModelArtifact`、请求并等待 `ModelCache` Ready（预热期不占 GPU 卡、不计费）
  - [ ] 自动编译生成 `Deployment`（含 `/dev/shm`、`startupProbe`、只读 CAS `subPath` 挂载）、GAIE `InferencePool` 与 `llm-d` `EPP`
  - [ ] 实现 `Running` ↔ `Stopped` 生命周期及与 `TrainPilot` 对齐的 `status.segments[]` 实例计费段记录
- [ ] 集群基准压测：使用 `guidellm` 对比验证 `llm-d` EPP 路由相比原生 Service 轮询在长短混合流量下的 P95 TTFT 收益

---

## M2 北向网关收口：用户级 Token 限速与防漏账流式计量闭环

目标：跑通「配置 `ModelRoute` 与 `TokenRateLimitPolicy` → 多用户持 API Key 并发流式请求 → RPM/TPM/并发流精准拦截 → 模拟客户端中途恶意断流仍准确生成补偿账单写入 Kafka/ClickHouse」。

- [ ] 实现 `ModelRouteReconciler` 与 `TokenRateLimitPolicyReconciler`，对接 Envoy AI Gateway 控制面配置下发
- [ ] 实现 `cmd/inferpilot-meter`（网关 `ext-proc` / Streaming Log 处理器）与 Redis Lua 原子双阶段令牌桶：
  - [ ] 自动改写注入 `stream_options.include_usage=true` 与 `max_tokens` 上限钳制
  - [ ] 注入用户等级优先级 Header 打通 `llm-d` EPP Flow Control 公平队列，并注入租户 `cache_salt`
  - [ ] 正常流读取权威 `usage`（含 `cached_prompt_tokens` 与 `reasoning_tokens`）+ 异常断流本地高速 Tokenizer 补偿计数
  - [ ] 异步批量写入 Kafka 并提供 ClickHouse 幂等落库表结构与对账校验脚本

---

## M3 动态 Multi-LoRA 共享池、精确 KV Events 路由与 KEDA 弹性闭环

目标：跑通「租户 SFT 微调导出 LoRA → 创建 `LoRABinding` 15 秒内零新增 GPU 上线并享受 EPP LoRA 亲和路由 → 开启 ZMQ 精确 KV Block 索引与 CPU 二级卸载 → KEDA 基于 EPP 队列深度自动扩缩容」。

- [ ] 实现 `LoRABindingReconciler` 与 ValidatingWebhook（强校验基座 Digest 与 `maxLoRARank`）：
  - [ ] 管理运行中基座副本的热加载/卸载与扩容新 Pod 自动同步注册，联动更新 EPP LoRA 路由权重
- [ ] 在 `InferenceService` 中打通 vLLM → EPP 的 ZMQ `BlockStored` 精确 KV 缓存事件流与主机内存二级 KV Offloading
- [ ] 实现 `KEDA ScaledObject` 自动渲染（基于 EPP 等待队列与 KV 占用率指标），支持低频租户模型 `minReplicas: 0` 冷备唤醒

---

## M4 高阶分布式拓扑（多机 LWS / PD 分离）与 RL 在线采样热同步通道

目标：跑通跨节点超大规模模型编排，以及与 `TrainPilot` 强化学习任务的秒级在线权重热更新闭环。

- [ ] 扩展 `InferenceService` 拓扑编译器支持 `MultiNode`（生成 `LeaderWorkerSet`，自动注入 `hostNetwork` + `rdma/hca` RoCE 配置）与 `Disaggregated`（Prefill/Decode 分离池 + NIXL RDMA 传输）
- [ ] 实现 `RolloutChannelReconciler` 与 `cmd/inferpilot-rl-coord` 协调器：
  - [ ] 完整实现 `Draining` → `SyncingWeights`（NCCL/RDMA 显存直传）→ `FlushingCache`（同步清空 vLLM 与 EPP 全局 KV 索引）→ `Serving`（`policyVersion` 栅栏校验）四步原子状态机
- [ ] 与 `TrainPilot` GRPO 训练任务端到端联调：验证多轮采样 Prefix Cache 加速比，并确认连续 1000 次在线权重刷新零 Off-policy 缓存污染、单次同步停顿 `< 3s`
