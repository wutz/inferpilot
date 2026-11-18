# Operator 与 CRD 规范

API Group：`inferpilot.wutz.dev/v1alpha1`（遵循 `*.wutz.dev` 家族统一约定，采用 Kubebuilder v4 脚手架标准设计）。

InferPilot 引入五个职责边界正交的 CRD，分别覆盖**南向模型池编排、轻量 LoRA 热挂载、RL 在线权重同步、北向路由发布与用户 Token 计量限速**：

1. **`InferenceService`**（简称 `isvc`）：声明一个推理模型池（支持平台公共池、共享 Multi-LoRA 基座池或租户独占池），自动编译为底层 `llm-d` `InferencePool` + `EPP` + `Deployment`/`LeaderWorkerSet`/PD 分离拓扑及 `KEDA` 弹性策略。
2. **`LoRABinding`**（简称 `lora`）：声明将一个 `ModelDock` 中的 `LoRAAdapter` 动态热加载至运行中的共享基座 `InferenceService`，零新增 GPU 秒级上线。
3. **`RolloutChannel`**（简称 `rchan`）：面向强化学习（GRPO / PPO）在线采样场景，绑定上游 `TrainPilot` Actor 任务与下游 `InferenceService` 采样池，管控原子权重热更新与 KV Cache 强制刷新。
4. **`ModelRoute`**（简称 `mroute`）：声明对外暴露的逻辑模型名（如 `qwen3-32b` 或租户自定义端点）、金丝雀灰度切流比例及 Fallback 降级池。
5. **`TokenRateLimitPolicy`**（简称 `trlp`）：声明面向平台公共 MaaS 或租户共享网关的用户级 RPM / TPM / 并发流限速、默认 `max_tokens` 约束及 EPP 调度优先级映射。

---

## 1. `InferenceService` CRD 定义

### 1.1 示例清单（全功能字段展示）

```yaml
apiVersion: inferpilot.wutz.dev/v1alpha1
kind: InferenceService
metadata:
  name: qwen3-32b-shared-base
  namespace: infer-shared
  labels:
    workspace.wutz.dev/project: platform
spec:
  displayName: "Qwen3-32B 共享基座推理池 (支持动态 Multi-LoRA)"
  desiredState: Running                  # Running | Stopped (Stopped 时释放全部 GPU Pod，保留配置与历史计费段)
  poolMode: SharedMultiLoRA              # DedicatedTenant | PublicMaaS | SharedMultiLoRA | RLRollout

  # 商业计费模式（租户独占模式 DedicatedTenant 下启用按实例段落计费，对齐 TrainPilot）
  billingMode: OnDemand                  # OnDemand | Spot
  pricePerReplicaHour: "16.00"           # 每个推理副本组每小时单价 (CNY/小时)

  # 模型来源：强绑定 ModelDock 中的 ModelArtifact（禁止裸写公网 HuggingFace ID）
  modelRef:
    namespace: modeldock-public
    name: qwen3-32b-instruct-bf16

  # 引擎与运行时配置
  engine:
    type: vllm                           # vllm | sglang | triton
    image: cr.example.com/public/vllm-openai:v0.8.5-cuda12.4
    maxModelLen: 32768
    gpuMemoryUtilization: "0.90"
    extraArgs: []

  # 拓扑与计算资源（topology=Auto 时根据单机卡数与 TP×PP 自动选择 SingleNode 或 MultiNode LWS）
  topology:
    mode: SingleNode                     # Auto | SingleNode | MultiNode | Disaggregated
    tensorParallelSize: 2
    pipelineParallelSize: 1
    expertParallel: false
    resourcesPerWorker:
      gpus: 2
      gpuModel: H800-80G                 # 匹配集群 GPU 机型标签（如 scclabs.dev/gpu）
      cpu: "16"
      memory: 160Gi
      shmRatio: "0.5"                    # 自动挂载 80Gi Memory emptyDir 到 /dev/shm

  # llm-d 智能路由与 KV Cache 策略
  routingAndCache:
    kvCacheRoutingMode: PreciseEvents    # Heuristic | PreciseEvents (开启 vLLM -> EPP ZMQ BlockStored 事件流)
    enforceTenantCacheSalt: true         # 强制按租户隔离 Prefix Cache 哈希命名空间，防 TTFT 侧信道探测
    cpuOffloadGiB: 32                    # 开启主机内存二级 KV Cache 卸载容量（0 表示关闭）
    targetTTFTMilliseconds: 800          # 供 EPP 延迟预测器与过载保护参考的 SLO 目标
    targetTPOTMilliseconds: 40

  # 动态 Multi-LoRA 容量规划（当 poolMode=SharedMultiLoRA 时生效）
  multiLoRA:
    enabled: true
    maxActiveLoRAsPerReplica: 16         # 单个副本 GPU 显存内同时驻留的最大 LoRA 槽位数
    maxCPULoRAsPerReplica: 64            # 主机内存二级热备槽位数
    maxLoRARank: 64                      # 准入期 Webhook 拦截超过此 Rank 的 LoRABinding

  # 基于 KEDA + EPP 饱和度指标的自动扩缩容
  autoscaling:
    minReplicas: 2                       # 支持设为 0（配合网关激活实现 Scale-to-Zero）
    maxReplicas: 8
    scaleUpQueueDepthThreshold: 12       # EPP 平均每副本等待队列超过 12 时扩容
    scaleUpKVCacheThresholdPercent: 82   # 平均 KV Cache 占用率持续超 82% 时扩容
    stabilizationWindowSeconds: 180
status:
  phase: Ready                           # Pending | WarmingCache | Starting | Ready | Updating | Degraded | Stopped
  activeModelDigest: "sha256:4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
  boundModelCache: mcache-qwen3-32b-bf16
  inferencePoolName: ipool-qwen3-32b-shared-base
  eppServiceEndpoint: qwen3-32b-shared-base-epp.infer-shared.svc:9002
  readyReplicas: 2
  desiredReplicas: 2
  activeLoRABindings: 14
  segments:                              # 对齐 TrainPilot 的运行计费段落账本
    - replicaCount: 2
      pricePerReplicaHour: "16.00"
      startTime: "2026-11-18T08:00:00Z"
      endTime: null
  conditions:
    - type: CacheReady
      status: "True"
      reason: CASVolumeMounted
    - type: InferencePoolReady
      status: "True"
      reason: EPPHealthy
```

### 1.2 PD 分离模式（`topology.mode: Disaggregated`）扩展示例片段

当针对高吞吐公共大模型启用 Prefill / Decode 分离时，`topology.disaggregated` 独立声明两池规格：

```yaml
  topology:
    mode: Disaggregated
    kvTransferBackend: nixl-rdma         # nixl-rdma | mooncake-rdma (要求节点具备 RDMA / RoCE HCA)
    prefillPool:
      replicas: 4
      tensorParallelSize: 2
      resourcesPerWorker:
        gpus: 2
        gpuModel: H800-80G
        rdmaHCA: 1
    decodePool:
      replicas: 2
      tensorParallelSize: 4
      resourcesPerWorker:
        gpus: 4
        gpuModel: H800-80G
        rdmaHCA: 1
```

---

## 2. `LoRABinding` CRD 定义（租户微调极速上线）

租户在自己的工作空间 Namespace（如 `forge-acme`）创建 `LoRABinding`，无需申请新 GPU 卡即可把 `TrainPilot` 微调导出的适配器挂载至平台共享基座池：

```yaml
apiVersion: inferpilot.wutz.dev/v1alpha1
kind: LoRABinding
metadata:
  name: acme-finance-copilot-v1
  namespace: forge-acme
  labels:
    workspace.wutz.dev/project: acme
spec:
  exposedModelName: "acme/finance-copilot:v1.2"  # 租户通过统一网关调用时填写的 model 字段值
  adapterArtifactRef:
    namespace: forge-acme
    name: finance-qwen3-lora-r32-step1200        # 指向 ModelDock 中的 LoRAAdapter 制品
  targetBaseServiceRef:
    namespace: infer-shared
    name: qwen3-32b-shared-base                  # 指向开启了 SharedMultiLoRA 的基座池
status:
  phase: Active                          # Pending | WarmingCache | Loading | Active | Unloading | Failed
  loadedReplicas: 2                      # 已就绪或可按需热拉取的副本数
  registeredInEPP: true
```

- **ValidatingWebhook 准入强校验**：
  1. 校验 `adapterArtifactRef` 的 `artifactType == LoRAAdapter`；
  2. 校验 `adapterArtifactRef.spec.baseModelRef.ociDigest` 与目标 `targetBaseServiceRef` 当前运行的基座 Digest 严格一致；
  3. 校验 `adapterArtifactRef.spec.loraSpec.rank <= targetBaseServiceRef.spec.multiLoRA.maxLoRARank`。

---

## 3. `RolloutChannel` CRD 定义（强化学习在线采样协同）

专为 GRPO / PPO / 多轮 Agentic RL 在线训练设计，管理推理采样池与训练 Actor 之间的秒级权重同步栅栏：

```yaml
apiVersion: inferpilot.wutz.dev/v1alpha1
kind: RolloutChannel
metadata:
  name: grpo-finance-rollout-chan
  namespace: forge-acme
spec:
  # 绑定的上游强化学习训练任务（TrainPilot）与下游专属推理池
  trainTaskRef:
    namespace: forge-acme
    name: grpo-finance-run04
  rolloutServiceRef:
    name: qwen3-32b-rl-rollout-pool      # poolMode 必须为 RLRollout

  transport:
    protocol: nccl-rdma                  # nccl-rdma | nixl-rdma
    drainTimeoutSeconds: 30              # 触发权重同步前等待在途长轨迹生成排空的最大宽限时间
    abortInFlightOnTimeout: true         # 超时未结束的长尾 Rollout 请求强制中断以防阻塞训练 Step
    flushEPPAndEngineKVCache: true       # 每次权重刷新后强制清空 vLLM 与 EPP 全局 Prefix Cache（默认强制开启）
status:
  phase: Serving                         # Idle | Draining | SyncingWeights | FlushingCache | Serving | Faulted
  activePolicyVersion: "step-1200"
  lastSyncDurationMilliseconds: 1840     # 最近一次 Drain + RDMA 广播 + 清空 KV 总耗时
  totalSyncCount: 1200
```

---

## 4. `ModelRoute` 与 `TokenRateLimitPolicy` CRD 定义（北向网关与计量限速）

### 4.1 `ModelRoute`（多版本灰度切流与模型目录发布）

```yaml
apiVersion: inferpilot.wutz.dev/v1alpha1
kind: ModelRoute
metadata:
  name: public-qwen3-32b
  namespace: infer-public
spec:
  modelName: "Qwen/Qwen3-32B"            # 客户端请求 JSON 中传入的 "model" 名称
  visibility: Public                     # Public (所有持有效 API Key 用户可调) | TenantPrivate (仅本租户可调)
  pricing:                               # 计费目录单价（CNY / 百万 Token，供异步账单引擎计算费用）
    inputPricePerMillion: "2.00"
    cachedInputPricePerMillion: "0.40"   # 命中 llm-d Prefix Cache 的优惠单价
    outputPricePerMillion: "8.00"
  backends:
    - serviceRef:
        name: qwen3-32b-prod-v1
      weight: 90
    - serviceRef:
        name: qwen3-32b-canary-v2        # 新模型晋级灰度 10% 流量验证
      weight: 10
  fallbackServiceRef:
    name: qwen3-32b-shared-backup        # 主池饱和熔断时的降级备用池
```

### 4.2 `TokenRateLimitPolicy`（按每个用户/等级 Token 限速与优先级控制）

```yaml
apiVersion: inferpilot.wutz.dev/v1alpha1
kind: TokenRateLimitPolicy
metadata:
  name: global-maas-tier-policy
  namespace: infer-public
spec:
  targetGatewayRef:
    name: inferpilot-ai-gateway
  defaultTier: free
  enforceStreamIncludeUsage: true        # 强制改写流式请求注入 include_usage=true
  enableAbortedStreamFallbackMetering: true # 开启客户端中途断流网关侧 Chunk 补偿计费

  tiers:
    - name: enterprise
      eppPriorityLevel: 100              # 映射至 llm-d EPP Flow Control 最高优队列
      fairnessWeight: 10
      limitsPerUser:
        requestsPerMinute: 600
        tokensPerMinute: 1000000         # 每用户每分钟 1M Tokens
        maxConcurrentStreams: 32         # 单用户同时保持的最大活跃 SSE 流数
        maxOutputTokensPerRequest: 16384 # 未传或超限时网关强制钳制 max_tokens
    - name: pro
      eppPriorityLevel: 50
      fairnessWeight: 3
      limitsPerUser:
        requestsPerMinute: 120
        tokensPerMinute: 200000
        maxConcurrentStreams: 8
        maxOutputTokensPerRequest: 8192
    - name: free
      eppPriorityLevel: 10
      fairnessWeight: 1
      limitsPerUser:
        requestsPerMinute: 15
        tokensPerMinute: 20000
        maxConcurrentStreams: 2
        maxOutputTokensPerRequest: 4096
```
