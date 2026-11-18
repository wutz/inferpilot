# 北向网关、Token 精准计量与限速架构

本文档详细阐述 InferPilot 如何面向“提供已部署好的模型，按每个用户使用 Token 计量和限速”场景，构建高吞吐、防逃单、低延迟的数据面治理闭环。

---

## 1. 请求全生命周期数据面流水线

客户端发起一次流式对话请求 `POST /v1/chat/completions` 在单层 Envoy 网关内的精确处理时序如下：

```text
客户端 ──(1. Bearer sk-xxx + JSON Body)──▶ Envoy Proxy
                                              │
   ┌──────────────────────────────────────────┘
   ▼
[阶段一：身份认证与入口预检 (Request Headers & Body Phase)]
  1. 校验 API Key 有效性、账户是否欠费停机（本地内存 LRU 缓存 5s + 控制面失效推送）
  2. 剥离客户端可能伪造的所有 x-inferpilot-* 内部头
  3. 执行 Redis Lua 原子预检脚本：
     - 校验 user_rpm < limit_rpm
     - 校验并递增 user_active_streams <= max_concurrent_streams
     - 校验 user_tpm_bucket > 0（若 <= 0 直接返回 HTTP 429 Too Many Requests + Retry-After）
  4. 改写请求 JSON：
     - 钳制 max_tokens <= tier.maxOutputTokensPerRequest
     - 若 stream==true，强制写入 "stream_options": {"include_usage": true}
     - 若模型池开启租户缓存隔离，注入租户专属 cache_salt
  5. 注入 x-inferpilot-priority 头，调用 llm-d EPP (ext-proc) 选出最优后端 Pod
   │
   ▼
[阶段二：SSE 流式转发与实时增量观测 (Response Body Streaming Phase)]
  - 零缓冲透传每个 data: chunk 给客户端，确保不劣化 TTFT 与 TPOT
  - 旁路计量状态机实时记录：首包到达时刻 (ttft_ms)、累计输出增量字符/分段、以及尾帧 usage 对象
   │
   ▼
[阶段三：流结束结算与异步出账 (Stream Complete / OnDestroy Phase)]
  1. 原子递减 Redis user_active_streams 并发流计数
  2. 判定完成状态：
     - 正常收到权威 usage 帧 ──▶ 提取 prompt_tokens / cached_tokens / completion_tokens / reasoning_tokens
     - 客户端中途断流/超时无 usage 帧 ──▶ 触发网关本地分词器补偿计数，标记 completion_state="aborted"
  3. 异步调用 Redis 扣减该用户 TPM 桶实际消耗总 Token 数
  4. 将结构化 UsageEvent 写入本地持久化环形缓冲并批量投递至 Kafka
```

---

## 2. 双阶段 Token 限速算法细节（Redis Lua 原子实现）

传统 API 网关在入口即可得知请求权重（Weight = 1），而 LLM 一次请求可能消耗 `100` 个 Token，也可能消耗 `32,000` 个 Token。如果入口预扣 `max_tokens`，会导致正常短对话用户的配额被严重虚占；如果入口完全不拦，恶意用户可在同一秒内并发打入 100 个长生成请求击穿配额。

InferPilot 采用 **“并发流硬门禁 + 预估输入软校验 + 真实消耗后置扣减（Debt-aware Token Bucket）”**：

1. **三维联合约束**：
   - **`maxConcurrentStreams`（防瞬时并发击穿）**：这是阻断突发透支的第一道闸门。例如 `free` 用户最多同时保持 `2` 条活跃流，即便单次请求生成较长，最大瞬时透支上界严格受控于 `maxConcurrentStreams × maxOutputTokensPerRequest`。
   - **入口输入字节快速折算**：入口根据请求 JSON 字节长度按保守经验比（如 `bytes / 3`）估算保底 Input Tokens，若仅估算输入已远超剩余 TPM 桶额度则直接返回 `429`，无需浪费后端 Prefill 算力。
   - **允许单次微透支（Negative Balance Debt）**：假设用户每分钟额度为 `20,000` Tokens，当前剩余 `2,000` Tokens 时发起了一次合法请求，最终实际消耗 `5,000` Tokens。流结束扣减后桶余额变为 `-3,000`；在随后的约 9 秒内令牌按恒定速率（`20000 / 60 ≈ 333 tokens/s`）回填至正数前，该用户的新请求在入口阶段将被毫秒级拦截，实现长期速率严格收敛于设定 TPM。
2. **故障降级策略（Fail-open / Local Fallback）**：当 Redis 发生瞬时网络抖动时，网关自动退化为基于每个 Envoy Worker 进程内存的本地令牌桶限速，绝不因限速存储抖动导致全站推理服务不可用。

---

## 3. 防漏账与幂等计量账本设计

### 3.1 为什么必须防范“流式断流逃单”？

在 OpenAI SSE 协议中，`usage` 统计信息仅在流结束前的倒数第二帧（`finish_reason: "stop"` 之后、`data: [DONE]` 之前）发送。若恶意调用方在收到所需代码或答案结尾符号的瞬间主动调用 `abort()` 关闭连接，连接会在 `usage` 帧发出前断开。

InferPilot 在 `inferpilot-meter` 组件中实现双轨保障：
1. **引擎侧断流上报 + 网关侧实时累加双重兜底**：网关在内存中为每条活跃流维护轻量级增量文本收集器（上限截断保护内存）与 UTF-8 码点计数器；一旦检测到下游 TCP RST/FIN 早于 `usage` 帧到达，立即使用模型对应的分词词表（如 `Qwen`/`TikToken` 高速 Rust/Go 绑定）计算已生成文本的精确 Token 数。
2. **一请求一唯一单号（Idempotency Key）**：每条进入网关的请求由网关分配全局唯一 `request_id`（UUIDv7，天然按时间有序），下游消费链路（Kafka → ClickHouse / `kubemeter`）以 `(tenant_id, date, request_id)` 作为去重主键（ReplacingMergeTree），彻底消除消息重投导致的重复扣费。

### 3.2 标准计量事件 Schema（投递至 Kafka `inferpilot.usage.v1`）

```json
{
  "event_version": "v1",
  "request_id": "01933e2a-7c90-7b12-9a44-8f01c2d3e4f5",
  "timestamp": "2026-11-18T10:15:32.418Z",
  "tenant_id": "acme",
  "user_id": "usr_89102",
  "api_key_id": "key_prod_03",
  "priority_tier": "pro",
  "requested_model": "Qwen/Qwen3-32B",
  "routed_service": "infer-public/qwen3-32b-prod-v1",
  "model_artifact_digest": "sha256:4b227777d4dd...",
  "completion_state": "completed",
  "usage_source": "engine_authoritative",
  "prompt_tokens": 620,
  "cached_prompt_tokens": 4096,
  "completion_tokens": 845,
  "reasoning_tokens": 312,
  "total_tokens": 5561,
  "ttft_ms": 184,
  "duration_ms": 9420
}
```

### 3.3 预付费余额实时熔断闭环

计费汇总服务消费 `inferpilot.usage.v1` 实时扣减租户/用户钱包余额：
- 当账户余额跌破 `0` 时，计费服务向控制面写入账户冻结状态并通过 Pub/Sub（或 K8s Secret/ConfigMap Watch）在 **1 秒内**广播至所有 Envoy 网关节点，后续请求直接在阶段一返回 `402 Payment Required`；
- 对于租户独占部署的 `InferenceService`（按 GPU 实例时长计费），欠费宽限期届满后由控制面自动将其 `spec.desiredState` 置为 `Stopped`，释放 GPU 资源并闭合当前计费段 `status.segments[]`。

---

## 4. 多租户安全与隔离红线

1. **Prefix Cache 侧信道隔离**：所有多租户共享池（`PublicMaaS` 与 `SharedMultiLoRA`）默认开启 `enforceTenantCacheSalt: true`。租户 A 与租户 B 即便发送一模一样的私有 Prompt，在 `llm-d` EPP 与 `vLLM` PagedAttention 内部也会映射为完全不相交的 Block Hash，杜绝跨租户缓存探测。
2. **东西向网络零信任隔离**：租户独占 `InferenceService` 部署在租户自有 Namespace 下，Operator 自动下发 `NetworkPolicy`——仅允许统一网关 Envoy 与对应 `EPP` 访问推理 Pod 的服务端口（以及在绑定了 `RolloutChannel` 时允许同租户指定 `TrainTask` Worker 访问 RDMA/NCCL 同步端口），禁止跨租户 Pod 直接绕过网关调取推理接口逃避计费。
3. **内部管理接口物理隔离**：vLLM 的动态加载 LoRA 接口（`/v1/load_lora_adapter`）、清空缓存接口（`/reset_prefix_cache`）与权重同步接口在网关层严格屏蔽外部路径映射，仅允许集群内 `InferPilot` 控制器与 `inferpilot-rl-coord` 凭 mTLS / ServiceAccount Token 调用。
