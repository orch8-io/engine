# Test Plan: 140 E2E Tests + 250 Unit Tests

## E2E TESTS (140)

### Instances API (15)

1. **create instance with idempotency key returns same ID on retry** — POST /instances twice with same idempotency_key, assert same instance ID returned
2. **create instance with invalid sequence_id returns 404** — reference a non-existent sequence
3. **create instance with mismatched tenant_id is rejected** — tenant header vs body tenant_id mismatch
4. **batch create instances returns correct count** — POST /instances/batch with 5 items, assert count=5
5. **list instances filters by state** — create 3 instances, wait for different states, filter by each
6. **list instances filters by sequence_id** — create instances on 2 sequences, filter by one
7. **list instances pagination works** — create 10 instances, page with limit=3, verify offset/total
8. **get instance returns 404 for unknown ID** — random UUID
9. **update instance state from scheduled to paused** — PATCH /instances/{id}/state {state: "paused"}
10. **update instance context merges data** — PATCH /instances/{id}/context with new fields, verify merge
11. **retry failed instance resets to scheduled** — fail an instance, POST /instances/{id}/retry, verify state
12. **bulk update state changes multiple instances** — create 5, bulk update to cancelled
13. **bulk reschedule shifts fire times** — create 3 scheduled instances, bulk reschedule +60s
14. **DLQ lists only failed instances** — create mix of completed/failed, GET /instances/dlq
15. **inject blocks into running instance** — POST /instances/{id}/inject-blocks, verify execution tree grows

### Sequences API (8)

16. **create sequence with duplicate block IDs returns 400** — two steps with id="s1"
17. **get sequence by name returns latest non-deprecated version** — create v1, v2, deprecate v2, get returns v1
18. **list sequence versions returns all including deprecated** — create v1 (deprecated), v2, v3, verify all 3 returned
19. **delete sequence returns 404 for unknown ID** — random UUID
20. **delete sequence after all instances complete succeeds** — run instance to completion, then delete
21. **deprecate sequence hides it from by-name lookup** — deprecate, then get-by-name returns nothing
22. **migrate instance to new sequence version** — create v1 instance, create v2, migrate, verify sequence_id changed
23. **create sequence with interceptors round-trips correctly** — create with all 5 hooks, get back, verify

### Signals (10)

24. **pause signal stops execution** — send pause to running instance, verify state=paused
25. **resume signal restarts paused instance** — pause then resume, verify state=scheduled then completes
26. **cancel signal terminates instance** — send cancel, verify state=cancelled
27. **cancel signal respects non-cancellable steps** — step with cancellable=false survives cancel
28. **update_context signal merges new data** — send update_context with new fields, verify merge
29. **custom signal is delivered and recorded in audit** — send custom signal, check audit log entry
30. **multiple signals processed in order** — send pause+resume rapidly, verify final state
31. **signal to completed instance is rejected** — send pause to completed instance, expect error
32. **signal to cancelled instance is rejected** — send cancel to already-cancelled instance
33. **cancel inside cancellation_scope only cancels scope children** — scope with 2 steps + step outside scope

### Composite Blocks (12)

34. **parallel block runs all branches concurrently** — 3 branches with sleep, verify total time < sum
35. **parallel block fails if any branch fails** — one branch fails, verify instance fails
36. **race block completes on first branch success** — 2 branches (fast noop + slow sleep), verify fast wins
37. **race block with FirstToSucceed ignores failures** — first branch fails, second succeeds
38. **loop block iterates until condition is false** — counter loop 0..3, verify 3 outputs
39. **loop block respects max_iterations** — infinite-true condition with max_iterations=5
40. **forEach block iterates over collection** — array of 3 items, verify 3 step executions
41. **forEach binds item variable correctly** — forEach over ["a","b","c"], verify each step sees correct item
42. **router selects correct branch by condition** — 3 routes with conditions, verify only matching route runs
43. **router falls through to default when no match** — conditions all false, default branch runs
44. **try-catch executes catch on failure** — try block fails, catch block runs, instance completes
45. **try-catch always runs finally** — try succeeds, verify finally block still executes

### Nested Composites (5)

46. **parallel inside loop** — loop 3x with parallel(2 branches), verify 6 total step executions
47. **try-catch inside parallel branch** — one parallel branch has try-catch, failure caught, parallel succeeds
48. **loop inside forEach** — forEach over 2 items, each runs loop 2x, verify 4 iterations
49. **router inside try block** — router picks branch, that branch fails, catch handles it
50. **deeply nested 4-level composite** — parallel > loop > forEach > step, verify all steps execute

### LLM Call Block (8)

51. **llm_call with OpenAI provider returns response** — single OpenAI provider, verify response content in step output
52. **llm_call with Anthropic provider uses Messages API** — Anthropic provider, verify correct API format used
53. **llm_call provider failover cascades on 500 error** — providers: [failing_openai, working_anthropic], verify second provider used
54. **llm_call failover stops on first success** — 3 providers, first fails, second succeeds, third never called
55. **llm_call all providers fail transitions to failed** — all providers return errors, verify instance fails with cascade exhaustion
56. **llm_call with api_key_env reads key from environment** — set env var, verify handler resolves it
57. **llm_call rate limited (429) retries with backoff** — mock 429 response, verify retry behavior
58. **llm_call with system prompt passes to provider** — set system param, verify it appears in API call

### SubSequence Block (8)

59. **SubSequence creates child instance** — parent with SubSequence block, verify child instance created with parent_instance_id
60. **SubSequence waits for child completion** — child has sleep step, parent waits in Waiting state until child completes
61. **SubSequence passes input to child context.data** — input: {key: "val"}, verify child sees it in context.data
62. **SubSequence captures child outputs** — child produces outputs, parent block output contains child results
63. **SubSequence with failed child fails parent node** — child fails, verify parent SubSequence node also fails
64. **SubSequence with cancelled child fails parent** — cancel child, parent SubSequence node transitions to failed
65. **SubSequence resolves latest version by name** — create v1 and v2 of target sequence, SubSequence picks v2
66. **SubSequence with explicit version pins to that version** — version=1, even though v2 exists, runs v1

### Signal Sending & Querying (10)

67. **send_signal handler delivers signal to target instance** — instance A sends signal to instance B, verify B receives it
68. **send_signal handler with pause signal pauses target** — send pause signal, verify target transitions to Paused
69. **send_signal handler with cancel signal cancels target** — send cancel, verify target transitions to Cancelled
70. **send_signal handler with custom signal delivers payload** — send custom signal with JSON payload, verify target receives data
71. **send_signal to terminal instance returns error** — target already completed, verify send_signal step fails
72. **send_signal cross-tenant is rejected** — tenant A tries to signal tenant B's instance, verify rejection
73. **query_instance handler returns target state** — query running instance, verify state field in output
74. **query_instance handler returns target outputs** — query completed instance, verify block outputs accessible
75. **emit_event spawns child workflow via trigger** — emit_event with trigger_slug, verify child instance created
76. **emit_event with dedupe_key prevents duplicate children** — emit same dedupe_key twice, verify only one child created

### Signal-Driven Workflow Orchestration (6)

77. **workflow A signals workflow B which signals workflow C** — chain of 3 workflows communicating via signals
78. **parent workflow pauses child via signal, then resumes** — parent sends pause to child, waits, sends resume, child completes
79. **custom signal carries structured data between workflows** — workflow A sends custom signal with nested JSON, workflow B receives full payload
80. **update_context signal modifies running workflow context** — send update_context, verify subsequent steps see new context values
81. **signal ordering preserved under rapid fire** — send 5 signals rapidly, verify delivery order matches send order
82. **cancel signal with cancellation_scope protects inner steps** — cancel sent to instance with CancellationScope, inner steps complete

### Step Features (12)

83. **step with retry retries on retryable error** — handler returns retryable error, verify retry count in output
84. **step with retry gives up after max_attempts** — max_attempts=2, handler always fails, verify instance fails after 2
85. **step with exponential backoff increases delay** — max_attempts=3, verify increasing gaps between attempts
86. **step with timeout fails after duration** — handler sleeps 5s, timeout=1s, verify failure
87. **step with rate_limit_key defers when exceeded** — fire 2 instances, rate limit allows 1/min, second defers
88. **step with send_window defers outside window** — window=Mon-Fri 09:00-17:00, run on weekend, verify deferred
89. **step with context_access restricts visible fields** — step sees only declared fields, not full context
90. **step with wait_for_input pauses for human approval** — step pauses, external approval resumes it
91. **step with queue_name dispatches to named worker queue** — poll from specific queue, complete task
92. **step with deadline fails on SLA breach** — deadline=500ms, handler sleeps 2s, verify failure with breach metadata
93. **step with deadline + escalation_handler runs escalation** — deadline breached, escalation handler invoked
94. **step output memoization returns cached result on same attempt** — same attempt number returns cached output

### Worker Tasks (8)

95. **poll worker tasks returns pending tasks for handler** — create worker task, poll, verify returned
96. **complete worker task transitions instance** — poll, complete with output, verify instance progresses
97. **fail worker task with retryable=true allows retry** — fail task, verify re-queued
98. **fail worker task with retryable=false fails instance** — fail permanently, verify instance fails
99. **heartbeat extends worker task claim** — claim task, heartbeat, verify not reaped
100. **stale worker task is reaped after timeout** — claim task, don't heartbeat, verify reaped
101. **poll from named queue returns only queue-specific tasks** — 2 queues, poll each, verify isolation
102. **worker task stats returns correct counts** — create mix of pending/claimed/completed, verify stats

### Cron Schedules (5)

103. **create cron schedule with valid expression** — "0 * * * *", verify next_fire_at calculated
104. **cron fires and creates instance at scheduled time** — short interval cron, wait, verify instance created
105. **update cron schedule changes expression** — update from hourly to daily, verify next_fire_at changes
106. **disable cron stops firing** — set enabled=false, wait past fire time, verify no new instance
107. **delete cron removes schedule** — delete, verify GET returns 404

### Sessions (5)

108. **create session and list its instances** — create session, create 2 instances with session_id, list
109. **get session by key returns correct session** — create with session_key, retrieve by key
110. **update session data merges new fields** — PATCH session data, verify merge
111. **update session state transitions correctly** — active -> completed
112. **session instances inherit session context** — verify instance has access to session data

### Encryption & Security (8)

113. **encrypted context.data is unreadable in raw DB** — create with key, query DB directly, verify encrypted
114. **encryption disabled when no key set** — start without key, verify context stored in plaintext
115. **invalid encryption key rejects server startup** — set 10-char key, verify startup failure
116. **API key auth rejects unauthenticated requests** — start with api_key, send request without header
117. **API key auth accepts valid key** — send request with correct X-API-Key header
118. **cross-tenant access is blocked** — tenant A tries to read tenant B's instance
119. **tenant header required when configured** — require_tenant_header=true, omit header, verify 400
120. **context size limit rejects oversized payloads** — send context > max_context_bytes, verify 413

### Resilience & Recovery (7)

121. **stale running instance is recovered after threshold** — kill server mid-execution, restart, verify recovery
122. **circuit breaker opens after consecutive failures** — fail handler 5x, verify circuit opens
123. **circuit breaker resets after manual reset** — open circuit, POST reset, verify closed
124. **idempotency key prevents duplicate processing** — create same key twice, verify single execution
125. **persistence across server restart** — create instance, stop server, restart, verify instance still exists
126. **DLQ retry moves failed instance back to scheduled** — fail instance, retry from DLQ
127. **concurrent instance claims don't double-process** — high concurrency, verify no duplicate processing

### Observability (5)

128. **audit log records state transitions** — run instance to completion, verify audit entries for each transition
129. **execution tree shows all nodes with correct states** — run composite, GET tree, verify structure
130. **block outputs are retrievable after completion** — run 3-step sequence, GET outputs, verify 3 entries
131. **SSE stream delivers real-time instance updates** — subscribe to stream, run instance, verify events
132. **prometheus metrics endpoint returns valid output** — GET /metrics, verify counter format

### Built-in Handlers (8)

133. **http_request handler makes external HTTP call** — mock HTTP endpoint, verify request sent and response captured
134. **http_request handler blocks private IP ranges** — target 192.168.x.x, verify SSRF protection rejects it
135. **tool_call handler invokes LLM with tool definitions** — provide tools array, verify tool_choice honored in output
136. **human_review handler transitions to Waiting state** — step pauses for human review, external approval completes it
137. **self_modify handler injects blocks into running workflow** — inject 2 new steps, verify they appear in execution tree and run
138. **log handler records message in step output** — log handler with message param, verify output contains message
139. **sleep handler delays for configured duration** — sleep 500ms, verify step takes at least 500ms
140. **fail handler forces instance failure with custom message** — fail with "forced error", verify instance state=failed and error message


---

## UNIT TESTS (250)

### Expression Evaluator (25)

1. **eval path `context.data.name` returns correct value**
2. **eval path `outputs.step1.result` returns step output**
3. **eval missing path returns null**
4. **eval deeply nested path `context.data.a.b.c.d` returns value**
5. **eval string literal `"hello"` returns string**
6. **eval number literal `42` returns number**
7. **eval boolean literal `true` returns true**
8. **eval `null` returns null**
9. **eval equality `context.data.x == 5` returns true when match**
10. **eval inequality `context.data.x != 5` returns true when different**
11. **eval greater than `context.data.x > 3` returns true**
12. **eval less than `context.data.x < 10` returns true**
13. **eval greater-equal `context.data.x >= 5` exact boundary**
14. **eval less-equal `context.data.x <= 5` exact boundary**
15. **eval logical AND `true && false` returns false**
16. **eval logical OR `false || true` returns true**
17. **eval logical NOT `!false` returns true**
18. **eval arithmetic addition `3 + 4` returns 7**
19. **eval arithmetic subtraction `10 - 3` returns 7**
20. **eval arithmetic multiplication `3 * 4` returns 12**
21. **eval arithmetic division `10 / 3` returns float**
22. **eval operator precedence `2 + 3 * 4` returns 14 not 20**
23. **eval parentheses `(2 + 3) * 4` returns 20**
24. **eval string comparison `"abc" == "abc"` returns true**
25. **eval empty expression returns null**

### Template Resolution (15)

26. **resolve `{{context.data.name}}` substitutes value**
27. **resolve `{{outputs.s1.result}}` substitutes step output**
28. **resolve `{{missing.path|default_val}}` uses fallback**
29. **resolve `{{context.data.x}}` preserves number type for whole-string**
30. **resolve `{{context.data.x}}` coerces to string when inlined in text**
31. **resolve `{{context.data.obj}}` preserves object type for whole-string**
32. **resolve nested `{{context.data.a.b.c}}` works**
33. **resolve multiple templates in single string**
34. **resolve with pipe fallback `{{a|b|c}}` uses first available**
35. **resolve unicode characters preserved**
36. **resolve special chars (quotes, backslashes) preserved**
37. **resolve with empty context returns fallback or empty**
38. **resolve `{{context.config.key}}` reads config section**
39. **resolve missing root returns error (not silent)**
40. **resolve `{{outputs.missing_step.x|default}}` uses fallback**

### Delay Calculation (12)

41. **zero duration returns `from` time unchanged**
42. **negative jitter offset produces time before base**
43. **fire_at_local with valid timezone resolves to UTC**
44. **fire_at_local with invalid timezone falls back to duration**
45. **fire_at_local during DST spring-forward rolls forward**
46. **fire_at_local during DST fall-back uses earliest**
47. **business_days + Saturday holiday skips to Monday**
48. **holidays from context.config merged with step holidays**
49. **5 consecutive holidays all skipped**
50. **jitter with business_days applies jitter after skip**
51. **invalid holiday date string ignored gracefully**
52. **fire_at_local parse error falls back to duration**

### Send Window (10)

53. **inside window returns None (no deferral needed)**
54. **outside window returns next open time**
55. **wrapping window 22:00-06:00 works across midnight**
56. **empty days list means any day is valid**
57. **weekend-only window defers weekday to Saturday**
58. **start_hour == end_hour means 24h window**
59. **timezone conversion applied correctly (EST vs UTC)**
60. **next window calculation crosses week boundary**
61. **invalid timezone falls back to UTC**
62. **window with single valid day finds correct next occurrence**

### Instance State Machine (12)

63. **Scheduled -> Running is valid**
64. **Scheduled -> Completed is invalid**
65. **Running -> Completed is valid**
66. **Running -> Failed is valid**
67. **Running -> Paused is valid**
68. **Running -> Cancelled is valid**
69. **Paused -> Scheduled is valid (resume)**
70. **Paused -> Completed is invalid**
71. **Waiting -> Running is valid**
72. **Completed -> anything is invalid (terminal)**
73. **Failed -> Scheduled is valid (retry)**
74. **Cancelled -> anything is invalid (terminal)**

### Signal Processing (15)

75. **pause signal transitions Running to Paused**
76. **pause signal on Paused is noop**
77. **resume signal transitions Paused to Scheduled**
78. **resume signal on Running is noop**
79. **cancel signal transitions Running to Cancelled**
80. **cancel signal on terminal state is rejected**
81. **update_context replaces context data**
82. **custom signal is delivered without state change**
83. **malformed signal payload is discarded (non-fatal)**
84. **multiple signals processed in queue order**
85. **scoped cancel skips non-cancellable steps**
86. **scoped cancel skips CancellationScope children**
87. **scoped cancel skips try-catch finally nodes**
88. **cancel with all nodes non-cancellable does not cancel instance**
89. **signal delivery marks signal as delivered with timestamp**

### Lifecycle Transitions (8)

90. **valid transition updates state in storage**
91. **invalid transition returns error, state unchanged**
92. **transition records audit entry**
93. **transition with next_fire_at sets fire time**
94. **transition without next_fire_at clears fire time**
95. **audit entry includes from_state and to_state**
96. **audit failure is non-fatal (best-effort)**
97. **concurrent transitions are serialized (no lost updates)**

### Interceptors (10)

98. **emit_before_step saves output with correct block_id format**
99. **emit_after_step saves output with correct block_id format**
100. **emit_on_signal includes signal payload in output**
101. **emit_on_complete saves output**
102. **emit_on_failure saves output**
103. **no interceptor configured produces no output**
104. **interceptor with non-object params wraps in `_params`**
105. **storage failure in interceptor is logged but non-fatal**
106. **interceptor output_size matches serialized length**
107. **emit_on_signal merges `_signal` into object params**

### Encryption (12)

108. **encrypt_value produces `enc:v1:` prefixed string**
109. **decrypt_value reverses encryption exactly**
110. **each encryption produces unique ciphertext (random nonce)**
111. **non-encrypted value passes through decrypt unchanged**
112. **is_encrypted detects `enc:v1:` prefix**
113. **is_encrypted returns false for plain values**
114. **from_hex_key rejects wrong length (not 64 chars)**
115. **from_hex_key rejects invalid hex characters**
116. **dual-key decrypts data encrypted with old key**
117. **dual-key encrypts new data with primary key only**
118. **dual-key primary key takes priority over old key**
119. **with_old_key rejects invalid hex key**

### Parallel Handler (8)

120. **all branches complete -> parallel completes**
121. **any branch fails -> parallel fails**
122. **empty branch list -> parallel completes immediately**
123. **single branch acts like sequential**
124. **branches execute in parallel (not sequential)**
125. **branch index groups nodes correctly**
126. **untagged children default to branch 0**
127. **partial branch completion waits for remaining**

### Race Handler (8)

128. **first branch completing wins the race**
129. **losing branches are cancelled**
130. **worker tasks in losing branches are cancelled**
131. **all branches fail -> race fails**
132. **empty race completes immediately**
133. **single branch race completes normally**
134. **FirstToSucceed ignores failed branches**
135. **FirstToResolve treats failure as resolution**

### Try-Catch Handler (10)

136. **try success skips catch, runs finally**
137. **try failure runs catch then finally**
138. **catch success recovers from try failure**
139. **catch failure -> instance fails (after finally)**
140. **finally always runs even when try+catch both fail**
141. **error context injected into catch block**
142. **error context contains failed block IDs**
143. **empty try block -> immediate success**
144. **empty catch block -> noop on failure**
145. **empty finally block -> noop**

### Loop Handler (10)

146. **loop iterates correct number of times**
147. **loop stops when condition becomes false**
148. **loop respects max_iterations cap**
149. **loop counter increments correctly per iteration**
150. **infinite loop hits LOOP_ABSOLUTE_MAX (1M)**
151. **loop with false initial condition runs zero times**
152. **nested loop resets inner iteration counter**
153. **loop body nodes reset to Pending on new iteration**
154. **loop preserves step outputs from prior iterations**
155. **falsy values (0, "", null, false, []) stop loop**

### ForEach Handler (8)

156. **forEach iterates over array of 3 items**
157. **forEach binds item_var in context.data for each item**
158. **forEach with empty collection runs zero iterations**
159. **forEach with path reference resolves collection from context**
160. **forEach respects max_iterations**
161. **forEach hits FOR_EACH_ABSOLUTE_MAX on huge collection**
162. **forEach with non-array collection treats as single-item**
163. **forEach cleans up descendant markers between iterations**

### Router Handler (8)

164. **router picks first matching route**
165. **router skips non-matching routes**
166. **router uses default when no route matches**
167. **router fails when no match and no default**
168. **router evaluates conditions against current context**
169. **router with single route always matches or falls to default**
170. **router inflates externalized markers before eval**
171. **router condition evaluation error skips route**

### AB Split Handler (6)

172. **deterministic variant selection based on instance_id hash**
173. **weight distribution is approximately correct over many instances**
174. **non-chosen variants are Skipped**
175. **chosen variant executes normally**
176. **single variant always selected**
177. **variant with weight=0 is never selected**

### LLM Handler (10)

178. **single provider OpenAI call constructs correct request**
179. **single provider Anthropic call uses Messages API format**
180. **provider failover tries all providers in order**
181. **failover stops on first success**
182. **failover returns error when all fail**
183. **merge_provider_params overlays fields correctly**
184. **merge_provider_params removes `providers` key**
185. **resolve_api_key reads from env var**
186. **resolve_api_key reads from direct param**
187. **resolve_base_url returns correct URL per provider**

### Step Handler (10)

188. **execute_step returns Completed on success**
189. **execute_step returns Failed on permanent error**
190. **execute_step returns Deferred on retryable error**
191. **backoff calculation doubles each attempt**
192. **backoff respects max_backoff cap**
193. **backoff multiplier applied correctly**
194. **timeout cancels handler after duration**
195. **output externalization triggers above threshold**
196. **memoized output returned for same attempt**
197. **unknown handler returns error**

### Scheduler Core (15)

198. **claim_due_instances respects batch_size**
199. **claim_due_instances skips instances with future fire_at**
200. **concurrency key limits parallel execution**
201. **max_concurrency=1 enforces sequential per key**
202. **debug breakpoint pauses instance**
203. **delay check defers instance with correct fire_at**
204. **rate limit check defers when exceeded**
205. **human input check transitions to Waiting**
206. **SLA deadline breach transitions to Failed**
207. **SLA deadline with escalation runs escalation handler**
208. **external worker dispatch creates worker task**
209. **self-modify injects blocks at correct position**
210. **parent instance woken when child completes**
211. **empty sequence blocks list completes immediately**
212. **sequence cache hit avoids storage query**

### Evaluator (12)

213. **evaluate creates execution tree from block definitions**
214. **evaluate activates first pending root node**
215. **evaluate dispatches Running step nodes**
216. **evaluate re-evaluates composites deepest-first**
217. **evaluate detects decided race and skips step**
218. **evaluate stops after max 200 iterations per tick**
219. **find_block locates block at any nesting depth**
220. **children_of filters by parent and branch**
221. **all_terminal returns true when all nodes completed/failed/skipped**
222. **has_waiting_nodes detects external work**
223. **injected blocks merged into block list without rebuild**
224. **new nodes added for injected blocks only**

### Storage Backend (20)

225. **create_instance and get_instance round-trip all fields**
226. **create_instance with duplicate ID returns conflict error**
227. **claim_due_instances respects max_per_tenant fairness**
228. **update_instance_state validates transition (Completed -> Running fails)**
229. **merge_context_data adds key without overwriting existing**
230. **count_instances with filter returns correct count**
231. **bulk_update_state changes only matching instances**
232. **save_block_output and get_block_output round-trip**
233. **get_completed_block_ids returns only completed step IDs**
234. **enqueue_signal_if_active rejects signal to terminal instance**
235. **mark_signals_delivered marks batch atomically**
236. **find_by_idempotency_key returns correct instance**
237. **recover_stale_instances transitions stale Running to Scheduled**
238. **create_cron_schedule and claim_due_cron round-trip**
239. **claim_worker_tasks marks tasks as Claimed**
240. **complete_worker_task sets output and state**
241. **reap_stale_worker_tasks reaps unclaimed tasks**
242. **save_externalized_state and get round-trip**
243. **delete_expired_externalized_state removes old entries**
244. **append_audit_log and list_audit_log round-trip**

### Config & Validation (8)

245. **SecretString redacts in Debug output**
246. **SecretString expose() returns original value**
247. **SequenceDefinition with duplicate block IDs returns validation error**
248. **DelaySpec serializes duration as milliseconds**
249. **RetryPolicy defaults: max_attempts=3, backoff_multiplier=2.0**
250. **InstanceFilter with no fields matches all instances**
251. **Pagination defaults: offset=0, limit=100**
252. **WebhookConfig with empty URLs produces no webhooks**

### Webhooks (5)

253. **webhook sends POST with correct payload**
254. **webhook retries on 5xx response**
255. **webhook gives up after max_retries**
256. **webhook timeout fires after configured duration**
257. **empty webhook URL list is noop**

### Cron (5)

258. **cron expression `0 * * * *` calculates correct next_fire_at**
259. **cron with timezone adjusts fire time**
260. **cron claim updates last_triggered_at and next_fire_at**
261. **disabled cron is not claimed**
262. **cron with invalid expression returns error on create**

### Rate Limiting (3)

263. **check_rate_limit under threshold returns allowed**
264. **check_rate_limit at threshold returns exceeded with retry_after**
265. **upsert_rate_limit updates window correctly**

### Concurrency (5)

266. **count_running_by_concurrency_key returns accurate count**
267. **concurrency_position returns queue position**
268. **no concurrency_key means no limit applied**
269. **max_concurrency=0 blocks all execution**
270. **concurrent claims with same key are serialized**

### Metrics (5)

271. **INSTANCES_COMPLETED counter increments on completion**
272. **STEPS_EXECUTED counter increments per step**
273. **TICK_DURATION histogram records tick time**
274. **QUEUE_DEPTH gauge reflects current queue size**
275. **Timer struct records duration on drop**

### Auth & Tenant Isolation (5)

276. **enforce_tenant_access rejects cross-tenant read**
277. **enforce_tenant_create validates tenant header matches body**
278. **api_key_middleware uses constant-time comparison**
279. **missing X-Tenant-Id header returns 400 when required**
280. **scoped_tenant_id prefers header over query param**

### Error Mapping (5)

281. **StorageError::NotFound maps to ApiError::NotFound (404)**
282. **StorageError::Conflict maps to ApiError::AlreadyExists (409)**
283. **ApiError::Conflict maps to 409**
284. **ApiError::PayloadTooLarge maps to 413**
285. **StepError::Retryable maps to EngineError with retryable=true**

### Recovery & GC (5)

286. **recover_stale_instances finds instances stuck in Running**
287. **recovery skips instances within threshold**
288. **GC deletes expired externalized state**
289. **GC emit dedupe cleanup removes old entries**
290. **recovery counter metric incremented on recovery**

### EncryptingStorage Decorator (5)

291. **create_instance encrypts context.data before storage**
292. **get_instance decrypts context.data after retrieval**
293. **non-instance methods pass through without encryption**
294. **delete_sequence delegates to inner storage**
295. **batch create encrypts all instances in batch**

### LLM Call Handler (12)

296. **build_openai_request constructs correct chat completion body**
297. **build_anthropic_request uses Messages API format with system field**
298. **resolve_api_key prefers direct param over env var**
299. **resolve_api_key returns error when neither param nor env set**
300. **resolve_base_url returns correct URL for each of 10 providers**
301. **merge_provider_params overlays model and api_key from provider entry**
302. **merge_provider_params strips `providers` array from merged params**
303. **classify_error maps 429 to Retryable**
304. **classify_error maps 401/403 to Permanent**
305. **classify_error maps 5xx to Retryable**
306. **classify_error maps network timeout to Retryable**
307. **failover loop skips provider with unresolvable credentials**

### SubSequence Handler (8)

308. **SubSequence creates child with correct parent_instance_id**
309. **SubSequence passes input as child context.data**
310. **SubSequence sets _parent_block_id in child metadata**
311. **SubSequence resolves latest version when version omitted**
312. **SubSequence pins to explicit version when specified**
313. **SubSequence node transitions to Waiting after child creation**
314. **SubSequence completes when child reaches Completed**
315. **SubSequence fails when child reaches Failed or Cancelled**

### Send Signal Handler (8)

316. **send_signal enqueues signal via storage**
317. **send_signal rejects signal to terminal instance**
318. **send_signal validates cross-tenant access**
319. **send_signal constructs correct SignalPayload for pause**
320. **send_signal constructs correct SignalPayload for custom type**
321. **send_signal returns signal_id in output**
322. **send_signal with missing instance_id param returns error**
323. **send_signal with invalid UUID returns error**

### Query Instance Handler (5)

324. **query_instance returns target state and metadata**
325. **query_instance returns block outputs when requested**
326. **query_instance validates same-tenant access**
327. **query_instance returns error for non-existent instance**
328. **query_instance does not expose context.data (read isolation)**

### Emit Event Handler (10)

329. **emit_event creates child instance from trigger_slug**
330. **emit_event passes data as child context.data**
331. **emit_event sets parent_instance_id on child**
332. **emit_event with dedupe_key prevents duplicate child creation**
333. **emit_event dedupe_scope=parent scopes to parent instance**
334. **emit_event dedupe_scope=tenant scopes to tenant**
335. **emit_event returns instance_id and deduped flag**
336. **emit_event with missing trigger_slug returns error**
337. **emit_event merges meta fields into child metadata**
338. **emit_event deduped=true returns existing instance_id**

### CancellationScope Handler (7)

339. **CancellationScope protects children from external cancel**
340. **CancellationScope allows internal failure to propagate**
341. **CancellationScope completes normally when not cancelled**
342. **CancellationScope children run to completion during cancel**
343. **nested CancellationScope protects inner scope independently**
344. **CancellationScope with cancellable=true overrides protection**
345. **CancellationScope node marked Completed after children finish**

### Misc Edge Cases (5)

346. **required_fields returns None for unknown block (full access)**
347. **collect_holidays skips unparseable date strings**
348. **compress/decompress round-trips large payloads**
349. **BlockId display format matches expected string**
350. **ExecutionContext merge preserves existing keys not in patch**
