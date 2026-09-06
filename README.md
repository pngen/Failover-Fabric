Failover Fabric is an open-source, vendor-neutral C++20 runtime for governing service failover, replacement selection, promotion authority, traffic cutover, failure-domain eligibility, and SLO-aware recovery across heterogeneous accelerator infrastructure.

It answers one systems question:

> When an active serving target fails or becomes ineligible, which replacement can safely assume service authority, what must be fenced and revalidated before traffic moves, and can the resulting recovery satisfy the service's stated continuity and recovery requirements?

## Defining thesis

Service failover is an authoritative transition across targets, state, and routes. A replacement becomes current only when its eligibility is proven, old authority is fenced at the governed boundary, and new service execution is verified.

Detecting a failure is not the same as completing a safe failover. This runtime makes the service transition explicit:

*failure evidence → eligibility evaluation → replacement selection → preparation coordination → old-authority fencing → promotion authorization → target activation → route cutover → verification → recovery closure → standby replenishment intent*

## Systems boundary

Failover Fabric owns service-level failover decisions and authority transitions across eligible engines, replicas, nodes, and failure domains. It owns service identity, configuration generation, assignment, active-target authority, failover scope, failure-evidence consumption, failure-domain exclusions, candidate eligibility, deterministic replacement selection, failover plans and attempts, promotion authorization, fencing requirements, route-cutover coordination, cutover verification, recovery state, continuity requirements, recovery-budget evaluation, failback policy, anti-flapping policy, failover history, conservative recovery, and inspectable explanations.

It deliberately does **not** own engine preparation (Engine Residency), generic replica lifecycle (Replica Fabric), arbitrary recovery planning (Recovery Planner), universal failure detection (Failure Fabric / Accelerator Health), SLO definitions (SLO Fabric), request scheduling (Scheduler/Batch/Prefill/Decode), execution-attempt correctness (Execution Fabric), durable workload identity (Workload Fabric), dependency propagation (Dependency Fabric), model/KV/checkpoint storage (Model/KV/State/Checkpoint runtimes), scarce-resource arbitration (Resource/Reservation Fabric), or a service mesh / load balancer. It provides a narrow routing contract and a real reference gateway sufficient to prove cutover.

## Authority model

Every versioned concept has a distinct strongly typed generation (ServiceGeneration, AssignmentGeneration, ServiceAuthorityGeneration, RouteGeneration, PromotionGeneration, GatewayBootId, WorkerBootId, CoordinatorEpoch, and so on). A stale RouteGeneration must not route new work to an old target; a stale PromotionGeneration must not activate a replacement; a stale WorkerBootId must not republish an old target as current. Generation advance refuses to wrap: reaching its maximum throws GenerationExhausted so old valid authority is never reused.

At most one exclusive assignment may authorize new work for a service slot at a time. Historical assignments remain inspectable but non-authoritative. A set_initial_assignment that would double-assign throws. Coordinator authority is a durable, monotonic epoch; ownership is established by a single reference coordinator over a loopback-bound control port, and fail-closes if ownership cannot be established. This reference does **not** claim distributed consensus, automatic multi-controller HA, or partition-safe leadership from local locking and epochs.

## Failure evidence

Typed evidence is consumed with explicit categories (process exit, transport disconnect, heartbeat expiry, backend execution failure, readiness revocation, device/node unavailable, resource-claim revocation, dependency invalidation, administrative failover, planned maintenance, degraded health, unknown). Each event binds source and boot, affected target/domain, generation, observation/receipt timestamps, severity, confidence, provenance, and a stated observation mechanism. Evidence status is SUSPECTED, CONFIRMED, CLEARED, STALE, or UNKNOWN. A socket disconnect does not prove the worker stopped executing, and an OS process-exit observation does not prove a node or GPU failed. An older healthy observation never erases a newer confirmed failure.

## Domain eligibility

Failure domains are explicit and may nest or overlap arbitrarily. A target may share multiple correlated domains; unknown membership never silently satisfies an independence requirement. Hard eligibility rejects candidates that are the failed target, inside a failed domain, sharing a failure domain with the failed target, or whose independence is unknown — and reports the exact blocking domain.

## Candidate selection

Candidates bind target/replica/engine generation, worker boot, readiness profile, health, model/runtime compatibility, state availability, domains, resources, commitments, activation eligibility, and estimated costs. A candidate snapshot is coherent: readiness, capacity, state, and domains all come from the same observation generation. Hard exclusions run before ranking and return typed reasons (NO_ELIGIBLE_TARGET, STALE_READINESS, INCOMPATIBLE_PROFILE, FAILED_DOMAIN, DOMAIN_INDEPENDENCE_UNKNOWN, INSUFFICIENT_CAPACITY, COMMITMENT_CONFLICT, STATE_UNAVAILABLE, STATE_TOO_OLD, FENCING_UNPROVEN, RECOVERY_BUDGET_INFEASIBLE, and more). UNKNOWN never silently becomes eligible. Ranking uses named factors and deterministic tie-breaking; the cheaper candidate never overrides a hard continuity or fencing requirement.

## Continuity classes

Continuity is represented as STATELESS_RESTART, REPLAY_SAFE_REQUESTS, CHECKPOINT_RESTORE, SESSION_STATE_REBIND, SESSION_RESTART_REQUIRED, MANUAL_RECOVERY_REQUIRED, or UNKNOWN. Stateful continuation requires actual compatible state evidence: model/KV bytes existing somewhere do not prove correct session, tenant, model generation, format, completeness, integrity, or replay safety. The runtime never silently downgrades a session-continuity requirement to a stateless restart.

## Fencing enforcement

Fencing is mandatory before an exclusive replacement serves new authoritative work. The mandatory reference path funnels new request dispatch and accepted results through the current authority gate (coordinator assignment authority, reference gateway dispatch, worker admission, result acceptance). Workers get bounded per-request authorization bound to slot, assignment generation, coordinator epoch, target incarnation, request/attempt, route generation, and the permitted operation. A stale coordinator epoch, stale route generation, or fenced worker boot is rejected. If a mandated enforcement point cannot enforce fencing, the promotion returns FENCING_UNPROVEN and does not proceed.

## Transaction milestones

A failover transaction is a recoverable sequence: validate current service and source assignment → select and reserve an eligible candidate → obtain fresh preparation/readiness evidence → record failover intent → fence new admission to the old assignment → authorize the replacement under a new service authority generation → obtain activation acknowledgment → install the new route generation → obtain gateway acknowledgment → execute a verification request through the route → commit verified recovery → retire the old assignment and release unused commitments. Milestones are recorded so a restart can distinguish intent-only, old-admission-fenced, replacement-authorized, activation-acknowledged, route-installed, and verification-complete states. After old authority is fenced, authority is never restored by decrementing a generation; restoration requires a fresh evaluation and a new authorized generation. A competing attempt that would clear a newer attempt's authority is superseded rather than committed.

The core transaction, eligibility, selection, and cutover logic is validated end-to-end in-process by the test suite (see Tests).

## Gateway contract

The reference gateway consults the coordinator for per-dispatch and per-result authority, carries request/assignment/route identity, rejects stale route updates, acknowledges installed routes, exposes the current route generation, confirms results match the CPU reference (CPU parity), reports ambiguous requests, and fails closed when current authority cannot be established. It is a reference for correctness, not a production service mesh; its latency and availability tradeoff is documented by its per-request coordinator round-trips.

## Request ambiguity

Service cutover is separate from in-flight request disposition. Requests are classified (not dispatched, dispatched, accepted by target, confirmed complete, response committed, failed, outcome unknown). A connection loss after dispatch may produce OUTCOME_UNKNOWN; the runtime does not fabricate final success, does not auto-retry non-idempotent or externally effectful work without explicit authority, and never claims exactly-once execution from deduplicated responses. A late old-target result is rejected and classified as ambiguous (in-flight outcome unknown) or stale (already committed), independent of successful recovery.

## Recovery objectives

Recovery requirements carry an explicit RTO clock origin (first accepted failure observation, explicit failover request, or documented contract anchor) and a recovery-point requirement expressed in the actual available unit (committed sequence, checkpoint generation, or explicitly supported elapsed-time bound) — never translated into milliseconds without evidence. Recovery is complete only at a defined verified boundary (successful routed reference work on the new target). A missed recovery budget never forces unsafe promotion; it is reported, and the best policy-permitted alternative may be selected without being labeled SLO-compliant.

## Restart behavior

On coordinator restart the runtime establishes exclusive ownership, durably advances the epoch, recovers definitions and history, marks dynamic readiness/route/activation evidence as revalidation-required, rejects old-epoch traffic, and reconciles partially completed cutovers idempotently. A saved ACTIVE field or route is never replayed as fresh authority; the service remains explicitly unavailable/revalidation-required until fresh evidence is obtained. Exactly one permitted current assignment emerges, or the service stays unavailable.

## Real hardware and process proofs

The repository ships a real reference gateway and coordinator, real CPU reference workers, and a real CUDA reference worker (cuda/ directory) that performs genuine H2D transfers, bounded kernel launches, synchronization, and D2H copies on an RTX 5090 (sm_120). The CMake build produces a runnable CUDA worker (CUDA 13.x, sm_120). The multiprocess proof driver (tools/multiproc_proof.cpp) spawns the coordinator, gateway, and two workers as independent OS processes and, over loopback TCP, registers fresh boots, serves real client traffic through the gateway to the active worker with CPU parity, terminates the active worker as a real OS process, records process-loss evidence, fences the old worker, selects and activates the standby, installs and acknowledges a new route, issues a routed verification request to the standby, commits verified recovery with CPU parity, rejects stale old-worker registration/route/activation/result, proves a fresh replacement does not reclaim, and executes a real coordinator restart (durable epoch advance, revalidation-required, old-epoch rejection). The same scenario runs against two real CUDA workers (same-device process failover). Further real-process scenarios prove independent gateway restart with live route re-acquisition and routed verification, live old-worker fencing, in-flight ambiguity (OUTCOME_UNKNOWN, verified replacement retry, non-retryable automatic-replay refusal with no replacement attempt dispatched, stale-publication rejection, no exactly-once claim), failback / anti-flapping, interrupted-cutover recovery at each transaction milestone, and stateful checkpoint restore. The in-process failover proof additionally validates the exclusive-assignment invariant, request ambiguity classification, failback/anti-flap and recovery-point policy rejection semantics, and persistence/corruption handling.

The two reference workers on this reference deployment share one physical GPU when run with --cuda; that scenario proves same-device process failover, not device/node failure tolerance. Synthetic multi-host/rack/zone data validates policy logic, not physically measured multi-node redundancy.

## Installation

Requires CMake >= 3.24, a C++20 compiler, and (for the CUDA worker) the CUDA 13.x toolkit and an sm_120-capable device.

```
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build --prefix <install-prefix>
```

## Examples and CLI

Runnable examples are in examples/ (basic exclusive failover, failure-domain exclusions, no eligible candidate, warm vs cold replacement, recovery-budget violation, state continuity requirement, old-worker fencing, planned switchover, failback, interrupted-cutover recovery). A CLI (tools/ff_cli) inspects service assignments, current authority, failure evidence, candidate eligibility, exclusion reasons, domain overlap, selected alternatives, recovery estimates, transaction milestones, activations, route generations, verification, ambiguous requests, failback policy, and history, returning typed structured results plus deterministic readable output.

## Tests

The test suite (tests/) validates service/authority invariants, failure evidence ordering, domain eligibility, candidate selection, recovery objectives, transaction/compensation, route reconciliation, continuity/ambiguity, failback/anti-flapping (with an injectable clock), stateful recovery-point rejection semantics (wrong-session, corrupt, insufficiently-fresh), persistence/corruption, deterministic property tests over the model, genuine competing-thread concurrency, and adversarial hardening (stale generations, incompatible profiles, wrong-session checkpoints, fenced boots, revoked resources, stale readiness revalidation, fencing-unavailable, duplicate promotion, wrong-boot route ACK). All tests run without any configured test timeout.

## Benchmarks

benchmarks/ measures evidence publication, candidate filtering, ranking, plan creation, promotion bookkeeping, route reconciliation, verification commit, and persistence save/recovery at 100/1k/10k services, and reports integrated proof timings as measured, never single-sample p99.

## Build quality

Release and Debug builds use /W4 /WX on MSVC with zero first-party warnings. The core is backend-neutral and independently usable; CUDA sits behind an optional FF_BUILD_CUDA boundary. Persistence is versioned and integrity-checked and loads into validated temporary structures before replacing live state.

## Actual limitations

- The reference coordinator, gateway, and workers provide a real control/data plane over loopback TCP for correctness purposes, not production service-mesh performance or availability.
- This runtime does **not** implement distributed consensus, universal partition safety, exactly-once execution, zero downtime, multi-node redundancy, or state continuity beyond what is proven by the reference protocol.
- The multiprocess runner now validates, over real processes: the CPU and CUDA failover, stale-authority rejection and stale late-result rejection, fresh-replacement no-reclaim, live old-worker fencing (worker stays alive after its control connection is severed; failover proceeds through the governed fencing mechanism; old authority rejected; no reclaim), in-flight ambiguity (compute demonstrably happens but the response is withheld and the request is not reported as success; precise ambiguity classification is proven in-process), independent gateway restart (fresh GatewayBootId registers; stale old-boot acknowledgments are rejected; the promoted standby is verified live/active), and coordinator restart reconciliation (durable epoch advance, revalidation-required, old-epoch rejection).
- The multiprocess runner also validates, over real processes: independent gateway restart with a fresh GatewayBootId that actually re-acquires the current route and serves routed verification (a residual nested-mutex deadlock on the coordinator route-replay was repaired: on re-registration the registration lock is released before re-acquiring it for the worker-lookup, and the replayed route is re-bound to the fresh gateway boot); failback/anti-flapping (a recovered preferred target does not reclaim automatically, policy authorization + fresh readiness are required, a fresh assignment + route generation are issued through the authoritative transaction, routed verification succeeds after failback, old authority stays rejected, and a manual-authorization / cooldown / hysteresis / attempt-budget gate governs repeated changes); interrupted-cutover recovery (the coordinator is terminated and restarted at each material cutover boundary — intent recorded, old admission fenced, promotion authorized, activation acknowledged, route installed — with durable milestone checkpoints; after restart the epoch advances, stale old-epoch traffic is rejected, the interrupted cutover is never revived, the service is left explicitly revalidation-required, and only a fresh verified recovery restores service with at most one exclusive current assignment); stateful checkpoint restore (a real worker mutates a deterministic session, persists an integrity-checked checkpoint, advances beyond it, and on failure the replacement reopens + restores the checkpoint, verifies integrity/identity/compatibility/sequence, and the coordinator selects it under an explicit CHECKPOINT_RESTORE recovery-point policy while wrong-session / corrupt / insufficiently-fresh checkpoints are rejected); and end-to-end request ambiguity (a dispatched request whose target computes but withholds the response is recorded as an explicit OUTCOME_UNKNOWN, never fabricated as success, a non-retryable request refuses automatic replay (classified OUTCOME_UNKNOWN, no replacement attempt dispatched or executed), a replay-safe/new-attempt path returns a verified current response, a delayed stale publication cannot commit, and response commitment is not described as exactly-once physical execution).
- No telemetry transmission.

## License

Apache License 2.0. Copyright 2026 Summon Software Labs. No telemetry transmission.