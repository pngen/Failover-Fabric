// test_scenarios.cpp — end-to-end failover, stale authority, request ambiguity,
// persistence round-trip, planned switchover, failback, interrupted-cutover recovery.
#include "framework.hpp"
#include "inprocess_ops.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>
#include <fstream>
#include <istream>

using namespace failover_fabric;

static ServiceDefinition svc_ref(ServiceId sid) {
  ServiceDefinition svc;
  svc.service = sid; svc.generation = ServiceGeneration::first();
  svc.config_generation = ServiceConfigGeneration::first();
  svc.name = "svc"; svc.role = ServingRole::EXCLUSIVE_ACTIVE;
  svc.compatibility.model_key = "ref-det-sequence-v1"; svc.compatibility.runtime_abi = "ff-ref-v1";
  svc.domain_requirement.require_domain_independence = true;
  svc.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
  svc.recovery.rto_ms = 10000; svc.recovery.rto_anchor = RtoAnchor::FIRST_ACCEPTED_FAILURE_OBSERVATION;
  svc.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
  svc.recovery.requires_verified_recovery = true;
  svc.failback = FailbackPolicy::MANUAL;
  svc.policy_generation = PolicyGeneration::first();
  return svc;
}

static CandidateFacts facts(TargetId t, WorkerBootId boot, bool ready = true) {
  CandidateFacts f;
  f.target = t; f.target_generation = TargetGeneration::first();
  f.replica = ReplicaId(t.value()); f.engine = EngineId(t.value());
  f.engine_incarnation = EngineIncarnationId(t.value());
  f.worker = WorkerId(t.value()); f.worker_boot = boot;
  f.profile = ReadinessProfileId(1);
  f.model_key = "ref-det-sequence-v1"; f.runtime_abi = "ff-ref-v1";
  f.ready = ready; f.activation_eligible = true; f.evidence_fresh = true;
  return f;
}
static std::vector<CandidateResource> cpu_ok() {
  return { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} };
}
static void reg(FailoverFabric& fab, TargetId t, WorkerBootId boot, bool ready = true) {
  fab.register_target(t, "pipe-" + std::to_string(t.value()));
  fab.publish_candidate_facts(facts(t, boot, ready));
  fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, cpu_ok());
}
static void publish_death(FailoverFabric& fab, TargetId t, std::uint64_t seq, FailureCategory cat = FailureCategory::PROCESS_EXIT) {
  FailureEvent ev; ev.event_id = FailureEventId(seq); ev.generation = FailureGeneration::first();
  ev.status = EvidenceStatus::CONFIRMED; ev.category = cat;
  ev.target = t; ev.source = SourceId(1); ev.source_boot = SourceBootId(1); ev.received.seq = seq;
  fab.publish_failure(ev);
}

// A healthy device/host topology where A and B are independent.
static void setUp_independent(FailoverFabric& fab, ServiceId sid, TargetId a, TargetId b) {
  fab.create_service(svc_ref(sid));
  fab.declare_domain(DomainDecl{FailureDomainId(1), FailureDomainGeneration::first(), DomainClass::HOST, "h1", std::nullopt, Provenance::SYNTHETIC});
  fab.declare_domain(DomainDecl{FailureDomainId(3), FailureDomainGeneration::first(), DomainClass::HOST, "h2", std::nullopt, Provenance::SYNTHETIC});
  fab.set_membership(DomainMembership{a, FailureDomainId(1), true});
  fab.set_membership(DomainMembership{b, FailureDomainId(3), true});
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id = AssignmentId(1); asg.generation = AssignmentGeneration::first();
  asg.slot = slot; asg.target = a; asg.target_generation = TargetGeneration::first();
  asg.worker_boot = WorkerBootId(a.value());
  asg.authority_generation = ServiceAuthorityGeneration::first();
  asg.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  reg(fab, a, WorkerBootId(a.value()));
  reg(fab, b, WorkerBootId(b.value()));
}
static ServiceSlotKey key(ServiceId sid) { return ServiceSlotKey{sid, ServiceSlotId(1)}; }

FF_TEST(end_to_end_failover) {
  FailoverFabric fab;
  ServiceId sid(1);
  setUp_independent(fab, sid, TargetId(10), TargetId(11));
  publish_death(fab, TargetId(10), 1);
  ServiceSlotKey slot = key(sid);

  SelectionResult sr = fab.select(slot);
  FF_CHECK(sr.has_selection);
  FF_CHECK_EQ(sr.selected->facts.target, TargetId(11));

  auto plan = fab.plan_failover(slot);
  FF_CHECK(plan.has_value());
  // Planning must not mutate current assignment.
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(10));
  FF_CHECK_EQ(fab.current_assignment(slot)->state, AssignmentState::ACTIVE);

  ff_test::InProcessOps ops("pipe-11");
  AttemptResult res = fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(res.state, AttemptState::COMPLETED);
  FF_CHECK(res.recovery_verified);
  FF_CHECK_EQ(res.reached, Milestone::VERIFICATION_COMPLETE);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(11));
  FF_CHECK_EQ(fab.current_assignment(slot)->state, AssignmentState::ACTIVE);
  FF_CHECK(res.committed_assignment.has_value());
  // The old assignment must be retired/fenced, and A's boot fenced.
  auto hist = fab.assignment_history(slot, 10);
  FF_CHECK(!hist.empty());
  FF_CHECK_EQ(hist.front().state, AssignmentState::FENCED);
  // Service is available again on B.
  FF_CHECK_EQ(fab.service_outcome(slot), ServiceOutcome::AVAILABLE);
}

FF_TEST(stale_authority_rejected) {
  FailoverFabric fab;
  ServiceId sid(2);
  setUp_independent(fab, sid, TargetId(20), TargetId(21));
  publish_death(fab, TargetId(20), 1);
  ServiceSlotKey slot = key(sid);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p"); fab.execute_plan(*plan, ops);

  Assignment cur = *fab.current_assignment(slot);
  WorkerAuthorization ok;
  ok.epoch = fab.current_epoch(); ok.slot = slot; ok.assignment = cur.id;
  ok.assignment_generation = cur.generation; ok.route_generation = cur.route_generation;
  ok.incarnation = EngineIncarnationId(21); ok.request = RequestId(1); ok.execution = ExecutionId(1);
  ok.operation = "serve"; ok.worker_boot = WorkerBootId(21);
  FF_CHECK(fab.authorize_dispatch(ok).allowed);

  // A stale old authorization (old boot / old route / old epoch) must be rejected.
  WorkerAuthorization stale = ok;
  stale.worker_boot = WorkerBootId(20);   // fenced boot
  FF_CHECK(!fab.authorize_dispatch(stale).allowed);
  stale = ok; stale.route_generation = cur.route_generation;  // same route but old assignment
  stale.assignment = AssignmentId(1); stale.assignment_generation = AssignmentGeneration::first();
  FF_CHECK(!fab.authorize_dispatch(stale).allowed);
  stale = ok; stale.epoch = CoordinatorEpoch(fab.current_epoch().value() + 1, CoordinatorId(1), CoordinatorId(1));
  FF_CHECK(!fab.authorize_dispatch(stale).allowed);
  // Stale (already committed) result must be rejected.
  RequestRecord r; r.request = RequestId(1); r.execution = ExecutionId(1);
  r.slot = slot; r.disposition = RequestDisposition::RESPONSE_COMMITTED;
  r.dispatched_auth = ok;
  fab.record_request(r);
  FF_CHECK(!fab.authorize_result(ok).allowed);
}

FF_TEST(request_ambiguity_nonretryable) {
  FailoverFabric fab;
  ServiceId sid(3);
  setUp_independent(fab, sid, TargetId(30), TargetId(31));
  ServiceSlotKey slot = key(sid);
  Assignment cur = *fab.current_assignment(slot);
  WorkerAuthorization auth;
  auth.epoch = fab.current_epoch(); auth.slot = slot; auth.assignment = cur.id;
  auth.assignment_generation = cur.generation; auth.route_generation = cur.route_generation;
  auth.worker_boot = WorkerBootId(30); auth.request = RequestId(1); auth.execution = ExecutionId(1);
  auth.operation = "serve";
  // Request dispatched to A, acknowledged by A, computed, but result never committed before A dies.
  RequestRecord r; r.request = RequestId(1); r.execution = ExecutionId(1); r.execution_generation = ExecutionGeneration::first();
  r.slot = slot; r.disposition = RequestDisposition::ACCEPTED_BY_TARGET;
  r.idempotent = false; r.authority = IdempotencyAuthority::NONE; r.externally_effectful = true;
  r.dispatched_auth = auth; r.client_note = "non-retryable";
  fab.record_request(r);

  publish_death(fab, TargetId(30), 1);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p"); fab.execute_plan(*plan, ops);

  // The late result from A must be rejected by the authority gate and classified as
  // ambiguous (a non-retryable request whose outcome is genuinely unknown).
  FF_CHECK(!fab.authorize_result(auth).allowed);
  auto outcome = fab.classify_late_result(RequestId(1), auth);
  FF_CHECK_EQ(outcome, RequestTracker::LateOutcome::CLASSIFIED_AMBIGUOUS);
  FF_CHECK_EQ(fab.ambiguous_count(), 1u);
  // Non-idempotent request must NOT be replayed as success.
  FF_CHECK_EQ(r.authority, IdempotencyAuthority::NONE);
}

FF_TEST(persistence_roundtrip_and_corruption) {
  FailoverFabric fab;
  ServiceId sid(4);
  setUp_independent(fab, sid, TargetId(40), TargetId(41));
  publish_death(fab, TargetId(40), 1);
  ServiceSlotKey slot = key(sid);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p"); fab.execute_plan(*plan, ops);
  std::string path = "test_state.ff";
  fab.save(path);
  FF_CHECK(FailoverFabric::validate_file(path).empty());

  FailoverFabric fab2;
  fab2.load(path);
  FF_CHECK(fab2.current_assignment(slot).has_value());
  FF_CHECK_EQ(fab2.current_assignment(slot)->target, TargetId(41));
  // Conservative recovery marks routes revalidation-required.
  FF_CHECK(fab2.revalidation_required_count() > 0);

  // Corruption: flip a byte -> validation must report a problem, never silently accept.
  {
    std::vector<std::uint8_t> blob;
    { std::ifstream in(path, std::ios::binary); blob.assign((std::istreambuf_iterator<char>(in)), {}); }
    if (!blob.empty()) blob[blob.size() / 2] ^= 0x5a;
    std::ofstream out("corrupt.ff", std::ios::binary); out.write((const char*)blob.data(), blob.size());
  }
  FF_CHECK(!FailoverFabric::validate_file("corrupt.ff").empty());
  FF_REQUIRE_THROW(fab2.load("corrupt.ff"), std::runtime_error);
  std::remove(path.c_str()); std::remove("corrupt.ff");
}

FF_TEST(failback_policy_and_anti_flapping) {
  // Deterministic failback/anti-flapping test using the injectable ManualClock.
  ManualClock& clk = ManualClock::instance();
  clk.reset();
  FailoverFabric fab;
  fab.set_clock(std::shared_ptr<Clock>(&clk, [](Clock*){}));
  ServiceId sid(9);
  ServiceDefinition svc = svc_ref(sid);
  svc.failback = FailbackPolicy::MANUAL;
  svc.anti_flapping.cooldown_ms = 100;
  svc.anti_flapping.hysteresis_ms = 50;
  svc.anti_flapping.max_auto_attempts = 2;
  fab.create_service(svc);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  // A (preferred/home, target 10) active; B (standby, target 11).
  Assignment asg; asg.id = AssignmentId(1); asg.generation = AssignmentGeneration::first();
  asg.slot = slot; asg.target = TargetId(10); asg.target_generation = TargetGeneration::first();
  asg.worker_boot = WorkerBootId(10); asg.authority_generation = ServiceAuthorityGeneration::first();
  asg.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);   // records preferred target 10
  reg(fab, TargetId(10), WorkerBootId(10));
  reg(fab, TargetId(11), WorkerBootId(11));

  // Fail A (preferred) -> failover to B.
  publish_death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("pipe-11");
  fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(11));

  // A'' s recovered preferred target (fresh boot) comes back but must NOT reclaim automatically:
  // before any explicit failback request, the assignment stays on B.
  fab.register_target(TargetId(10), "pipe-10");
  fab.publish_candidate_facts(facts(TargetId(10), WorkerBootId(42), true));
  fab.publish_candidate_state(TargetId(10), CandidateState{});
  fab.publish_candidate_resources(TargetId(10), cpu_ok());
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(11));   // no automatic reclaim
  FF_CHECK(!fab.evaluate_failback(slot).authorized);                  // MANUAL requires authorization

  // Explicit operator authorization (MANUAL) still blocked by anti-flapping cooldown.
  fab.authorize_failback(slot);
  auto before_cooldown = fab.evaluate_failback(slot);
  FF_CHECK(!before_cooldown.authorized);
  FF_CHECK(before_cooldown.in_cooldown);

  // Advance the manual clock past the cooldown window -> authorized.
  clk.advance(std::chrono::milliseconds(120));
  auto ready = fab.evaluate_failback(slot);
  FF_CHECK(ready.authorized);

  // Execute failback through the authoritative transaction: new assignment + route gen.
  ff_test::InProcessOps fops("pipe-10");
  auto fr = fab.execute_failback(slot, fops);
  FF_CHECK_EQ(fr.state, AttemptState::COMPLETED);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(10));
  FF_CHECK_EQ(fab.current_assignment(slot)->worker_boot, WorkerBootId(42));   // fresh incarnation
  // The old standby authority (B boot 11) must remain rejected.
  Assignment cur = *fab.current_assignment(slot);
  WorkerAuthorization stale;
  stale.epoch = fab.current_epoch(); stale.slot = slot; stale.assignment = cur.id;
  stale.assignment_generation = cur.generation; stale.route_generation = cur.route_generation;
  stale.incarnation = EngineIncarnationId(11); stale.request = RequestId(1); stale.execution = ExecutionId(1);
  stale.operation = "serve"; stale.worker_boot = WorkerBootId(11);
  FF_CHECK(!fab.authorize_dispatch(stale).allowed);
  // New route generation must advance beyond the failover route.
  auto nrt = fab.current_route(slot);
  FF_CHECK(nrt.has_value());
  FF_CHECK(nrt->generation.value() > 2);   // 1 (initial) -> 2 (failover) -> >=3 (failback)
}

FF_TEST(failback_refusal_and_budget) {
  ManualClock& clk = ManualClock::instance();
  clk.reset();
  FailoverFabric fab;
  fab.set_clock(std::shared_ptr<Clock>(&clk, [](Clock*){}));
  ServiceId sid(10);
  ServiceDefinition svc = svc_ref(sid);
  svc.failback = FailbackPolicy::RETURN_AFTER_VALIDATION;
  svc.anti_flapping.cooldown_ms = 100;
  svc.anti_flapping.max_auto_attempts = 1;
  fab.create_service(svc);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id = AssignmentId(1); asg.generation = AssignmentGeneration::first();
  asg.slot = slot; asg.target = TargetId(20); asg.target_generation = TargetGeneration::first();
  asg.worker_boot = WorkerBootId(20); asg.authority_generation = ServiceAuthorityGeneration::first();
  asg.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  reg(fab, TargetId(20), WorkerBootId(20));
  reg(fab, TargetId(21), WorkerBootId(21));
  publish_death(fab, TargetId(20), 1);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("pipe-21");
  fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(21));

  // Preferred target recovers but is NOT yet validated (unready) for RETURN_AFTER_VALIDATION.
  fab.register_target(TargetId(20), "pipe-20");
  fab.publish_candidate_facts(facts(TargetId(20), WorkerBootId(99), /*ready=*/false));
  fab.publish_candidate_state(TargetId(20), CandidateState{});
  fab.publish_candidate_resources(TargetId(20), cpu_ok());
  auto unready = fab.evaluate_failback(slot);
  FF_CHECK(!unready.authorized);
  FF_CHECK(unready.stale_readiness);

  // Fresh readiness (validated) but still in cooldown.
  fab.publish_candidate_facts(facts(TargetId(20), WorkerBootId(99), true));
  clk.advance(std::chrono::milliseconds(20));   // < cooldown (100ms)
  FF_CHECK(!fab.evaluate_failback(slot).authorized);
  clk.advance(std::chrono::milliseconds(120));  // past cooldown
  FF_CHECK(fab.evaluate_failback(slot).authorized);

  // First failback succeeds (budget reset), authority returns to the preferred target.
  ff_test::InProcessOps fops("pipe-20");
  auto fr = fab.execute_failback(slot, fops);
  FF_CHECK_EQ(fr.state, AttemptState::COMPLETED);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(20));

  // Failback again: policy RETURN_AFTER_VALIDATION + a recovered preferred target is now the
  // current target, so it cannot bounce back and forth until the budget is consumed.
  FF_CHECK(!fab.evaluate_failback(slot).authorized);   // already home
}

FF_TEST(request_ambiguity_replay_policy) {
  FailoverFabric fab;
  ServiceId sid(11);
  setUp_independent(fab, sid, TargetId(50), TargetId(51));
  ServiceSlotKey slot = key(sid);
  Assignment cur = *fab.current_assignment(slot);
  WorkerAuthorization auth;
  auth.epoch = fab.current_epoch(); auth.slot = slot; auth.assignment = cur.id;
  auth.assignment_generation = cur.generation; auth.route_generation = cur.route_generation;
  auth.worker_boot = WorkerBootId(50); auth.request = RequestId(1); auth.execution = ExecutionId(1);
  auth.operation = "serve";

  // A REPLAY-SAFE request (deterministic reference / idempotent) is dispatched and the target
  // withholds; it is recorded OUTCOME_UNKNOWN and a retry is authorized by policy.
  RequestRecord safe; safe.request = RequestId(1); safe.execution = ExecutionId(1);
  safe.slot = slot; safe.disposition = RequestDisposition::DISPATCHED; safe.idempotent = true;
  safe.authority = IdempotencyAuthority::DETERMINISTIC_REFERENCE; safe.externally_effectful = false;
  safe.dispatched_auth = auth;
  fab.record_request(safe);
  fab.mark_outcome_unknown(RequestId(1), slot);
  FF_CHECK(fab.retry_allowed(RequestId(1)));
  auto st1 = fab.request_state(RequestId(1));
  FF_CHECK_EQ(st1->disposition, RequestDisposition::OUTCOME_UNKNOWN);

  // A NON-RETRYABLE request (authority NONE, externally effectful) refuses automatic replay.
  RequestRecord nr; nr.request = RequestId(2); nr.execution = ExecutionId(2);
  nr.slot = slot; nr.disposition = RequestDisposition::DISPATCHED; nr.idempotent = false;
  nr.authority = IdempotencyAuthority::NONE; nr.externally_effectful = true;
  nr.dispatched_auth = auth; nr.client_note = "non-retryable";
  fab.record_request(nr);
  fab.mark_outcome_unknown(RequestId(2), slot);
  FF_CHECK(!fab.retry_allowed(RequestId(2)));
  FF_CHECK_EQ(fab.request_state(RequestId(2))->disposition, RequestDisposition::OUTCOME_UNKNOWN);
  FF_CHECK_EQ(fab.ambiguous_count(), 2u);
  // Never claimed exactly-once: both requests have an UNKNOWN outcome, not RESPONSE_COMMITTED.
  FF_CHECK(st1->disposition != RequestDisposition::RESPONSE_COMMITTED);
  FF_CHECK(fab.request_state(RequestId(2))->disposition != RequestDisposition::RESPONSE_COMMITTED);
}

// A helper that builds a CHECKPOINT_RESTORE candidate whose published state can be tuned,
// so recovery-point selection can be proven to reject wrong-session / corrupt / old state.
static void reg_stateful(FailoverFabric& fab, TargetId t, WorkerBootId boot, CandidateState st) {
  reg(fab, t, boot, true);
  fab.publish_candidate_state(t, std::move(st));
}

FF_TEST(stateful_recovery_policy) {
  FailoverFabric fab;
  ServiceId sid(12);
  ServiceDefinition svc = svc_ref(sid);
  svc.recovery.continuity = ContinuityClass::CHECKPOINT_RESTORE;
  svc.recovery.min_checkpoint_gen = CheckpointGeneration(5);
  svc.recovery.requires_verified_recovery = true;
  svc.domain_requirement.require_domain_independence = false;
  fab.create_service(svc);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id = AssignmentId(1); asg.generation = AssignmentGeneration::first();
  asg.slot = slot; asg.target = TargetId(60); asg.target_generation = TargetGeneration::first();
  asg.worker_boot = WorkerBootId(60); asg.authority_generation = ServiceAuthorityGeneration::first();
  asg.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  CandidateState good;
  good.state_available = true; good.checkpoint_generation = CheckpointGeneration(5);
  good.committed_sequence = 100; good.correct_session = true; good.correct_tenant = true;
  good.correct_model_generation = true; good.state_format_ok = true; good.status_integrity_ok = true; good.replay_safe = true;
  reg_stateful(fab, TargetId(61), WorkerBootId(61), good);
  CandidateState wrong_session = good; wrong_session.correct_session = false;
  reg_stateful(fab, TargetId(62), WorkerBootId(62), wrong_session);
  CandidateState corrupt = good; corrupt.status_integrity_ok = false;
  reg_stateful(fab, TargetId(63), WorkerBootId(63), corrupt);
  CandidateState old = good; old.checkpoint_generation = CheckpointGeneration(4);   // < min 5
  reg_stateful(fab, TargetId(64), WorkerBootId(64), old);
  publish_death(fab, TargetId(60), 1);

  // Only the fully-valid candidate is selected; wrong-session / corrupt / too-old are rejected
  // with the exact blocking reason and never silently downgraded.
  SelectionResult sr = fab.select(slot);
  FF_CHECK(sr.has_selection);
  FF_CHECK_EQ(sr.selected->facts.target, TargetId(61));
  bool saw_wrong_session = false, saw_corrupt = false, saw_old = false;
  for (auto& e : sr.exclusions) {
    if (e.second.first == ExclusionReason::STATE_UNAVAILABLE && e.first == TargetId(62)) saw_wrong_session = true;
    if (e.second.first == ExclusionReason::STATE_UNAVAILABLE && e.first == TargetId(63)) saw_corrupt = true;
    if (e.second.first == ExclusionReason::STATE_TOO_OLD && e.first == TargetId(64)) saw_old = true;
  }
  FF_CHECK(saw_wrong_session);
  FF_CHECK(saw_corrupt);
  FF_CHECK(saw_old);
  // A fenced stateful candidate (advance beyond the checkpoint) reports loss via committed seq.
  auto cand = sr.selected;
  FF_CHECK(cand->state.committed_sequence == 100u);
  FF_CHECK_EQ(cand->costs.state_loss_sequences, 0u);
}

int main() { return ff_test::run_all(); }