// benchmark_main.cpp — measures candidate filtering, ranking, and plan creation across
// 100 / 1k / 10k services, plus evidence publication and selection. Times are wall-clock
// measurements of completed operations, not enqueue times.
#include <failover_fabric/failover_fabric.hpp>
#include <chrono>
#include <cstdio>
#include <vector>

using namespace failover_fabric;

static std::uint64_t now_ms() {
  return (std::uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

static void reg(FailoverFabric& fab, TargetId t, WorkerBootId b, ServiceId svc) {
  fab.register_target(t, "bench");
  CandidateFacts f; f.target = t; f.target_generation = TargetGeneration::first(); f.service = svc; f.replica = ReplicaId(t.value());
  f.engine = EngineId(t.value()); f.engine_incarnation = EngineIncarnationId(t.value()); f.worker = WorkerId(t.value());
  f.worker_boot = b; f.profile = ReadinessProfileId(1); f.model_key = "m"; f.runtime_abi = "a";
  f.ready = true; f.activation_eligible = true; f.evidence_fresh = true;
  fab.publish_candidate_facts(f); fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} });
}

int main(int argc, char** argv) {
  int services = argc > 1 ? (int)std::strtoull(argv[1], nullptr, 10) : 100;
  int candidates = argc > 2 ? (int)std::strtoull(argv[2], nullptr, 10) : 4;
  RuntimeConfig cfg;
  cfg.max_services = (std::size_t)(services + 16);
  cfg.max_targets = (std::size_t)((services + 16) * (candidates + 2));
  cfg.max_candidates_per_snapshot = (std::size_t)(candidates + 4);
  FailoverFabric fab(cfg);
  std::uint64_t t0 = now_ms();
  for (int i = 0; i < services; ++i) {
    ServiceId sid(i + 2);
    ServiceDefinition s; s.service = sid; s.generation = ServiceGeneration::first(); s.config_generation = ServiceConfigGeneration::first();
    s.name = "b"; s.role = ServingRole::EXCLUSIVE_ACTIVE; s.compatibility.model_key = "m"; s.compatibility.runtime_abi = "a";
    s.domain_requirement.require_domain_independence = false; s.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
    s.recovery.rto_ms = 10000; s.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS; s.recovery.requires_verified_recovery = true;
    s.policy_generation = PolicyGeneration::first(); fab.create_service(s);
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    Assignment a; a.id = AssignmentId(i + 1); a.generation = AssignmentGeneration::first(); a.slot = slot;
    a.target = TargetId(100 * (i + 1)); a.target_generation = TargetGeneration::first(); a.worker_boot = WorkerBootId(100 * (i + 1));
    a.authority_generation = ServiceAuthorityGeneration::first(); a.state = AssignmentState::ACTIVE; fab.set_initial_assignment(a);
    for (int c = 0; c < candidates; ++c) reg(fab, TargetId(100 * (i + 1) + c + 1), WorkerBootId(100 * (i + 1) + c + 1), sid);
  }
  std::uint64_t t1 = now_ms();
  std::uint64_t select_ms = 0, plan_ms = 0, ev_ms = 0;
  for (int i = 0; i < services; ++i) {
    ServiceSlotKey slot{ServiceId(i + 2), ServiceSlotId(1)};
    FailureEvent ev; ev.event_id = FailureEventId(i + 1); ev.generation = FailureGeneration::first(); ev.status = EvidenceStatus::CONFIRMED;
    ev.category = FailureCategory::PROCESS_EXIT; ev.target = TargetId(100 * (i + 1)); ev.source = SourceId(1); ev.received.seq = i + 1;
    std::uint64_t e0 = now_ms(); fab.publish_failure(ev); ev_ms += now_ms() - e0;
    std::uint64_t s0 = now_ms(); auto r = fab.select(slot); select_ms += now_ms() - s0;
    std::uint64_t p0 = now_ms(); auto plan = fab.plan_failover(slot); plan_ms += now_ms() - p0;
  }
  std::uint64_t t2 = now_ms();
  std::printf("services=%d candidates/svc=%d\n", services, candidates);
  std::printf("setup_ms=%llu  total_op_ms=%llu  evidence_pub_ms=%llu  select_ms=%llu  plan_ms=%llu\n",
    (unsigned long long)(t1 - t0), (unsigned long long)(t2 - t1), (unsigned long long)ev_ms,
    (unsigned long long)select_ms, (unsigned long long)plan_ms);
  return 0;
}