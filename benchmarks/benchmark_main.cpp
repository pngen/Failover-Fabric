// benchmark_main.cpp - measures the failover runtime at controlled scales.
#include <failover_fabric/failover_fabric.hpp>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <vector>
using namespace failover_fabric;
static std::uint64_t now_ms() { return (std::uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static double now_us() { return (double)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count() / 1000.0; }
static void reg(FailoverFabric& fab, TargetId t, WorkerBootId b, ServiceId svc) {
  fab.register_target(t, "bench");
  CandidateFacts f; f.target = t; f.target_generation = TargetGeneration::first(); f.service = svc;
  f.replica = ReplicaId(t.value()); f.engine = EngineId(t.value()); f.engine_incarnation = EngineIncarnationId(t.value());
  f.worker = WorkerId(t.value()); f.worker_boot = b; f.profile = ReadinessProfileId(1); f.model_key = "m"; f.runtime_abi = "a";
  f.ready = true; f.activation_eligible = true; f.evidence_fresh = true;
  fab.publish_candidate_facts(f); fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} });
}
int main(int argc, char** argv) {
  int services = argc > 1 ? (int)std::strtoull(argv[1], nullptr, 10) : 100;
  int candidates = argc > 2 ? (int)std::strtoull(argv[2], nullptr, 10) : 4;
  int reps = argc > 3 ? (int)std::strtoull(argv[3], nullptr, 10) : 3;
  RuntimeConfig cfg; cfg.max_services = (std::size_t)(services + 16);
  cfg.max_targets = (std::size_t)((services + 16) * (candidates + 2));
  cfg.max_candidates_per_snapshot = (std::size_t)(candidates + 4);
  FailoverFabric fab(cfg);
  std::uint64_t t0 = now_ms();
  for (int i = 0; i < services; ++i) {
    ServiceId sid(i + 2);
    ServiceDefinition s; s.service = sid; s.generation = ServiceGeneration::first();
    s.config_generation = ServiceConfigGeneration::first(); s.name = "b"; s.role = ServingRole::EXCLUSIVE_ACTIVE;
    s.compatibility.model_key = "m"; s.compatibility.runtime_abi = "a";
    s.domain_requirement.require_domain_independence = false; s.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
    s.recovery.rto_ms = 10000; s.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
    s.recovery.requires_verified_recovery = true; s.policy_generation = PolicyGeneration::first();
    fab.create_service(s);
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    Assignment a; a.id = AssignmentId(i + 1); a.generation = AssignmentGeneration::first(); a.slot = slot;
    a.target = TargetId(100 * (i + 1)); a.target_generation = TargetGeneration::first();
    a.worker_boot = WorkerBootId(100 * (i + 1)); a.authority_generation = ServiceAuthorityGeneration::first();
    a.state = AssignmentState::ACTIVE; fab.set_initial_assignment(a);
    for (int c = 0; c < candidates; ++c) reg(fab, TargetId(100 * (i + 1) + c + 1), WorkerBootId(100 * (i + 1) + c + 1), sid);
  }
  std::uint64_t t1 = now_ms();
  double pub_us = 0, sel_us = 0, plan_us = 0; std::uint64_t ops = 0;
  for (int i = 0; i < services; ++i) {
    ServiceSlotKey slot{ServiceId(i + 2), ServiceSlotId(1)};
    for (int r = 0; r < reps; ++r) {
      FailureEvent ev; ev.event_id = FailureEventId(i * reps + r + 1); ev.generation = FailureGeneration::first();
      ev.status = EvidenceStatus::CONFIRMED; ev.category = FailureCategory::PROCESS_EXIT;
      ev.target = TargetId(100 * (i + 1)); ev.source = SourceId(1); ev.received.seq = i * reps + r + 1;
      double p0 = now_us(); fab.publish_failure(ev); pub_us += (now_us() - p0);
      double s0 = now_us(); (void)fab.select(slot); sel_us += (now_us() - s0);
      double q0 = now_us(); (void)fab.plan_failover(slot); plan_us += (now_us() - q0);
      ++ops;
    }
  }
  std::uint64_t t2 = now_ms();
  std::uint64_t total_candidates = (std::uint64_t)services * (candidates + 1);
  double per_op = ops ? (pub_us + sel_us + plan_us) / (double)ops : 0.0;
  std::printf("services=%d candidates_per_service=%d reps=%d total_candidates=%llu operations=%llu\n", services, candidates, reps, (unsigned long long)total_candidates, (unsigned long long)ops);
  std::printf("setup_ms=%llu completion_ms=%llu pub_ms=%llu select_ms=%llu plan_ms=%llu\n", (unsigned long long)(t1 - t0), (unsigned long long)(t2 - t1), (unsigned long long)pub_us, (unsigned long long)sel_us, (unsigned long long)plan_us);
  std::printf("time_per_completed_op_us=%.1f\n", per_op);
  return 0;
}