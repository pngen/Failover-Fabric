// test_concurrency.cpp — genuine competing threads over the core, with a start barrier.
// No arbitrary sleeps as the primary correctness proof: threads synchronize on an atomic
// start flag and the assertions check invariants after all threads join.
#include "framework.hpp"
#include "inprocess_ops.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

using namespace failover_fabric;

static ServiceDefinition svc(ServiceId sid) {
  ServiceDefinition s; s.service=sid; s.generation=ServiceGeneration::first();
  s.config_generation=ServiceConfigGeneration::first(); s.name="s"; s.role=ServingRole::EXCLUSIVE_ACTIVE;
  s.compatibility.model_key="ref-det-sequence-v1"; s.compatibility.runtime_abi="ff-ref-v1";
  s.domain_requirement.require_domain_independence=false;
  s.resources.push_back(ResourceRequirement{"cpu",1.0,true});
  s.recovery.rto_ms=10000; s.recovery.continuity=ContinuityClass::REPLAY_SAFE_REQUESTS;
  s.recovery.requires_verified_recovery=true; s.failback=FailbackPolicy::MANUAL; s.policy_generation=PolicyGeneration::first();
  return s;
}

static void reg(FailoverFabric& fab, TargetId t, WorkerBootId b) {
  fab.register_target(t,"p");
  CandidateFacts f; f.target=t; f.target_generation=TargetGeneration::first(); f.replica=ReplicaId(t.value());
  f.engine=EngineId(t.value()); f.engine_incarnation=EngineIncarnationId(t.value());
  f.worker=WorkerId(t.value()); f.worker_boot=b; f.profile=ReadinessProfileId(1);
  f.model_key="ref-det-sequence-v1"; f.runtime_abi="ff-ref-v1"; f.ready=true; f.activation_eligible=true; f.evidence_fresh=true;
  fab.publish_candidate_facts(f);
  fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
}

FF_TEST(concurrency_competing_attempts) {
  FailoverFabric fab;
  ServiceId sid(1);
  fab.create_service(svc(sid));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
  a.target=TargetId(10); a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(10);
  a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  reg(fab, TargetId(10), WorkerBootId(10));
  reg(fab, TargetId(11), WorkerBootId(11));
  FailureEvent ev; ev.event_id=FailureEventId(1); ev.generation=FailureGeneration::first();
  ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT; ev.target=TargetId(10);
  ev.source=SourceId(1); ev.received.seq=1; fab.publish_failure(ev);

  // Both planner threads select B and try to execute; only one may commit.
  std::atomic<bool> start{false};
  std::atomic<int> completed{0};
  std::atomic<int> failed{0};
  auto attempt = [&]() {
    while (!start.load()) { std::this_thread::yield(); }
    auto plan = fab.plan_failover(slot);
    if (plan) {
      ff_test::InProcessOps ops("p");
      AttemptResult r = fab.execute_plan(*plan, ops);
      if (r.state == AttemptState::COMPLETED) ++completed; else ++failed;
    }
  };
  auto reader = [&]() {
    while (!start.load()) { std::this_thread::yield(); }
    for (int i = 0; i < 500; ++i) { (void)fab.select(slot); (void)fab.current_assignment(slot); }
  };
  auto churn = [&]() {
    while (!start.load()) { std::this_thread::yield(); }
    FailureEvent healthy; healthy.event_id=FailureEventId(2); healthy.generation=FailureGeneration::first();
    healthy.status=EvidenceStatus::CLEARED; healthy.target=TargetId(10); healthy.source=SourceId(1); healthy.received.seq=2;
    fab.publish_failure(healthy);
  };

  std::vector<std::thread> th;
  th.emplace_back(attempt); th.emplace_back(attempt);
  th.emplace_back(reader); th.emplace_back(churn);
  start.store(true);
  for (auto& t : th) t.join();

  // Exactly one competing attempt may commit as COMPLETED.
  FF_CHECK_EQ(completed.load(), 1);
  FF_CHECK_EQ(failed.load(), 1);
  // Exactly one current assignment, for exactly one slot.
  auto cur = fab.current_assignment(slot);
  FF_CHECK(cur.has_value());
  FF_CHECK_EQ(cur->target, TargetId(11));
  FF_CHECK_EQ(cur->state, AssignmentState::ACTIVE);
}

FF_TEST(concurrency_mixed_reads_and_mutations) {
  FailoverFabric fab;
  ServiceId sid(2);
  fab.create_service(svc(sid));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
  a.target=TargetId(20); a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(20);
  a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  reg(fab, TargetId(20), WorkerBootId(20)); reg(fab, TargetId(21), WorkerBootId(21));

  std::atomic<bool> start{false};
  auto pub = [&](bool is_fail) {
    while (!start.load()) { std::this_thread::yield(); }
    for (int i = 0; i < 200; ++i) {
      FailureEvent ev; ev.event_id=FailureEventId(50+i); ev.generation=FailureGeneration::first();
      ev.status=is_fail ? EvidenceStatus::CONFIRMED : EvidenceStatus::CLEARED;
      ev.category=is_fail ? FailureCategory::PROCESS_EXIT : FailureCategory::DEGRADED_HEALTH;
      ev.target=TargetId(20); ev.source=SourceId(1); ev.received.seq=100+i;
      fab.publish_failure(ev);
    }
  };
  std::vector<std::thread> th;
  th.emplace_back(pub, true); th.emplace_back(pub, false);
  th.emplace_back([&](){ while(!start.load()){std::this_thread::yield();} for(int i=0;i<200;++i) (void)fab.select(slot); });
  start.store(true);
  for (auto& t : th) t.join();
  // Exercise the state is coherent (no crash, one assignment).
  FF_CHECK(fab.current_assignment(slot).has_value());
}

int main() { return ff_test::run_all(); }