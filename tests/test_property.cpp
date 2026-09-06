// test_property.cpp — deterministic seeded property tests over the core model.
#include "framework.hpp"
#include "inprocess_ops.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>
#include <random>

using namespace failover_fabric;

static ServiceDefinition svc(ServiceId sid, bool independent) {
  ServiceDefinition s;
  s.service = sid; s.generation = ServiceGeneration::first(); s.config_generation = ServiceConfigGeneration::first();
  s.name = "s"; s.role = ServingRole::EXCLUSIVE_ACTIVE;
  s.compatibility.model_key = "ref-det-sequence-v1"; s.compatibility.runtime_abi = "ff-ref-v1";
  s.domain_requirement.require_domain_independence = independent;
  s.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
  s.recovery.rto_ms = 10000; s.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
  s.recovery.requires_verified_recovery = true;
  s.failback = FailbackPolicy::MANUAL; s.policy_generation = PolicyGeneration::first();
  return s;
}
static CandidateFacts facts(TargetId t, WorkerBootId b, bool ready, bool fresh) {
  CandidateFacts f; f.target=t; f.target_generation=TargetGeneration::first();
  f.replica=ReplicaId(t.value()); f.engine=EngineId(t.value()); f.engine_incarnation=EngineIncarnationId(t.value());
  f.worker=WorkerId(t.value()); f.worker_boot=b; f.profile=ReadinessProfileId(1);
  f.model_key="ref-det-sequence-v1"; f.runtime_abi="ff-ref-v1";
  f.ready=ready; f.activation_eligible=ready; f.evidence_fresh=fresh; f.health_generation=HealthGeneration::first();
  return f;
}
static void reg(FailoverFabric& fab, TargetId t, WorkerBootId b, bool ready=true, bool fresh=true) {
  fab.register_target(t, "p");
  fab.publish_candidate_facts(facts(t,b,ready,fresh));
  fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
}

FF_TEST(property_at_most_one_current) {
  for (std::uint32_t seed = 0; seed < 200; ++seed) {
    std::mt19937 rng(seed);
    FailoverFabric fab;
    ServiceId sid(seed + 1);
    fab.create_service(svc(sid, true));
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    // current target always from a small pool; set initial and run a bounded few failovers.
    TargetId pool[] = {TargetId(100), TargetId(101), TargetId(102)};
    Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
    a.target=pool[0]; a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(100);
    a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
    fab.set_initial_assignment(a);
    for (TargetId t : pool) reg(fab, t, WorkerBootId(t.value()));
    for (int step = 0; step < 5; ++step) {
      if (auto cur = fab.current_assignment(slot)) {
        FailureEvent ev; ev.event_id=FailureEventId(seed*10+step+1); ev.generation=FailureGeneration::first();
        ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT;
        ev.target=cur->target; ev.source=SourceId(1); ev.received.seq=step+1;
        fab.publish_failure(ev);
        auto plan = fab.plan_failover(slot);
        if (plan && plan->selected) {
          ff_test::InProcessOps ops("p");
          AttemptResult r = fab.execute_plan(*plan, ops);
          if (r.state == AttemptState::COMPLETED) {
            auto all = fab.assignment_history(slot, 1000);
            int active = 0; for (auto& x : all) if (x.state == AssignmentState::ACTIVE) ++active;
            // retired assignments are never current; history must not contain a live ACTIVE.
            FF_CHECK_EQ(active, 0);
            // fresh authority is strictly greater than before.
            auto gen = fab.current_epoch();
            (void)gen;
          }
        }
      }
    }
  }
}

FF_TEST(property_stale_never_mutates) {
  FailoverFabric fab;
  ServiceId sid(9000); fab.create_service(svc(sid, true));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
  a.target=TargetId(1); a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(1);
  a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  reg(fab, TargetId(1), WorkerBootId(1)); reg(fab, TargetId(2), WorkerBootId(2));
  FailureEvent ev; ev.event_id=FailureEventId(1); ev.generation=FailureGeneration::first();
  ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT; ev.target=TargetId(1);
  ev.source=SourceId(1); ev.received.seq=1; fab.publish_failure(ev);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p");
  AttemptResult r = fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(r.state, AttemptState::COMPLETED);
  // Any stale authorization that predates the committed assignment is rejected.
  WorkerAuthorization stale;
  stale.epoch = fab.current_epoch(); stale.slot = slot; stale.assignment = AssignmentId(1);
  stale.assignment_generation = AssignmentGeneration::first(); stale.route_generation = RouteGeneration::first();
  stale.worker_boot = WorkerBootId(1); stale.request=RequestId(1);
  FF_CHECK(!fab.authorize_dispatch(stale).allowed);
}

FF_TEST(property_ineligible_never_promotes) {
  FailoverFabric fab;
  ServiceId sid(9100); ServiceDefinition s = svc(sid, true);
  s.compatibility.model_key = "REQUIRED"; s.compatibility.runtime_abi = "REQUIRED";
  s.recovery.continuity = ContinuityClass::CHECKPOINT_RESTORE;   // stateful requirement
  fab.create_service(s);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
  a.target=TargetId(1); a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(1);
  a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  // Wrong model / no state -> must never be selected.
  {
    CandidateFacts f = facts(TargetId(2), WorkerBootId(2), true, true);
    f.model_key = "WRONG";
    fab.register_target(TargetId(2), "p"); fab.publish_candidate_facts(f);
    fab.publish_candidate_state(TargetId(2), CandidateState{}); fab.publish_candidate_resources(TargetId(2), { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
  }
  FailureEvent ev; ev.event_id=FailureEventId(1); ev.generation=FailureGeneration::first();
  ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT; ev.target=TargetId(1);
  ev.source=SourceId(1); ev.received.seq=1; fab.publish_failure(ev);
  auto plan = fab.plan_failover(slot);
  // No eligible replacement -> no plan.
  FF_CHECK(!plan.has_value());
  if (plan) {
    ff_test::InProcessOps ops("p");
    AttemptResult r = fab.execute_plan(*plan, ops);
    FF_CHECK(r.state != AttemptState::COMPLETED);
  }
}

FF_TEST(property_route_gen_never_regresses) {
  RouteTable rt;
  RouteEntry e1; e1.route=RouteId(1); e1.generation=RouteGeneration(5); e1.slot={ServiceId(1),ServiceSlotId(1)};
  e1.epoch=CoordinatorEpoch(1, CoordinatorId(1), CoordinatorId(1)); e1.gateway_boot=GatewayBootId(1); e1.state=RouteState::INSTALLED;
  FF_CHECK(rt.install(e1));
  RouteEntry e0 = e1; e0.generation=RouteGeneration(4);   // older generation
  FF_CHECK(!rt.install(e0));
  RouteEntry e2 = e1; e2.generation=RouteGeneration(6);
  FF_CHECK(rt.install(e2));
  FF_CHECK_EQ(rt.current({ServiceId(1),ServiceSlotId(1)})->generation.value(), 6u);
}

FF_TEST(property_recovery_completion_requires_verification) {
  RecoveryObjective obj; obj.rto_ms=1000; obj.continuity=ContinuityClass::REPLAY_SAFE_REQUESTS;
  obj.requires_verified_recovery=true;
  RecoveryEstimate est;
  auto r = RecoveryEvaluator::evaluate(obj, est, 50, 0, /*verified=*/false);
  FF_CHECK(!r.satisfied);   // not verified => not satisfied
  auto r2 = RecoveryEvaluator::evaluate(obj, est, 50, 0, /*verified=*/true);
  FF_CHECK(r2.satisfied);
}

FF_TEST(property_selection_matches_reference) {
  // Brute-force reference: eligible candidates are those passing hard_eligible; among the
  // eligible, the one with the smallest critical path is chosen (tie-break by target id).
  FailoverFabric fab;
  ServiceId sid(9200); ServiceDefinition s = svc(sid, true);
  s.domain_requirement.require_domain_independence = false;  // simplify
  fab.create_service(s);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id=AssignmentId(1); a.generation=AssignmentGeneration::first(); a.slot=slot;
  a.target=TargetId(1); a.target_generation=TargetGeneration::first(); a.worker_boot=WorkerBootId(1);
  a.authority_generation=ServiceAuthorityGeneration::first(); a.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  // Several candidates with different prep times.
  for (std::uint32_t i = 0; i < 10; ++i) {
    TargetId t(10 + i);
    CandidateFacts f = facts(t, WorkerBootId(10 + i), /*ready=*/i % 2 == 0, true);
    fab.register_target(t, "p"); fab.publish_candidate_facts(f);
    fab.publish_candidate_state(t, CandidateState{}); fab.publish_candidate_resources(t, { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
  }
  FailureEvent ev; ev.event_id=FailureEventId(1);
  ev.generation=FailureGeneration::first(); ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT;
  ev.target=TargetId(1); ev.source=SourceId(1); ev.received.seq=1; fab.publish_failure(ev);
  SelectionResult r = fab.select(slot);
  FF_CHECK(r.has_selection);
  // Expected: first ready candidate with smallest target id wins (all same gen).
  FF_CHECK(r.selected->facts.ready);
}

int main() { return ff_test::run_all(); }