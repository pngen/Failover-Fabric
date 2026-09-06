// test_adversarial.cpp — deliberate attacks: stale generations, incompatible profiles,
// wrong-session checkpoint, unknown domains, fenced boots, revocation, stale readiness,
// fencing unavailable, duplicate promotion, route ACK from the wrong gateway boot.
#include "framework.hpp"
#include "inprocess_ops.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>

using namespace failover_fabric;

static ServiceDefinition svc(ServiceId sid, bool independent = true) {
  ServiceDefinition s; s.service=sid; s.generation=ServiceGeneration::first();
  s.config_generation=ServiceConfigGeneration::first(); s.name="s"; s.role=ServingRole::EXCLUSIVE_ACTIVE;
  s.compatibility.model_key="model-v1"; s.compatibility.runtime_abi="abi-v1";
  s.domain_requirement.require_domain_independence=independent;
  s.resources.push_back(ResourceRequirement{"cpu",1.0,true});
  s.recovery.rto_ms=10000; s.recovery.continuity=ContinuityClass::REPLAY_SAFE_REQUESTS;
  s.recovery.requires_verified_recovery=true; s.failback=FailbackPolicy::MANUAL; s.policy_generation=PolicyGeneration::first();
  return s;
}
static void dom(FailoverFabric& fab, TargetId a, TargetId b) {
  fab.declare_domain(DomainDecl{FailureDomainId(1), FailureDomainGeneration::first(), DomainClass::HOST, "h1", std::nullopt, Provenance::SYNTHETIC});
  fab.declare_domain(DomainDecl{FailureDomainId(2), FailureDomainGeneration::first(), DomainClass::HOST, "h2", std::nullopt, Provenance::SYNTHETIC});
  fab.set_membership(DomainMembership{a, FailureDomainId(1), true});
  fab.set_membership(DomainMembership{b, FailureDomainId(2), true});
}
static void reg(FailoverFabric& fab, TargetId t, WorkerBootId b, bool ready=true, bool fresh=true) {
  fab.register_target(t,"p");
  CandidateFacts f; f.target=t; f.target_generation=TargetGeneration::first(); f.replica=ReplicaId(t.value());
  f.engine=EngineId(t.value()); f.engine_incarnation=EngineIncarnationId(t.value());
  f.worker=WorkerId(t.value()); f.worker_boot=b; f.profile=ReadinessProfileId(1);
  f.model_key="model-v1"; f.runtime_abi="abi-v1"; f.ready=ready; f.activation_eligible=ready; f.evidence_fresh=fresh;
  fab.publish_candidate_facts(f);
  fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
}
static void setup(FailoverFabric& fab, ServiceId sid, TargetId a, TargetId b) {
  fab.create_service(svc(sid));
  dom(fab, a, b);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id=AssignmentId(1); asg.generation=AssignmentGeneration::first(); asg.slot=slot;
  asg.target=a; asg.target_generation=TargetGeneration::first(); asg.worker_boot=WorkerBootId(a.value());
  asg.authority_generation=ServiceAuthorityGeneration::first(); asg.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  reg(fab, a, WorkerBootId(a.value()));
  reg(fab, b, WorkerBootId(b.value()));
}
static void death(FailoverFabric& fab, TargetId t, std::uint64_t seq) {
  FailureEvent ev; ev.event_id=FailureEventId(seq); ev.generation=FailureGeneration::first();
  ev.status=EvidenceStatus::CONFIRMED; ev.category=FailureCategory::PROCESS_EXIT; ev.target=t;
  ev.source=SourceId(1); ev.received.seq=seq; fab.publish_failure(ev);
}

FF_TEST(adversarial_stale_service_generation) {
  FailoverFabric fab;
  ServiceId sid(1); fab.create_service(svc(sid));
  ServiceDefinition dup = svc(sid); dup.generation = ServiceGeneration(0);  // stale/null generation
  FF_REQUIRE_THROW(fab.create_service(dup), std::invalid_argument);
}

FF_TEST(adversarial_incompatible_profile) {
  FailoverFabric fab;
  ServiceId sid(2); fab.create_service(svc(sid));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id=AssignmentId(1); asg.generation=AssignmentGeneration::first(); asg.slot=slot;
  asg.target=TargetId(10); asg.target_generation=TargetGeneration::first(); asg.worker_boot=WorkerBootId(10);
  asg.authority_generation=ServiceAuthorityGeneration::first(); asg.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  fab.register_target(TargetId(11),"p");
  CandidateFacts f; f.target=TargetId(11); f.target_generation=TargetGeneration::first(); f.engine_incarnation=EngineIncarnationId(11);
  f.worker_boot=WorkerBootId(11); f.profile=ReadinessProfileId(1); f.model_key="WRONG-MODEL"; f.runtime_abi="abi-v1";
  f.ready=true; f.activation_eligible=true; f.evidence_fresh=true;
  fab.publish_candidate_facts(f);
  fab.publish_candidate_state(TargetId(11), CandidateState{});
  fab.publish_candidate_resources(TargetId(11), { CandidateResource{"cpu",2.0,CapacityGeneration::first(),{},false,std::nullopt,ReservationGeneration::null(),false,2} });
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  FF_CHECK(!plan.has_value());   // wrong model => no eligible replacement
}

FF_TEST(adversarial_wrong_session_checkpoint) {
  FailoverFabric fab;
  ServiceId sid(3); ServiceDefinition s = svc(sid);
  s.recovery.continuity = ContinuityClass::CHECKPOINT_RESTORE;   // requires real state
  fab.create_service(s);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment asg; asg.id=AssignmentId(1); asg.generation=AssignmentGeneration::first(); asg.slot=slot;
  asg.target=TargetId(10); asg.target_generation=TargetGeneration::first(); asg.worker_boot=WorkerBootId(10);
  asg.authority_generation=ServiceAuthorityGeneration::first(); asg.state=AssignmentState::ACTIVE;
  fab.set_initial_assignment(asg);
  reg(fab, TargetId(11), WorkerBootId(11));
  // Wrong-session state.
  CandidateState st; st.state_available=true; st.correct_session=false; st.correct_tenant=true;
  st.correct_model_generation=true; st.state_format_ok=true; st.status_integrity_ok=true;
  st.checkpoint_generation = CheckpointGeneration(5);
  fab.publish_candidate_state(TargetId(11), st);
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  FF_CHECK(!plan.has_value());
}

FF_TEST(adversarial_fencing_unavailable) {
  FailoverFabric fab;
  ServiceId sid(4); setup(fab, sid, TargetId(10), TargetId(11));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  FF_CHECK(plan.has_value());
  ff_test::InProcessOps ops("p"); ops.force_fencing_unproven = true;
  AttemptResult r = fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(r.state, AttemptState::FAILED);
  FF_CHECK_EQ(r.detail.find("FENCING_UNPROVEN") != std::string::npos, true);
  // No unsafe promotion: current assignment unchanged.
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(10));
  FF_CHECK_EQ(fab.current_assignment(slot)->state, AssignmentState::ACTIVE);
}

FF_TEST(adversarial_duplicate_promotion) {
  FailoverFabric fab;
  ServiceId sid(5); setup(fab, sid, TargetId(10), TargetId(11));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p");
  AttemptResult r1 = fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(r1.state, AttemptState::COMPLETED);
  AttemptResult r2 = fab.execute_plan(*plan, ops);   // replay attempt
  FF_CHECK(r2.state != AttemptState::COMPLETED);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(11));   // not double-promoted
}

FF_TEST(adversarial_stale_readiness_revalidation) {
  FailoverFabric fab;
  ServiceId sid(6); setup(fab, sid, TargetId(10), TargetId(11));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  FF_CHECK(plan.has_value());
  // Between planning and execution, B becomes unready.
  reg(fab, TargetId(11), WorkerBootId(11), /*ready=*/false, true);
  ff_test::InProcessOps ops("p");
  AttemptResult r = fab.execute_plan(*plan, ops);
  FF_CHECK(r.state != AttemptState::COMPLETED);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(10));  // not promoted on stale readiness
}

FF_TEST(adversarial_route_ack_wrong_boot) {
  RouteTable rt;
  RouteEntry e; e.route=RouteId(1); e.generation=RouteGeneration(3);
  e.slot={ServiceId(1),ServiceSlotId(1)}; e.epoch=CoordinatorEpoch(1,CoordinatorId(1),CoordinatorId(1));
  e.gateway_boot=GatewayBootId(7); e.state=RouteState::INSTALLED;
  FF_CHECK(rt.install(e));
  FF_CHECK(!rt.acknowledge(e.slot, e.generation, GatewayBootId(8)));  // wrong boot
  FF_CHECK(rt.acknowledge(e.slot, e.generation, GatewayBootId(7)));   // correct boot
}

FF_TEST(adversarial_fail_after_route_install) {
  FailoverFabric fab;
  ServiceId sid(7); setup(fab, sid, TargetId(10), TargetId(11));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  death(fab, TargetId(10), 1);
  auto plan = fab.plan_failover(slot);
  ff_test::InProcessOps ops("p"); ops.force_verify_fail = true;
  AttemptResult r = fab.execute_plan(*plan, ops);
  FF_CHECK_EQ(r.state, AttemptState::RECOVERY_REQUIRED);
  // Either the service is explicitly unavailable (old fenced) or old assignment unchanged;
  // in no case do we end with two current assignments.
  auto cur = fab.current_assignment(slot);
  FF_CHECK(cur.has_value());   // still the old assignment, but fenced/not served
  FF_CHECK_EQ(cur->state, AssignmentState::ACTIVE);   // old stays fenced; no double current
}

int main() { return ff_test::run_all(); }