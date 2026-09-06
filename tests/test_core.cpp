// test_core.cpp — core doctrine: evidence, domains, eligibility, selection,
// exclusive authority, transactional cutover, fencing, ambiguity, persistence.
#include "framework.hpp"
#include "inprocess_ops.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>
#include <map>
#include <string>

using namespace failover_fabric;

static ServiceDefinition make_service(ServiceId sid) {
  ServiceDefinition svc;
  svc.service = sid;
  svc.generation = ServiceGeneration::first();
  svc.config_generation = ServiceConfigGeneration::first();
  svc.name = "ref-det-service";
  svc.role = ServingRole::EXCLUSIVE_ACTIVE;
  svc.compatibility.model_key = "ref-det-sequence-v1";
  svc.compatibility.runtime_abi = "ff-ref-v1";
  svc.domain_requirement.require_domain_independence = true;
  svc.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
  svc.recovery.rto_ms = 10000;
  svc.recovery.rto_anchor = RtoAnchor::FIRST_ACCEPTED_FAILURE_OBSERVATION;
  svc.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
  svc.recovery.requires_verified_recovery = true;
  svc.failback = FailbackPolicy::MANUAL;
  svc.policy_generation = PolicyGeneration::first();
  return svc;
}

static CandidateFacts facts(TargetId t, WorkerBootId boot, bool ready = true) {
  CandidateFacts f;
  f.target = t;
  f.target_generation = TargetGeneration::first();
  f.replica = ReplicaId(t.value());
  f.engine = EngineId(t.value());
  f.engine_incarnation = EngineIncarnationId(t.value());
  f.worker = WorkerId(t.value());
  f.worker_boot = boot;
  f.profile = ReadinessProfileId(1);
  f.model_key = "ref-det-sequence-v1";
  f.runtime_abi = "ff-ref-v1";
  f.ready = ready;
  f.activation_eligible = true;
  f.evidence_fresh = true;
  return f;
}

static CandidateState ready_state() {
  CandidateState s;
  s.state_available = false;   // stateless reference
  return s;
}

static std::vector<CandidateResource> cpu_ok() {
  return { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} };
}

static void register_worker(FailoverFabric& fab, TargetId t, WorkerBootId boot, bool ready = true) {
  fab.register_target(t, "pipe-" + std::to_string(t.value()));
  fab.publish_candidate_facts(facts(t, boot, ready));
  fab.publish_candidate_state(t, ready_state());
  fab.publish_candidate_resources(t, cpu_ok());
}

FF_TEST(service_identity_and_types) {
  ServiceId s(7); TargetId t(3);
  FF_CHECK_EQ(s, ServiceId(7));
  FF_CHECK(s != ServiceId(8));
  RouteGeneration rg(5);
  FF_CHECK_EQ(rg.next().value(), 6);
  FF_REQUIRE_THROW(Gen<RouteTag>(std::numeric_limits<std::uint64_t>::max()).next(), GenerationExhausted);
}

FF_TEST(exclusive_assignment_invariant) {
  FailoverFabric fab;
  ServiceId sid(1);
  fab.create_service(make_service(sid));
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a;
  a.id = AssignmentId(1); a.generation = AssignmentGeneration::first();
  a.slot = slot; a.target = TargetId(10); a.target_generation = TargetGeneration::first();
  a.authority_generation = ServiceAuthorityGeneration::first();
  a.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  // A second current assignment for the same slot must be refused.
  Assignment b = a; b.id = AssignmentId(2); b.target = TargetId(11);
  FF_REQUIRE_THROW(fab.set_initial_assignment(b), std::logic_error);
  FF_CHECK_EQ(fab.current_assignment(slot)->target, TargetId(10));
}

FF_TEST(failure_evidence_ordering) {
  EvidenceStore es;
  FailureEvent dead;
  dead.event_id = FailureEventId(1); dead.generation = FailureGeneration::first();
  dead.status = EvidenceStatus::CONFIRMED; dead.category = FailureCategory::PROCESS_EXIT;
  dead.target = TargetId(1); dead.source = SourceId(1); dead.source_boot = SourceBootId(1);
  dead.received.seq = 2;
  FF_CHECK(es.publish(dead));
  // An older healthy/clear observation must not erase the confirmed failure.
  FailureEvent late_clear;
  late_clear.event_id = FailureEventId(2); late_clear.generation = FailureGeneration::first();
  late_clear.status = EvidenceStatus::CLEARED; late_clear.target = TargetId(1);
  late_clear.received.seq = 1;  // older receipt
  FF_CHECK(es.publish(late_clear));  // accepted as separate event but does not clear
  FF_CHECK(es.latest_failure_for(TargetId(1)).has_value());
}

FF_TEST(domain_eligibility) {
  FailoverFabric fab;
  ServiceId sid(2);
  ServiceDefinition svc = make_service(sid);
  svc.domain_requirement.require_domain_independence = true;
  fab.create_service(svc);
  fab.declare_domain(DomainDecl{FailureDomainId(1), FailureDomainGeneration::first(), DomainClass::HOST, "h1", std::nullopt, Provenance::SYNTHETIC});
  fab.declare_domain(DomainDecl{FailureDomainId(3), FailureDomainGeneration::first(), DomainClass::HOST, "h2", std::nullopt, Provenance::SYNTHETIC});
  fab.declare_domain(DomainDecl{FailureDomainId(2), FailureDomainGeneration::first(), DomainClass::DEVICE, "d1", FailureDomainId(1), Provenance::SYNTHETIC});
  fab.declare_domain(DomainDecl{FailureDomainId(4), FailureDomainGeneration::first(), DomainClass::DEVICE, "d2", FailureDomainId(3), Provenance::SYNTHETIC});
  fab.set_membership(DomainMembership{TargetId(10), FailureDomainId(2), true});
  fab.set_membership(DomainMembership{TargetId(11), FailureDomainId(2), true});
  fab.set_membership(DomainMembership{TargetId(12), FailureDomainId(1), false});
  fab.set_membership(DomainMembership{TargetId(13), FailureDomainId(4), true});

  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id = AssignmentId(1); a.generation = AssignmentGeneration::first();
  a.slot = slot; a.target = TargetId(10); a.target_generation = TargetGeneration::first();
  a.worker_boot = WorkerBootId(10);
  a.authority_generation = ServiceAuthorityGeneration::first();
  a.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);

  register_worker(fab, TargetId(11), WorkerBootId(11));
  register_worker(fab, TargetId(12), WorkerBootId(12));
  register_worker(fab, TargetId(13), WorkerBootId(13));

  FailureEvent ev; ev.event_id = FailureEventId(1); ev.generation = FailureGeneration::first();
  ev.status = EvidenceStatus::CONFIRMED; ev.category = FailureCategory::DEVICE_UNAVAILABLE;
  ev.target = TargetId(10); ev.source = SourceId(1); ev.received.seq = 1;
  fab.publish_failure(ev);

  SelectionResult r = fab.select(slot);
  FF_CHECK(r.has_selection);
  FF_CHECK_EQ(r.selected->facts.target, TargetId(13));
  bool saw_same = false, saw_unk = false;
  for (auto& e : r.exclusions) {
    if (e.first == TargetId(11) && e.second.first == ExclusionReason::FAILED_DOMAIN) saw_same = true;
    if (e.first == TargetId(12) && e.second.first == ExclusionReason::DOMAIN_INDEPENDENCE_UNKNOWN) saw_unk = true;
  }
  FF_CHECK(saw_same); FF_CHECK(saw_unk);
}

int main() { return ff_test::run_all(); }