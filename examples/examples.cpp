// examples.cpp — runnable demonstrations of the core failover machine (in-process).
// Each scenario uses a real FailoverFabric and a small in-process CutoverOps adapter so
// the authoritative transition (select -> fence -> activate -> route -> verify -> commit)
// is exercised without a socket dependency.
#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>
#include <string>

using namespace failover_fabric;

namespace {
struct SimpleOps : CutoverOps {
  bool unproven{false};
  FenceOutcome fence_old(const Assignment&, WorkerBootId) override {
    FenceOutcome fo;
    if (unproven) { fo.fencing_unproven = true; fo.state = FenceState::UNPROVEN; }
    else { fo.applied = true; fo.state = FenceState::ENFORCED; }
    return fo;
  }
  ActivateOutcome activate(const Candidate&, const WorkerAuthorization&, ServiceAuthorityGeneration) override { return ActivateOutcome{true, "", "ok"}; }
  RouteOutcome install_route(const RouteEntry& e, GatewayBootId) override { return RouteOutcome{true, e.route, e.generation, "ok"}; }
  VerifyOutcome verify(const RouteEntry&, const WorkerAuthorization&) override { return VerifyOutcome{true, "cpu-parity-ok", false, "ok"}; }
};

ServiceDefinition make_svc(ServiceId sid, bool independent = true, ContinuityClass cont = ContinuityClass::REPLAY_SAFE_REQUESTS) {
  ServiceDefinition s; s.service = sid; s.generation = ServiceGeneration::first();
  s.config_generation = ServiceConfigGeneration::first(); s.name = "demo"; s.role = ServingRole::EXCLUSIVE_ACTIVE;
  s.compatibility.model_key = "ref-det-sequence-v1"; s.compatibility.runtime_abi = "ff-ref-v1";
  s.domain_requirement.require_domain_independence = independent;
  s.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
  s.recovery.rto_ms = 10000; s.recovery.rto_anchor = RtoAnchor::FIRST_ACCEPTED_FAILURE_OBSERVATION;
  s.recovery.continuity = cont; s.recovery.requires_verified_recovery = true;
  s.failback = FailbackPolicy::MANUAL; s.policy_generation = PolicyGeneration::first();
  return s;
}
void reg(FailoverFabric& fab, TargetId t, WorkerBootId b, bool ready = true) {
  fab.register_target(t, "demo-" + std::to_string(t.value()));
  CandidateFacts f; f.target = t; f.target_generation = TargetGeneration::first(); f.replica = ReplicaId(t.value());
  f.engine = EngineId(t.value()); f.engine_incarnation = EngineIncarnationId(t.value()); f.worker = WorkerId(t.value());
  f.worker_boot = b; f.profile = ReadinessProfileId(1); f.model_key = "ref-det-sequence-v1"; f.runtime_abi = "ff-ref-v1";
  f.ready = ready; f.activation_eligible = ready; f.evidence_fresh = true;
  fab.publish_candidate_facts(f);
  fab.publish_candidate_state(t, CandidateState{});
  fab.publish_candidate_resources(t, { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} });
}
void set_active(FailoverFabric& fab, ServiceSlotKey slot, TargetId t, WorkerBootId b) {
  Assignment a; a.id = AssignmentId(t.value()); a.generation = AssignmentGeneration::first(); a.slot = slot;
  a.target = t; a.target_generation = TargetGeneration::first(); a.worker_boot = b;
  a.authority_generation = ServiceAuthorityGeneration::first(); a.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
}
void fail(FailoverFabric& fab, TargetId t, std::uint64_t seq) {
  FailureEvent ev; ev.event_id = FailureEventId(seq); ev.generation = FailureGeneration::first();
  ev.status = EvidenceStatus::CONFIRMED; ev.category = FailureCategory::PROCESS_EXIT; ev.target = t;
  ev.source = SourceId(1); ev.received.seq = seq; fab.publish_failure(ev);
}
}  // namespace

int main() {
  std::printf("FailoverFabric example\n");

  // Scenario 1: basic exclusive failover A -> B.
  {
    std::printf("-- basic exclusive failover --\n");
    FailoverFabric fab; ServiceId sid(1); fab.create_service(make_svc(sid));
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    set_active(fab, slot, TargetId(10), WorkerBootId(10));
    reg(fab, TargetId(10), WorkerBootId(10)); reg(fab, TargetId(11), WorkerBootId(11));
    fail(fab, TargetId(10), 1);
    auto plan = fab.plan_failover(slot);
    std::printf("  plan=%s selected=%llu\n", plan ? "yes" : "no", plan && plan->selected ? (unsigned long long)plan->selected->facts.target.value() : 0ull);
    SimpleOps ops;
    AttemptResult r = plan ? fab.execute_plan(*plan, ops) : AttemptResult{};
    std::printf("  attempt=%s new_target=%llu\n", to_string(r.state), r.committed_assignment ? (unsigned long long)r.committed_assignment->target.value() : 0ull);
    std::printf("  outcome=%s\n", to_string(fab.service_outcome(slot)));
  }

  // Scenario 2: failure-domain exclusion (same host rejected).
  {
    std::printf("-- failure-domain exclusion --\n");
    FailoverFabric fab; ServiceId sid(2); fab.create_service(make_svc(sid, true));
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    fab.declare_domain(DomainDecl{FailureDomainId(1), FailureDomainGeneration::first(), DomainClass::HOST, "h1", std::nullopt, Provenance::SYNTHETIC});
    fab.declare_domain(DomainDecl{FailureDomainId(2), FailureDomainGeneration::first(), DomainClass::DEVICE, "d2", FailureDomainId(1), Provenance::SYNTHETIC});
    fab.set_membership(DomainMembership{TargetId(10), FailureDomainId(1), true});
    fab.set_membership(DomainMembership{TargetId(11), FailureDomainId(1), true});  // same host
    fab.set_membership(DomainMembership{TargetId(12), FailureDomainId(2), true});  // independent device in same host? parent id1 -> same host too
    set_active(fab, slot, TargetId(10), WorkerBootId(10));
    reg(fab, TargetId(10), WorkerBootId(10)); reg(fab, TargetId(11), WorkerBootId(11)); reg(fab, TargetId(12), WorkerBootId(12));
    fail(fab, TargetId(10), 1);
    SelectionResult sr = fab.select(slot);
    std::printf("  has_selection=%s\n", sr.has_selection ? "yes" : "no");
    for (auto& e : sr.exclusions) std::printf("  excluded target=%llu reason=%s\n", (unsigned long long)e.first.value(), to_string(e.second.first));
  }

  // Scenario 3: no eligible candidate.
  {
    std::printf("-- no eligible candidate --\n");
    FailoverFabric fab; ServiceId sid(3); fab.create_service(make_svc(sid, true));
    ServiceSlotKey slot{sid, ServiceSlotId(1)};
    set_active(fab, slot, TargetId(10), WorkerBootId(10));
    // Only a same-host, unready candidate.
    fab.declare_domain(DomainDecl{FailureDomainId(1), FailureDomainGeneration::first(), DomainClass::HOST, "h1", std::nullopt, Provenance::SYNTHETIC});
    fab.set_membership(DomainMembership{TargetId(10), FailureDomainId(1), true});
    fab.set_membership(DomainMembership{TargetId(11), FailureDomainId(1), true});
    reg(fab, TargetId(10), WorkerBootId(10)); reg(fab, TargetId(11), WorkerBootId(11), /*ready=*/false);
    fail(fab, TargetId(10), 1);
    auto plan = fab.plan_failover(slot);
    std::printf("  plan=%s (expected no)\n", plan ? "yes" : "no");
  }

  // Scenario 4: recovery objective violation is never labelled SLO-compliant.
  {
    std::printf("-- recovery objective --\n");
    RecoveryObjective obj; obj.rto_ms = 1; obj.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS; obj.requires_verified_recovery = true;
    auto r = RecoveryEvaluator::evaluate(obj, RecoveryEstimate{}, 5000, 0, /*verified=*/true);
    std::printf("  rto_met=%d satisfied=%d (expected 0,0)\n", (int)r.rto_met, (int)r.satisfied);
  }

  std::printf("done\n");
  return 0;
}
