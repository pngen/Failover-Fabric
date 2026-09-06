// ff_cli.cpp — CLI inspection of the failover runtime state.
//   ff_cli --demo                     run an in-process scenario and print structured state
//   ff_cli --state <file.ff>          load a persisted state file and print current authority
#include <failover_fabric/failover_fabric.hpp>

#include <cstdio>
#include <string>

using namespace failover_fabric;

static void print_assignment(const char* label, const Assignment& a) {
  std::printf("  [%s] slot(service=%llu slot=%llu) target=%llu state=%s authority_gen=%llu worker_boot=%llu\n",
    label, (unsigned long long)a.slot.service.value(), (unsigned long long)a.slot.slot.value(),
    (unsigned long long)a.target.value(), to_string(a.state),
    (unsigned long long)a.authority_generation.value(), (unsigned long long)a.worker_boot.value());
}

static int demo() {
  FailoverFabric fab; ServiceId sid(1);
  ServiceDefinition s; s.service = sid; s.generation = ServiceGeneration::first(); s.config_generation = ServiceConfigGeneration::first();
  s.name = "svc"; s.role = ServingRole::EXCLUSIVE_ACTIVE; s.compatibility.model_key = "m"; s.compatibility.runtime_abi = "a";
  s.domain_requirement.require_domain_independence = true;
  s.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
  s.recovery.rto_ms = 10000; s.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS; s.recovery.requires_verified_recovery = true;
  s.failback = FailbackPolicy::MANUAL; s.policy_generation = PolicyGeneration::first();
  fab.create_service(s);
  ServiceSlotKey slot{sid, ServiceSlotId(1)};
  Assignment a; a.id = AssignmentId(1); a.generation = AssignmentGeneration::first(); a.slot = slot;
  a.target = TargetId(10); a.target_generation = TargetGeneration::first(); a.worker_boot = WorkerBootId(10);
  a.authority_generation = ServiceAuthorityGeneration::first(); a.state = AssignmentState::ACTIVE;
  fab.set_initial_assignment(a);
  for (TargetId t : {TargetId(10), TargetId(11)}) {
    fab.register_target(t, "demo");
    CandidateFacts f; f.target = t; f.target_generation = TargetGeneration::first(); f.engine_incarnation = EngineIncarnationId(t.value());
    f.worker_boot = WorkerBootId(t.value()); f.profile = ReadinessProfileId(1); f.model_key = "m"; f.runtime_abi = "a";
    f.ready = true; f.evidence_fresh = true;
    fab.publish_candidate_facts(f); fab.publish_candidate_state(t, CandidateState{});
    fab.publish_candidate_resources(t, { CandidateResource{"cpu", 2.0, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, 2} });
  }
  std::printf("Current assignment:\n"); print_assignment("current", *fab.current_assignment(slot));
  std::printf("Eligible candidates: %zu\n", fab.eligible_candidates(slot).size());
  FailureEvent ev; ev.event_id = FailureEventId(1); ev.generation = FailureGeneration::first();
  ev.status = EvidenceStatus::CONFIRMED; ev.category = FailureCategory::PROCESS_EXIT; ev.target = TargetId(10);
  ev.source = SourceId(1); ev.received.seq = 1; fab.publish_failure(ev);
  SelectionResult sr = fab.select(slot);
  std::printf("After failure, selection: %s\n", sr.has_selection ? "yes" : "no");
  for (auto& e : sr.exclusions) std::printf("  excluded target=%llu reason=%s\n", (unsigned long long)e.first.value(), to_string(e.second.first));
  if (sr.selected) std::printf("  selected target=%llu\n", (unsigned long long)sr.selected->facts.target.value());
  return 0;
}

int main(int argc, char** argv) {
  if (argc >= 2) {
    if (std::string(argv[1]) == "--demo") return demo();
    if (std::string(argv[1]) == "--state" && argc >= 3) {
      try {
        FailoverFabric fab; fab.load(argv[2]);
        std::printf("Loaded %s\n", argv[2]);
        for (auto& sv : fab.services()) std::printf("Service %llu %s\n", (unsigned long long)sv.service.value(), sv.name.c_str());
        for (auto& slot : fab.slots(ServiceId(1))) {
          if (auto cur = fab.current_assignment(slot)) print_assignment("current", *cur);
        }
        std::printf("revalidation_required_count=%zu\n", fab.revalidation_required_count());
        return 0;
      } catch (const std::exception& e) { std::fprintf(stderr, "load failed: %s\n", e.what()); return 1; }
    }
  }
  std::fprintf(stderr, "usage: ff_cli --demo | --state <file.ff>\n");
  return 2;
}
