// plan.hpp — Failover plan and its irreversible milestones.
//
// Planning MUST NOT mutate authoritative assignment. The plan is validated again before
// execution, and a superseded plan must not commit. Each milestone is recorded so a
// restart can distinguish intent-only from old-admission-fenced, replacement-authorized,
// activation-acknowledged, route-installed, and verification-complete states.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/candidate.hpp"
#include "failover_fabric/evidence.hpp"
#include "failover_fabric/fence.hpp"
#include "failover_fabric/recovery.hpp"
#include "failover_fabric/service.hpp"

namespace failover_fabric {

// The set of order-preserving milestones a transaction passes. These are the durable
// intent markers used for restart reconciliation.
enum class Milestone : std::uint8_t {
  INTENT_ONLY,
  OLD_ADMISSION_FENCED,
  REPLACEMENT_AUTHORIZED,
  ACTIVATION_ACKNOWLEDGED,
  ROUTE_INSTALLED,
  VERIFICATION_COMPLETE
};
const char* to_string(Milestone m) noexcept;

struct RollbackBoundary {
  Milestone up_to;                     // safe to abandon up to and including this
  bool old_assignment_may_remain{false};
};

struct FailoverPlan {
  FailoverPlanId plan{FailoverPlanId::null()};
  FailoverPlanGeneration generation;
  ServiceSlotKey slot;
  std::optional<Assignment> source_assignment;
  std::vector<FailureEvent> triggering_evidence;
  CandidateSnapshot snapshot;
  std::optional<Candidate> selected;
  ContinuityClass continuity{ContinuityClass::UNKNOWN};
  std::vector<FailureDomainId> resource_commitments_domain;
  FencingRequirement fencing_requirement;
  RouteId requested_route{RouteId::null()};
  RouteGeneration requested_route_generation;
  RecoveryObjective recovery_objective;
  RecoveryEstimate estimated_stages;
  std::vector<RollbackBoundary> rollback_boundaries;
  PolicyGeneration policy_gen;
  std::vector<Milestone> reached_milestones;
  Provenance provenance{Provenance::DERIVED};

  bool reached(Milestone m) const;
  void record_milestone(Milestone m);
  bool is_valid() const;
};

}  // namespace failover_fabric
