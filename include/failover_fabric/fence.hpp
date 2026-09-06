// fence.hpp — Fencing authorization and enforcement points.
//
// Fencing is mandatory before an exclusive replacement can serve new authoritative
// work. Generation-bound authorization means an isolated old worker may finish already
// admitted physical work, but its NEW work and STALE result publication are rejected at
// the governed boundary, and it is never granted unlimited future serving authority.
#pragma once

#include <cstdint>
#include <string>

#include "failover_fabric/authority.hpp"
#include "failover_fabric/identities.hpp"
#include "failover_fabric/service.hpp"

namespace failover_fabric {

enum class FenceEnforcement : std::uint8_t {
  COORDINATOR_ASSIGNMENT_AUTHORITY,
  GATEWAY_DISPATCH,
  WORKER_ADMISSION,
  RESULT_ACCEPTANCE,
  EXTERNAL_SIDE_EFFECT_BOUNDARY
};
const char* to_string(FenceEnforcement e) noexcept;

enum class FenceState : std::uint8_t {
  NONE,
  REQUESTED,
  ENFORCED,
  UNPROVEN,
  SUPERSEDED
};
const char* to_string(FenceState s) noexcept;

// A fence grants/revokes authority for one assignment + worker boot.
struct FenceAuthorization {
  FenceId fence{FenceId::null()};
  FenceGeneration generation;
  CoordinatorEpoch epoch;
  ServiceSlotKey slot;
  AssignmentId assignment{AssignmentId::null()};
  AssignmentGeneration assignment_generation;
  WorkerBootId fenced_boot{WorkerBootId::null()};   // boot whose authority is revoked
  WorkerBootId permitted_boot{WorkerBootId::null()}; // boot that may now serve
  FenceState state{FenceState::NONE};
  std::uint64_t deadline_ns{0};   // bound on offered authority lifetime
  bool revokes_new_dispatch{false};
};

// A set of enforcement points must ALL be satisfiable before an exclusive replacement
// is allowed to serve authoritative work. If any mandated point cannot enforce fencing,
// the promotion must return FENCING_UNPROVEN and MUST NOT proceed.
struct FencingRequirement {
  bool require_gateway_dispatch{true};
  bool require_worker_admission{true};
  bool require_result_acceptance{true};
  bool require_coordinator_authority{true};
  bool require_external_effect_boundary{false};
  std::string effect_sink_note;
};

// --------------------------------------------------------------------------- //
// Fence evaluation: given the generated fence for the candidate, is every required
// enforcement point actually satisfied by the reference runtime?
// --------------------------------------------------------------------------- //
class FenceEvaluator {
 public:
  static bool enforcement_satisfied(FenceEnforcement e, const FenceAuthorization& fence);
  // Returns true when the requirement is satisfied; false when FENCING_UNPROVEN.
  static bool full_enforcement_possible(const FencingRequirement& req, const FenceAuthorization& fence);
};

}  // namespace failover_fabric
