// service.hpp — Service contract, slots, and assignments.
//
// A service contract is represented independently of any current target. An
// assignment binds a service slot to a single exclusive target (plus the engine
// incarnation, replica generation, readiness profile, authority/activation/route
// generations, resource commitments, and continuity references). At most one
// assignment may authorize new work for a slot at a time; historical assignments
// remain inspectable but non-authoritative.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/identities.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// A single service slot. Services may be partitioned into slots; each slot is an
// independent unit of failover and exclusive active authority.
// --------------------------------------------------------------------------- //
struct ServiceSlotKey {
  ServiceId service{ServiceId::null()};
  ServiceSlotId slot{ServiceSlotId::null()};

  friend bool operator==(const ServiceSlotKey& a, const ServiceSlotKey& b) noexcept {
    return a.service == b.service && a.slot == b.slot;
  }
  friend bool operator<(const ServiceSlotKey& a, const ServiceSlotKey& b) noexcept {
    return a.service != b.service ? a.service < b.service : a.slot < b.slot;
  }
};

// --------------------------------------------------------------------------- //
// Serving role.
// --------------------------------------------------------------------------- //
enum class ServingRole : std::uint8_t { EXCLUSIVE_ACTIVE, PARTITIONED_SLOTS, MULTI_ACTIVE_SLOTS };
const char* to_string(ServingRole r) noexcept;

struct SlotRange {
  std::uint32_t first{1};
  std::uint32_t count{1};
};

// --------------------------------------------------------------------------- //
// Anti-flapping policy.
// --------------------------------------------------------------------------- //
struct AntiFlappingPolicy {
  std::uint32_t max_auto_attempts{3};
  std::uint32_t cooldown_ms{5000};
  std::uint32_t hysteresis_ms{5000};
  bool manual_intervention_required{false};
};

// --------------------------------------------------------------------------- //
// Recovery requirements.
// --------------------------------------------------------------------------- //
enum class RtoAnchor : std::uint8_t {
  FIRST_ACCEPTED_FAILURE_OBSERVATION,
  EXPLICIT_FAILOVER_REQUEST,
  SERVICE_CONTRACT_ANCHOR
};
const char* to_string(RtoAnchor a) noexcept;

struct RecoveryObjective {
  std::uint64_t rto_ms{0};                      // recovery-time objective (product deadline)
  RtoAnchor rto_anchor{RtoAnchor::FIRST_ACCEPTED_FAILURE_OBSERVATION};
  ContinuityClass continuity{ContinuityClass::UNKNOWN};
  std::uint64_t max_state_loss_sequences{0};   // permitted committed-sequence loss
  std::optional<CheckpointGeneration> min_checkpoint_gen;
  bool requires_verified_recovery{true};
};

// --------------------------------------------------------------------------- //
// Capability / compatibility requirement.
// --------------------------------------------------------------------------- //
struct CompatibilityRequirement {
  std::string model_key;                  // e.g. "ref-stateful-sequence-v1"
  std::string runtime_abi;                // e.g. "ff-ref-v1"
  std::optional<ReadinessProfileId> readiness_profile;
  std::vector<DependencyId> dependencies;
  std::vector<CompatibilityId> required_capabilities;
};

// --------------------------------------------------------------------------- //
// Resource requirements.
// --------------------------------------------------------------------------- //
struct ResourceRequirement {
  std::string resource_class;             // "cuda_device", "cpu", "memory"
  double amount{0.0};
  bool exactly_one_target{true};
};

// --------------------------------------------------------------------------- //
// Failure-domain requirement.
// --------------------------------------------------------------------------- //
struct DomainRequirement {
  bool require_domain_independence{false};
  std::uint32_t min_distinct_hosts{0};
  std::uint32_t min_distinct_devices{0};
  std::vector<DomainClass> forbidden_overlap_classes;
};

// --------------------------------------------------------------------------- //
// The service contract, independent of any current target.
// --------------------------------------------------------------------------- //
struct ServiceDefinition {
  ServiceId service{ServiceId::null()};
  ServiceGeneration generation;
  ServiceConfigGeneration config_generation;
  std::string name;
  ServingRole role{ServingRole::EXCLUSIVE_ACTIVE};
  SlotRange slots;
  CompatibilityRequirement compatibility;
  DomainRequirement domain_requirement;
  std::vector<ResourceRequirement> resources;
  RecoveryObjective recovery;
  FailbackPolicy failback{FailbackPolicy::MANUAL};
  AntiFlappingPolicy anti_flapping;
  std::vector<StateId> required_state;
  Provenance provenance{Provenance::REPORTED};
  PolicyGeneration policy_generation;

  bool has_valid_identity() const noexcept { return !service.is_null(); }
};

// --------------------------------------------------------------------------- //
// A single exclusive assignment. At most one may be ACTIVE per slot.
// --------------------------------------------------------------------------- //
struct Assignment {
  AssignmentId id{AssignmentId::null()};
  AssignmentGeneration generation;
  ServiceSlotKey slot;
  TargetId target{TargetId::null()};
  TargetGeneration target_generation;
  ReplicaId replica{ReplicaId::null()};
  ReplicaGeneration replica_generation;
  EngineId engine{EngineId::null()};
  EngineIncarnationId engine_incarnation{EngineIncarnationId::null()};
  ReadinessProfileId readiness_profile{ReadinessProfileId::null()};
  ServiceAuthorityGeneration authority_generation;
  ActivationId activation{ActivationId::null()};
  ActivationGeneration activation_generation;
  RouteId route{RouteId::null()};
  RouteGeneration route_generation;
  WorkerBootId worker_boot{WorkerBootId::null()};
  std::vector<ResourceClaimId> resource_claims;
  std::vector<StateId> continuity_state;
  AssignmentState state{AssignmentState::CANDIDATE};
  bool provisory{false};   // true while this assignment is a not-yet-committed plan
  Provenance provenance{Provenance::DERIVED};

  bool is_authorizing() const noexcept {
    return state == AssignmentState::AUTHORIZED || state == AssignmentState::ACTIVE;
  }
};

}  // namespace failover_fabric
namespace std {
template <> struct hash<failover_fabric::ServiceSlotKey> {
  std::size_t operator()(const failover_fabric::ServiceSlotKey& k) const noexcept {
    auto h1 = std::hash<failover_fabric::ServiceId>{}(k.service);
    auto h2 = std::hash<failover_fabric::ServiceSlotId>{}(k.slot);
    return h1 ^ (h2 + 0x9e3779b9u + (h1 << 6) + (h1 >> 2));
  }
};
}  // namespace std
