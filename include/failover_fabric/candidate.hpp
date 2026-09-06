// candidate.hpp — Candidate bindings and coherent snapshots.
//
// A candidate snapshot must be coherent: capacity, readiness, state, health and
// domains must all come from the same observation generation. Combining capacity
// from one generation with readiness from another and calling the result eligible is
// a bug. Candidate facts are revalidated before committing a promotion.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/domains.hpp"
#include "failover_fabric/identities.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

struct CandidateFacts {
  TargetId target{TargetId::null()};
  TargetGeneration target_generation{TargetGeneration::null()};
  ReplicaId replica{ReplicaId::null()};
  ReplicaGeneration replica_generation{ReplicaGeneration::null()};
  EngineId engine{EngineId::null()};
  EngineGeneration engine_generation{EngineGeneration::null()};
  EngineIncarnationId engine_incarnation{EngineIncarnationId::null()};
  WorkerId worker{WorkerId::null()};
  WorkerBootId worker_boot{WorkerBootId::null()};
  ReadinessProfileId profile{ReadinessProfileId::null()};
  std::string model_key;
  std::string runtime_abi;
  ReadinessProfileGeneration profile_generation{ReadinessProfileGeneration::null()};
  ReadinessGeneration readiness_generation{ReadinessGeneration::null()};
  bool ready{false};
  bool activation_eligible{false};
  HealthGeneration health_generation{HealthGeneration::null()};
  Provenance provenance{Provenance::UNKNOWN};
  Timestamp observed;
  bool evidence_fresh{false};
};

struct CandidateState {
  bool state_required{false};
  bool state_available{false};
  std::optional<StateGeneration> state_generation;
  std::optional<CheckpointGeneration> checkpoint_generation;
  std::uint64_t committed_sequence{0};
  bool correct_session{false};
  bool correct_tenant{false};
  bool correct_model_generation{false};
  bool state_format_ok{false};
  bool status_integrity_ok{false};
  bool replay_safe{false};
};

struct CandidateResource {
  std::string resource_class;
  double amount{0.0};
  CapacityGeneration capacity_generation{CapacityGeneration::null()};
  std::vector<ResourceClaimId> existing_claims;
  bool has_conflicting_commitment{false};
  std::optional<ReservationId> reservation;
  ReservationGeneration reservation_generation{ReservationGeneration::null()};
  bool resources_revoked{false};
  std::uint64_t unit={0};   // opaque resource unit for capacity accounting
};

// Timing/cost estimates (measured vs estimated separated; parallel stages accounted
// through a dependency / critical-path model rather than blind summation).
struct CandidateCosts {
  std::uint64_t estimated_prep_ms{0};
  std::uint64_t estimated_cutover_ms{0};
  std::uint64_t state_restore_ms{0};
  std::uint64_t estimated_critical_path_ms{0};
  bool cost_estimated{true};
  std::uint64_t state_loss_sequences{0};
  std::string route_cost;
};

// --------------------------------------------------------------------------- //
// A fully optional candidate: binding + eligibility evaluation result.
// --------------------------------------------------------------------------- //
struct Candidate {
  CandidateFacts facts;
  std::vector<FailureDomainId> domains;
  std::vector<DomainMembership> memberships;
  CandidateState state;
  std::vector<CandidateResource> resources;
  CandidateCosts costs;

  std::optional<ContinuityClass> continuity;
  std::optional<ExclusionReason> failed_reason;   // set when hard-eligible=false
  std::string failed_detail;
  std::vector<DomainClass> blocked_domain_classes;
  bool hard_eligible{false};
};

// A coherent, immutable snapshot of all candidates considered at one point.
struct CandidateSnapshot {
  CandidateSnapshotGeneration generation;
  ServiceSlotKey slot;
  Timestamp at;
  std::vector<Candidate> candidates;
};

}  // namespace failover_fabric
