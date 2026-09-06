// identities.hpp — Strongly typed identities and generations.
//
// Every distinct concept (service, target, worker, route, …) has its own strongly
// typed identity, and every versioned concept has its own strongly typed generation.
// Semantically separate generations are NEVER collapsed into one interchangeable
// integer type. A stale RouteGeneration must not route new work to an old target; a
// stale PromotionGeneration must not activate a replacement; a stale WorkerBootId
// must not republish an old target as current.
//
// Generation advance refuses to wrap: reaching the maximum value is an exhaustion
// condition, and the caller must fail closed instead of reusing old valid authority.
#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <ostream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// Generation exhaustion
// --------------------------------------------------------------------------- //
class GenerationExhausted : public std::runtime_error {
 public:
  explicit GenerationExhausted(std::string what) : std::runtime_error(std::move(what)) {}
};

namespace detail {
constexpr std::uint64_t kGenMax = std::numeric_limits<std::uint64_t>::max();
}  // namespace detail

// --------------------------------------------------------------------------- //
// Id<Tag>: a strongly typed, opaque, orderable, hashable numeric identity.
// --------------------------------------------------------------------------- //
template <typename Tag, std::uint64_t Sentinel = 0>
class Id {
 public:
  using value_type = std::uint64_t;
  using tag_type = Tag;

  constexpr Id() noexcept : value_(Sentinel) {}
  explicit constexpr Id(value_type v) noexcept : value_(v) {}

  static constexpr Id null() noexcept { return Id(); }
  static constexpr Id min_id() noexcept { return Id(1); }

  constexpr bool is_null() const noexcept { return value_ == Sentinel; }
  constexpr Id next() const noexcept { return Id(value_ + 1); }
  constexpr value_type value() const noexcept { return value_; }

  constexpr explicit operator bool() const noexcept { return value_ != Sentinel; }

  friend constexpr bool operator==(const Id& a, const Id& b) noexcept { return a.value_ == b.value_; }
  friend constexpr bool operator!=(const Id& a, const Id& b) noexcept { return !(a == b); }
  friend constexpr bool operator<(const Id& a, const Id& b) noexcept { return a.value_ < b.value_; }
  friend constexpr bool operator<=(const Id& a, const Id& b) noexcept { return a.value_ <= b.value_; }
  friend constexpr bool operator>(const Id& a, const Id& b) noexcept { return a.value_ > b.value_; }
  friend constexpr bool operator>=(const Id& a, const Id& b) noexcept { return a.value_ >= b.value_; }

  friend std::ostream& operator<<(std::ostream& os, const Id& id) { return os << id.value_; }
  friend struct std::hash<Id>;

 private:
  value_type value_;
};

// --------------------------------------------------------------------------- //
// Generation<Tag>: a strictly monotonic, strongly typed generation counter.
// --------------------------------------------------------------------------- //
template <typename Tag>
class Gen {
 public:
  using value_type = std::uint64_t;
  using tag_type = Tag;

  constexpr Gen() noexcept : value_(1) {}
  explicit constexpr Gen(value_type v) noexcept : value_(v) {}

  static constexpr Gen first() noexcept { return Gen(1); }
  static constexpr Gen null() noexcept { return Gen(0); }

  constexpr value_type value() const noexcept { return value_; }
  constexpr bool is_null() const noexcept { return value_ == 0; }
  constexpr explicit operator bool() const noexcept { return value_ != 0; }

  // Advance monotonically. Throws on exhaustion rather than wrapping, so a stale
  // generation can never become a fresh one reused as current authority.
  constexpr Gen next() const {
    if (value_ >= detail::kGenMax) {
      throw GenerationExhausted("generation exhausted");
    }
    return Gen(value_ + 1);
  }
  constexpr bool can_advance() const noexcept { return value_ < detail::kGenMax; }

  friend constexpr bool operator==(const Gen& a, const Gen& b) noexcept { return a.value_ == b.value_; }
  friend constexpr bool operator!=(const Gen& a, const Gen& b) noexcept { return !(a == b); }
  friend constexpr bool operator<(const Gen& a, const Gen& b) noexcept { return a.value_ < b.value_; }
  friend constexpr bool operator<=(const Gen& a, const Gen& b) noexcept { return a.value_ <= b.value_; }
  friend constexpr bool operator>(const Gen& a, const Gen& b) noexcept { return a.value_ > b.value_; }
  friend constexpr bool operator>=(const Gen& a, const Gen& b) noexcept { return a.value_ >= b.value_; }

  friend std::ostream& operator<<(std::ostream& os, const Gen& g) { return os << g.value_; }
  friend struct std::hash<Gen>;

 private:
  value_type value_;
};

// --------------------------------------------------------------------------- //
// Identity/generation tags (opaque; give each concept its own type).
// --------------------------------------------------------------------------- //
struct ServiceTag {};
struct ServiceShardTag {};
struct ServiceSlotTag {};
struct AssignmentTag {};
struct TargetTag {};
struct ReplicaTag {};
struct EngineTag {};
struct EngineIncarnationTag {};
struct ReadinessProfileTag {};
struct ActivationTag {};
struct WorkerTag {};
struct WorkerBootTag {};
struct SourceTag {};
struct SourceBootTag {};
struct FailureDomainTag {};
struct FailureEventTag {};
struct FailoverPlanTag {};
struct FailoverAttemptTag {};
struct PromotionTag {};
struct FenceTag {};
struct RouteTag {};
struct GatewayTag {};
struct GatewayBootTag {};
struct CoordinatorTag {};
struct RegistrationPermitTag {};
struct StateTag {};
struct SessionTag {};
struct RequestTag {};
struct ExecutionTag {};
struct CompatibilityTag {};
struct CapacityTag {};
struct ResourceClaimTag {};
struct ReservationTag {};
struct DependencyTag {};
struct ServiceConfigTag {};
struct ServiceAuthorityTag {};
struct ReadinessGenerationTag {};
struct HealthGenerationTag {};
struct CandidateSnapshotTag {};
struct CutoverTag {};
struct VerificationTag {};
struct RecoveryTag {};
struct RevalidationTag {};
struct FailbackTag {};
struct PolicyTag {};
struct SloTag {};
struct CheckpointTag {};

// --------------------------------------------------------------------------- //
// Identity aliases.
// --------------------------------------------------------------------------- //
using ServiceId = Id<ServiceTag>;
using ServiceShardId = Id<ServiceShardTag>;
using ServiceSlotId = Id<ServiceSlotTag>;
using AssignmentId = Id<AssignmentTag>;
using TargetId = Id<TargetTag>;
using ReplicaId = Id<ReplicaTag>;
using EngineId = Id<EngineTag>;
using EngineIncarnationId = Id<EngineIncarnationTag>;
using ReadinessProfileId = Id<ReadinessProfileTag>;
using ActivationId = Id<ActivationTag>;
using WorkerId = Id<WorkerTag>;
using WorkerBootId = Id<WorkerBootTag>;
using SourceId = Id<SourceTag>;
using SourceBootId = Id<SourceBootTag>;
using FailureDomainId = Id<FailureDomainTag>;
using FailureEventId = Id<FailureEventTag>;
using FailoverPlanId = Id<FailoverPlanTag>;
using FailoverAttemptId = Id<FailoverAttemptTag>;
using PromotionId = Id<PromotionTag>;
using FenceId = Id<FenceTag>;
using RouteId = Id<RouteTag>;
using GatewayId = Id<GatewayTag>;
using GatewayBootId = Id<GatewayBootTag>;
using CoordinatorId = Id<CoordinatorTag>;
using RegistrationPermitId = Id<RegistrationPermitTag>;
using StateId = Id<StateTag>;
using SessionId = Id<SessionTag>;
using RequestId = Id<RequestTag>;
using ExecutionId = Id<ExecutionTag>;
using CompatibilityId = Id<CompatibilityTag>;
using CapacityId = Id<CapacityTag>;
using ResourceClaimId = Id<ResourceClaimTag>;
using ReservationId = Id<ReservationTag>;
using DependencyId = Id<DependencyTag>;

// --------------------------------------------------------------------------- //
// Generation aliases.
// --------------------------------------------------------------------------- //
using ServiceGeneration = Gen<ServiceTag>;
  using FailureDomainGeneration = Gen<FailureDomainTag>;
using ServiceConfigGeneration = Gen<ServiceConfigTag>;
using AssignmentGeneration = Gen<AssignmentTag>;
using ServiceAuthorityGeneration = Gen<ServiceAuthorityTag>;
using TargetGeneration = Gen<TargetTag>;
using ReplicaGeneration = Gen<ReplicaTag>;
using EngineGeneration = Gen<EngineTag>;
using ReadinessProfileGeneration = Gen<ReadinessProfileTag>;
using ReadinessGeneration = Gen<ReadinessGenerationTag>;
using ActivationGeneration = Gen<ActivationTag>;
using FailureGeneration = Gen<FailureEventTag>;
using HealthGeneration = Gen<HealthGenerationTag>;
using CandidateSnapshotGeneration = Gen<CandidateSnapshotTag>;
using FailoverPlanGeneration = Gen<FailoverPlanTag>;
using FailoverAttemptGeneration = Gen<FailoverAttemptTag>;
using PromotionGeneration = Gen<PromotionTag>;
using FenceGeneration = Gen<FenceTag>;
using RouteGeneration = Gen<RouteTag>;
using CutoverGeneration = Gen<CutoverTag>;
using VerificationGeneration = Gen<VerificationTag>;
using RecoveryGeneration = Gen<RecoveryTag>;
using RevalidationGeneration = Gen<RevalidationTag>;
using FailbackGeneration = Gen<FailbackTag>;
using PolicyGeneration = Gen<PolicyTag>;
using SloGeneration = Gen<SloTag>;
using CapacityGeneration = Gen<CapacityTag>;
using ResourceClaimGeneration = Gen<ResourceClaimTag>;
using ReservationGeneration = Gen<ReservationTag>;
using CompatibilityGeneration = Gen<CompatibilityTag>;
using DependencyGeneration = Gen<DependencyTag>;
using StateGeneration = Gen<StateTag>;
using CheckpointGeneration = Gen<CheckpointTag>;
using SessionGeneration = Gen<SessionTag>;
using ExecutionGeneration = Gen<ExecutionTag>;

// --------------------------------------------------------------------------- //
// std::hash specializations.
// --------------------------------------------------------------------------- //
}  // namespace failover_fabric

namespace std {
template <typename Tag, std::uint64_t S>
struct hash<failover_fabric::Id<Tag, S>> {
  std::size_t operator()(const failover_fabric::Id<Tag, S>& id) const noexcept {
    return std::hash<std::uint64_t>{}(id.value());
  }
};
template <typename Tag>
struct hash<failover_fabric::Gen<Tag>> {
  std::size_t operator()(const failover_fabric::Gen<Tag>& g) const noexcept {
    return std::hash<std::uint64_t>{}(g.value());
  }
};
}  // namespace std