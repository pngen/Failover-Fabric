// types.hpp — Shared enums, provenance, time, and small value types.
//
// These are the vocabulary of the runtime: evidence status, provenance, service
// outcome, attempt lifecycle, assignment lifecycle, continuity class, domain class,
// exclusion reasons, request disposition, failback policy, and anti-flapping policy.
#pragma once

#include <cstdint>
#include <chrono>
#include <cmath>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "failover_fabric/identities.hpp"

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// Evidence status
// --------------------------------------------------------------------------- //
enum class EvidenceStatus : std::uint8_t {
  SUSPECTED,        // suspicion only; not confirmed under an observation mechanism
  CONFIRMED,        // observed under the stated mechanism
  CLEARED,          // failure cleared; does not erase a newer failure
  STALE,            // too old / superseded under the freshness policy
  UNKNOWN           // cannot be classified
};

const char* to_string(EvidenceStatus s) noexcept;

// --------------------------------------------------------------------------- //
// Failure evidence categories
// --------------------------------------------------------------------------- //
enum class FailureCategory : std::uint8_t {
  PROCESS_EXIT,
  TRANSPORT_DISCONNECT,
  HEARTBEAT_EXPIRATION,
  BACKEND_EXECUTION_FAILURE,
  READINESS_REVOKED,
  DEVICE_UNAVAILABLE,
  NODE_UNAVAILABLE,
  RESOURCE_CLAIM_REVOKED,
  DEPENDENCY_INVALIDATION,
  ADMIN_FAILOVER,
  PLANNED_MAINTENANCE,
  DEGRADED_HEALTH,
  UNKNOWN_FAILURE
};

const char* to_string(FailureCategory c) noexcept;

// --------------------------------------------------------------------------- //
// Provenance
// --------------------------------------------------------------------------- //
enum class Provenance : std::uint8_t {
  MEASURED,        // directly observed by this runtime
  REPORTED,        // reported by a trusted source
  DERIVED,         // derived from other observations
  ESTIMATED,       // estimated, not directly observed
  SYNTHETIC,       // synthetic / injected (testing)
  RECONSTRUCTED,   // reconstructed from durable records
  UNKNOWN
};

const char* to_string(Provenance p) noexcept;

// --------------------------------------------------------------------------- //
// Service outcome
// --------------------------------------------------------------------------- //
enum class ServiceOutcome : std::uint8_t {
  UNKNOWN,
  AVAILABLE,
  DEGRADED,
  UNAVAILABLE,
  RECOVERING,
  REVALIDATION_REQUIRED
};

const char* to_string(ServiceOutcome o) noexcept;

// --------------------------------------------------------------------------- //
// Failover attempt lifecycle
// --------------------------------------------------------------------------- //
enum class AttemptState : std::uint8_t {
  REQUESTED,
  EVALUATING,
  BLOCKED,
  PREPARING,
  FENCING,
  PROMOTING,
  CUTTING_OVER,
  VERIFYING,
  COMPLETED,
  FAILED,
  CANCELLED,
  SUPERSEDED,
  RECOVERY_REQUIRED
};

const char* to_string(AttemptState s) noexcept;

// --------------------------------------------------------------------------- //
// Assignment lifecycle
// --------------------------------------------------------------------------- //
enum class AssignmentState : std::uint8_t {
  CANDIDATE,
  PREPARED,
  AUTHORIZED,
  ACTIVE,
  DRAINING,
  FENCED,
  RETIRED,
  REVALIDATION_REQUIRED
};

const char* to_string(AssignmentState s) noexcept;

// --------------------------------------------------------------------------- //
// Continuity class
// --------------------------------------------------------------------------- //
enum class ContinuityClass : std::uint8_t {
  STATELESS_RESTART,
  REPLAY_SAFE_REQUESTS,
  CHECKPOINT_RESTORE,
  SESSION_STATE_REBIND,
  SESSION_RESTART_REQUIRED,
  MANUAL_RECOVERY_REQUIRED,
  UNKNOWN
};

const char* to_string(ContinuityClass c) noexcept;

// --------------------------------------------------------------------------- //
// Failure domain class
// --------------------------------------------------------------------------- //
enum class DomainClass : std::uint8_t {
  PROCESS,
  DEVICE,
  HOST,
  RACK,
  ZONE,
  REGION,
  POWER,
  NETWORK,
  CUSTOM,
  UNKNOWN
};

const char* to_string(DomainClass d) noexcept;

// --------------------------------------------------------------------------- //
// Candidate exclusion reasons
// --------------------------------------------------------------------------- //
enum class ExclusionReason : std::uint8_t {
  NO_ELIGIBLE_TARGET,
  STALE_READINESS,
  INCOMPATIBLE_PROFILE,
  FAILED_DOMAIN,
  DOMAIN_INDEPENDENCE_UNKNOWN,
  INSUFFICIENT_CAPACITY,
  COMMITMENT_CONFLICT,
  STATE_UNAVAILABLE,
  STATE_TOO_OLD,
  FENCING_UNPROVEN,
  ROUTE_AUTHORITY_UNAVAILABLE,
  RECOVERY_BUDGET_INFEASIBLE,
  REVALIDATION_REQUIRED,
  STALE_AUTHORITY,
  FENCED_BOOT,
  UNKNOWN_REQUIRED_EVIDENCE,
  RESOURCES_REVOKED,
  UNREADY_ENGINE,
  UNSUPPORTED_CONTINUITY,
  POLICY_PROHIBITED_DEGRADATION,
  STALE_ROUTE_PRECONDITION
};

const char* to_string(ExclusionReason r) noexcept;

// --------------------------------------------------------------------------- //
// Request disposition (the request's own lifecycle)
// --------------------------------------------------------------------------- //
enum class RequestDisposition : std::uint8_t {
  NOT_DISPATCHED,
  DISPATCHED,
  ACCEPTED_BY_TARGET,
  CONFIRMED_COMPLETE,
  RESPONSE_COMMITTED,
  FAILED,
  OUTCOME_UNKNOWN
};

const char* to_string(RequestDisposition d) noexcept;

// --------------------------------------------------------------------------- //
// Failback policy
// --------------------------------------------------------------------------- //
enum class FailbackPolicy : std::uint8_t {
  MANUAL,
  STAY_ON_CURRENT,
  RETURN_AFTER_VALIDATION
};

const char* to_string(FailbackPolicy p) noexcept;

// --------------------------------------------------------------------------- //
// Route state
// --------------------------------------------------------------------------- //
enum class RouteState : std::uint8_t {
  EMPTY,
  PENDING_INSTALL,
  INSTALLED,
  ACKNOWLEDGED,
  REVALIDATION_REQUIRED,
  STALE
};

const char* to_string(RouteState s) noexcept;

// --------------------------------------------------------------------------- //
// Do not silently relax: this guards SLO / continuity checks.
// --------------------------------------------------------------------------- //
// --------------------------------------------------------------------------- //
// Time: an injectable monotonic clock for local elapsed-time policy.
// --------------------------------------------------------------------------- //
class Clock {
 public:
  using duration = std::chrono::nanoseconds;
  using time_point = std::chrono::steady_clock::time_point;
  virtual ~Clock() = default;
  virtual time_point now() const noexcept = 0;
};

class SteadyClock final : public Clock {
 public:
  time_point now() const noexcept override { return std::chrono::steady_clock::now(); }
};

// Callback clock for tests (advance under control).
class ManualClock final : public Clock {
 public:
  time_point now() const noexcept override { return base_ + elapsed_; }
  void advance(duration d) noexcept { elapsed_ += d; }
  static ManualClock& instance() {
    static ManualClock c;
    return c;
  }
 private:
  time_point base_ = std::chrono::steady_clock::now();
  duration elapsed_{0};
};

// A wall-clock timestamp (for durable records) plus a clock-domain label. Product
// semantics must not be compared across process incarnations from monotonic clocks.
struct Timestamp {
  std::int64_t wall_ns{0};   // wall clock, epoch phase / UTC-based
  std::uint64_t seq{0};      // per-source monotonic observation sequence
  bool has_wall() const noexcept { return wall_ns != 0; }
  friend bool operator==(const Timestamp& a, const Timestamp& b) noexcept {
    return a.wall_ns == b.wall_ns && a.seq == b.seq;
  }
};

// --------------------------------------------------------------------------- //
// Severity and confidence
// --------------------------------------------------------------------------- //
enum class Severity : std::uint8_t { LOW, MEDIUM, HIGH, CRITICAL };
const char* to_string(Severity s) noexcept;

struct BoundedF64 {
  double value{0.0};
  static BoundedF64 finite(double v) {
    if (!std::isfinite(v)) throw std::invalid_argument("non-finite value");
    return BoundedF64{v};
  }
  static BoundedF64 bounded(double v, double lo, double hi) {
    if (!std::isfinite(v) || v < lo || v > hi) throw std::invalid_argument("value out of bounds");
    return BoundedF64{v};
  }
};

}  // namespace failover_fabric
