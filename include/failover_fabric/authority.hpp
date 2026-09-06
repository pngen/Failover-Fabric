// authority.hpp — Coordinator epoch and the generation-bound authority gate.
//
// The gate is the single place that decides whether a dispatch or a result is allowed.
// Workers DO NOT obtain unlimited future serving authority from an old activation; they
// get bounded per-request authorization bound to slot, assignment generation,
// coordinator epoch, target incarnation, request/attempt, route generation and the
// permitted operation. It is validated at admission and at authoritative result
// acceptance. A stale coordinator epoch or a stale boot never authorizes.
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "failover_fabric/identities.hpp"
#include "failover_fabric/service.hpp"

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// Coordinator epoch: durable, monotonic, never reused.
// --------------------------------------------------------------------------- //
class CoordinatorEpoch {
 public:
  CoordinatorEpoch() noexcept = default;
  explicit CoordinatorEpoch(std::uint64_t epoch, CoordinatorId id, CoordinatorId boot = CoordinatorId::null())
      : epoch_(epoch), id_(id), boot_(boot) {}

  std::uint64_t value() const noexcept { return epoch_; }
  CoordinatorId id() const noexcept { return id_; }
  CoordinatorId boot() const noexcept { return boot_; }

  CoordinatorEpoch next() const {
    if (epoch_ == std::numeric_limits<std::uint64_t>::max()) throw GenerationExhausted("coordinator epoch exhausted");
    return CoordinatorEpoch(epoch_ + 1, id_, boot_);
  }

  friend bool operator==(const CoordinatorEpoch& a, const CoordinatorEpoch& b) noexcept {
    return a.epoch_ == b.epoch_ && a.id_ == b.id_ && a.boot_ == b.boot_;
  }
  friend bool operator!=(const CoordinatorEpoch& a, const CoordinatorEpoch& b) noexcept { return !(a == b); }
  friend bool operator<(const CoordinatorEpoch& a, const CoordinatorEpoch& b) noexcept { return a.epoch_ < b.epoch_; }

 private:
  std::uint64_t epoch_{0};
  CoordinatorId id_{CoordinatorId::null()};
  CoordinatorId boot_{CoordinatorId::null()};
};

std::ostream& operator<<(std::ostream& os, const CoordinatorEpoch& e);

// --------------------------------------------------------------------------- //
// Authorization: a bounded per-request grant to a worker.
// --------------------------------------------------------------------------- //
struct WorkerAuthorization {
  CoordinatorEpoch epoch;
  ServiceSlotKey slot;
  AssignmentId assignment{AssignmentId::null()};
  AssignmentGeneration assignment_generation;
  RouteGeneration route_generation;
  EngineIncarnationId incarnation{EngineIncarnationId::null()};
  RequestId request{RequestId::null()};
  ExecutionId execution{ExecutionId::null()};
  std::string operation;   // e.g. "serve", "verify"
  WorkerBootId worker_boot{WorkerBootId::null()};

  friend bool operator==(const WorkerAuthorization& a, const WorkerAuthorization& b) noexcept {
    return a.epoch == b.epoch && a.slot == b.slot && a.assignment == b.assignment &&
           a.request == b.request && a.execution == b.execution && a.worker_boot == b.worker_boot &&
           a.route_generation == b.route_generation && a.incarnation == b.incarnation;
  }
};

// --------------------------------------------------------------------------- //
// The authority gate: validates that a dispatch and a result are current.
// --------------------------------------------------------------------------- //
struct GateDecision {
  bool allowed{false};
  std::string reason;
  bool fail_closed{false};
};

class AuthorityGate {
 public:
  // Decide whether a dispatched request carrying `auth` is current. `current` is the
  // live authority oracle (supplied by the coordinator). fail closed on out-of-date.
  GateDecision authorize_dispatch(const WorkerAuthorization& auth) const;

  // Decide whether a result from a worker is accepted as authoritative for `auth`.
  GateDecision authorize_result(const WorkerAuthorization& auth) const;

  void set_authority(std::function<std::optional<WorkerAuthorization>(RequestId)> oracle);

 private:
  std::function<std::optional<WorkerAuthorization>(RequestId)> oracle_;
};

}  // namespace failover_fabric
