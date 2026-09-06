// failover_fabric.hpp — The FailoverFabric runtime facade.
//
// This is the public API. The core library is fully usable on its own: service
// definitions, candidate registration, evidence, domain eligibility, deterministic
// selection, planning, the transaction state machine, and persistence all work without
// the reference coordinator or gateway. Multiprocess coordination is layered on top.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/identities.hpp"
#include "failover_fabric/types.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/evidence.hpp"
#include "failover_fabric/domains.hpp"
#include "failover_fabric/candidate.hpp"
#include "failover_fabric/selection.hpp"
#include "failover_fabric/recovery.hpp"
#include "failover_fabric/authority.hpp"
#include "failover_fabric/fence.hpp"
#include "failover_fabric/route.hpp"
#include "failover_fabric/request.hpp"
#include "failover_fabric/plan.hpp"
#include "failover_fabric/cutover.hpp"

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// Config / resource discipline (bound every dynamic structure).
// --------------------------------------------------------------------------- //
struct RuntimeConfig {
  std::size_t max_services{10000};
  std::size_t max_slots_per_service{1024};
  std::size_t max_slots{20000};
  std::size_t max_targets{20000};
  std::size_t max_candidates_per_snapshot{4096};
  std::size_t max_domains{10000};
  std::size_t max_memberships{100000};
  std::size_t max_failure_history{10000};
  std::size_t max_requests{65536};
  std::size_t max_attempt_history{4096};
  std::size_t max_route_history{64};
  std::size_t max_explanations{4096};
  bool strict{true};
  bool bounds_checked{true};
};

// --------------------------------------------------------------------------- //
// Admission limit exceeded.
// --------------------------------------------------------------------------- //
class CapacityExceeded : public std::runtime_error {
 public:
  explicit CapacityExceeded(std::string what) : std::runtime_error(std::move(what)) {}
};

// --------------------------------------------------------------------------- //
// Result of attempting an execution.
// --------------------------------------------------------------------------- //
struct AttemptResult {
  FailoverAttemptId attempt{FailoverAttemptId::null()};
  FailoverAttemptGeneration generation;
  AttemptState state{AttemptState::REQUESTED};
  Milestone reached{Milestone::INTENT_ONLY};
  std::optional<Assignment> committed_assignment;
  std::string detail;
  std::vector<StageEstimate> stage_durations;
  std::uint64_t elapsed_ms{0};
  bool recovery_verified{false};
  bool violated_recovery_objective{false};
};

// --------------------------------------------------------------------------- //
// FailoverFabric: the authoritative core.
// --------------------------------------------------------------------------- //
class FailoverFabric {
 public:
  FailoverFabric();
  explicit FailoverFabric(RuntimeConfig cfg);
  ~FailoverFabric();
  FailoverFabric(FailoverFabric&&) noexcept;
  FailoverFabric& operator=(FailoverFabric&&) noexcept;

  // --- control plane (coordinator identity, gateway boot) --------------------- //
  void set_coordinator_identity(CoordinatorId id, std::uint64_t epoch);
  CoordinatorEpoch current_epoch() const noexcept;
  void set_gateway_boot(GatewayBootId boot);
  GatewayBootId gateway_boot() const noexcept;

  // --- service contract ----------------------------------------------------- //
  void create_service(ServiceDefinition def);
  std::optional<ServiceDefinition> get_service(ServiceId service) const;
  std::vector<ServiceDefinition> services() const;
  std::vector<ServiceSlotKey> slots(ServiceId service) const;

  // --- initial / current assignment ----------------------------------------- //
  void set_initial_assignment(Assignment a);
  std::optional<Assignment> current_assignment(ServiceSlotKey slot) const;
  std::vector<Assignment> assignment_history(ServiceSlotKey slot, std::size_t max) const;

  // --- domains -------------------------------------------------------------- //
  void declare_domain(DomainDecl d);
  void set_membership(DomainMembership m);
  const DomainRegistry& domains() const noexcept;

  // --- candidate registration ----------------------------------------------- //
  void register_target(TargetId id, std::string transport);
  void publish_candidate_facts(CandidateFacts f);
  void publish_candidate_state(TargetId t, CandidateState s);
  void publish_candidate_resources(TargetId t, std::vector<CandidateResource> r);

  // --- failure evidence ------------------------------------------------------ //
  void publish_failure(FailureEvent ev);
  void clear_failure(FailureEventId id);
  std::vector<FailureEvent> failures_for(TargetId t, std::size_t max) const;

  // --- selection ------------------------------------------------------------- //
  CandidateSnapshot snapshot(ServiceSlotKey slot) const;
  SelectionResult select(ServiceSlotKey slot) const;
  std::vector<Candidate> eligible_candidates(ServiceSlotKey slot) const;

  // --- planning / execution --------------------------------------------------- //
  std::optional<FailoverPlan> plan_failover(ServiceSlotKey slot);
  AttemptResult execute_plan(const FailoverPlan& plan, CutoverOps& ops);

  // --- gateway authorization (reference correctness) -------------------------- //
  GateDecision authorize_dispatch(const WorkerAuthorization& auth) const;
  GateDecision authorize_result(const WorkerAuthorization& auth) const;

  // --- requests --------------------------------------------------------------- //
  void record_request(RequestRecord r);
  RequestTracker::LateOutcome classify_late_result(RequestId id, const WorkerAuthorization& auth);
  std::size_t ambiguous_count() const noexcept;
  std::size_t rejected_late_count() const noexcept;

  // --- routes ------------------------------------------------------------------ //
  bool install_route(RouteEntry entry, GatewayBootId boot);
  std::optional<RouteEntry> current_route(ServiceSlotKey slot) const;
  std::vector<RouteEntry> route_history(ServiceSlotKey slot, std::size_t max) const;
  void mark_routes_revalidation();
  std::size_t revalidation_required_count() const noexcept;

  // --- introspection ------------------------------------------------------------ //
  ServiceOutcome service_outcome(ServiceSlotKey slot) const;
  std::vector<AttemptState> attempt_history() const;

  // --- persistence ---------------------------------------------------------------- //
  void save(const std::string& path) const;
  void load(const std::string& path);   // throws std::runtime_error on corruption
  static std::vector<std::string> validate_file(const std::string& path);

 private:
  struct Impl;
  GateDecision dispatch_allowed_locked(const WorkerAuthorization& auth) const;
  std::unique_ptr<Impl> impl_;
};

}  // namespace failover_fabric
