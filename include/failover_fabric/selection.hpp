// selection.hpp — Hard eligibility and deterministic replacement selection.
//
// Hard exclusions run before ranking. UNKNOWN never silently becomes eligible; the exact
// domain/constraint that blocked a candidate is reported. Ranking uses named factors (no
// opaque hidden score), and ties break deterministically.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/candidate.hpp"
#include "failover_fabric/domains.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

// Context the selection engine needs to make decisions that depend on the coordinator's
// view of the world (evidence, fences, route authority) rather than only the candidate.
struct SelectionContext {
  TargetId failed_target{TargetId::null()};
  std::vector<TargetId> confirmed_failed_targets;   // from evidence store
  std::vector<WorkerBootId> fenced_boots;            // boots whose authority is revoked
  std::vector<FailureDomainId> failed_domains;       // domains excluded by failure evidence
  std::optional<RouteGeneration> current_route_gen;  // for stale-route precondition
  bool requires_fresh_readiness{true};
  std::uint64_t now_receipt_seq{0};                   // freshness baseline
};

// Named ranking factors. A higher weighted score is preferred.
struct RankingFactor {
  std::string name;
  double weight{1.0};
  double normalized_value{0.0};   // in [0,1]   (0 = worst, 1 = best)
  std::string note;
};

struct SelectionResult {
  bool has_selection{false};
  std::optional<Candidate> selected;
  std::vector<Candidate> evaluated_alternatives;  // hard-eligible but not chosen
  std::vector<std::pair<TargetId, std::pair<ExclusionReason, std::string>>> exclusions;
  std::vector<RankingFactor> factors;
  std::vector<std::string> cost_components;
  Provenance provenance{Provenance::DERIVED};
  double uncertainty{0.0};
  PolicyGeneration policy_gen;
  CandidateSnapshotGeneration snapshot_gen;
};

class SelectionEngine {
 public:
  static const double kDomainIndependenceWeight;
  static const double kPreparationWeight;
  static const double kContinuityWeight;
  static const double kStateFreshnessWeight;
  static const double kCapacityWeight;
  static const double kResourceOverlapWeight;
  static const double kPriorityWeight;
  static const double kPrepCostWeight;
  static const double kRoutingCostWeight;
  static const double kPolicyPreferenceWeight;

  // Apply hard exclusions. Returns true if the candidate may be ranked; false + reason
  // otherwise. Failed details are recorded on the candidate.
  bool hard_eligible(Candidate& c, const ServiceDefinition& svc, const DomainRegistry& domains,
                     const SelectionContext& ctx) const;

  SelectionResult select(const CandidateSnapshot& snap, const ServiceDefinition& svc,
                         const DomainRegistry& domains, const SelectionContext& ctx) const;

  static std::uint64_t rank(const Candidate& c, const ServiceDefinition& svc, const SelectionContext& ctx);
};

}  // namespace failover_fabric
