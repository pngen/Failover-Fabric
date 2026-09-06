// recovery.hpp — Recovery objectives, stage estimates, and objective evaluation.
//
// RTO has an explicit clock origin (first accepted failure observation, explicit
// failover request, or a documented service-contract anchor). Recovery is complete only
// at a defined, verified boundary. A missed recovery budget never forces unsafe promotion.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/candidate.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

enum class RecoveryStage : std::uint8_t {
  FAILURE_OBSERVATION,
  CANDIDATE_EVALUATION,
  ENGINE_PREPARATION,
  STATE_RESTORE_OR_REBIND,
  FENCING,
  ACTIVATION,
  ROUTE_INSTALLATION,
  VERIFICATION
};
const char* to_string(RecoveryStage s) noexcept;

// A stage may be parallel with others; the estimate tracks a critical path rather than
// blindly summing overlapping stages.
struct StageEstimate {
  RecoveryStage stage;
  std::uint64_t estimated_ms{0};
  bool measured{false};
  std::uint64_t measured_ms{0};
  std::vector<RecoveryStage> depends_on;  // stages inside this one
};

struct RecoveryEstimate {
  std::vector<StageEstimate> stages;
  std::uint64_t critical_path_ms{0};
  bool all_stages_measured{false};
};

// Evaluate whether the achieved/estimated recovery meets the contract. The exact
// recovery-complete milestone (e.g. verified routed work on the new target) is
// defined by the caller of recovery_complete().
struct RecoveryEvaluation {
  bool satisfied{false};
  bool rto_met{false};
  bool rpo_met{false};
  bool verified{false};
  std::uint64_t elapsed_ms{0};
  std::uint64_t state_loss_sequences{0};
  std::string note;
};

// --------------------------------------------------------------------------- //
// Recovery objective evaluation.
// --------------------------------------------------------------------------- //
struct RecoveryEvaluator {
  static RecoveryEvaluation evaluate(const RecoveryObjective& obj, const RecoveryEstimate& est,
                                     std::uint64_t elapsed_ms, std::uint64_t state_loss_sequences,
                                     bool verified);
  static bool continuity_compatible(const ContinuityClass required, const ContinuityClass candidate) noexcept;
};

}  // namespace failover_fabric
