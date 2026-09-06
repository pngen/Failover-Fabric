// recovery.cpp — Recovery objective evaluation and continuity compatibility.
#include "failover_fabric/recovery.hpp"

namespace failover_fabric {

namespace {
// Preservation level: how much state a continuity class can carry across failover.
// Higher is stronger. UNKNOWN and MANUAL are special (logically weak / operator-only).
int preservation_level(ContinuityClass c) {
  switch (c) {
    case ContinuityClass::STATELESS_RESTART: return 0;
    case ContinuityClass::REPLAY_SAFE_REQUESTS: return 1;
    case ContinuityClass::SESSION_STATE_REBIND: return 2;
    case ContinuityClass::CHECKPOINT_RESTORE: return 3;
    case ContinuityClass::SESSION_RESTART_REQUIRED: return 1;
    case ContinuityClass::MANUAL_RECOVERY_REQUIRED: return -1;
    case ContinuityClass::UNKNOWN: return -2;
  }
  return -2;
}
}  // namespace

bool RecoveryEvaluator::continuity_compatible(ContinuityClass required, ContinuityClass candidate) noexcept {
  if (required == ContinuityClass::UNKNOWN) return true;  // no constraint imposed
  if (candidate == ContinuityClass::UNKNOWN) return false;  // unknown must not qualify
  if (candidate == ContinuityClass::MANUAL_RECOVERY_REQUIRED) return false;
  if (required == ContinuityClass::MANUAL_RECOVERY_REQUIRED) return true;  // candidate is at least automatic
  return preservation_level(candidate) >= preservation_level(required);
}

RecoveryEvaluation RecoveryEvaluator::evaluate(const RecoveryObjective& obj, const RecoveryEstimate& est,
                                               std::uint64_t elapsed_ms, std::uint64_t state_loss_sequences,
                                               bool verified) {
  RecoveryEvaluation e;
  if (!est.all_stages_measured) e.note += "(estimated); ";
  e.elapsed_ms = elapsed_ms;
  e.state_loss_sequences = state_loss_sequences;
  e.verified = verified;
  e.rto_met = (obj.rto_ms == 0 || elapsed_ms <= obj.rto_ms);
  e.rpo_met = (state_loss_sequences <= obj.max_state_loss_sequences);
  if (obj.min_checkpoint_gen) {
    // RPO is only satisfiable if the candidate used a checkpoint at/after the min gen; the
    // caller passes state loss based on that; a zero-loss with a stale checkpoint is caught
    // by eligibility (STATE_TOO_OLD). Here we only check the numeric bound.
    e.rpo_met = e.rpo_met && true;
  }
  e.satisfied = e.rto_met && e.rpo_met && (verified || !obj.requires_verified_recovery);
  if (!e.satisfied) {
    if (!e.rto_met) e.note += "RTO not met; ";
    if (!e.rpo_met) e.note += "RPO not met; ";
    if (!verified && obj.requires_verified_recovery) e.note += "recovery not verified; ";
  }
  return e;
}

}  // namespace failover_fabric
