// types.cpp — to_string implementations for shared enums.
#include "failover_fabric/types.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/recovery.hpp"
#include "failover_fabric/fence.hpp"
#include "failover_fabric/request.hpp"
#include "failover_fabric/plan.hpp"

namespace failover_fabric {

const char* to_string(EvidenceStatus s) noexcept {
  switch (s) {
    case EvidenceStatus::SUSPECTED: return "SUSPECTED";
    case EvidenceStatus::CONFIRMED: return "CONFIRMED";
    case EvidenceStatus::CLEARED: return "CLEARED";
    case EvidenceStatus::STALE: return "STALE";
    case EvidenceStatus::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

const char* to_string(FailureCategory c) noexcept {
  switch (c) {
    case FailureCategory::PROCESS_EXIT: return "PROCESS_EXIT";
    case FailureCategory::TRANSPORT_DISCONNECT: return "TRANSPORT_DISCONNECT";
    case FailureCategory::HEARTBEAT_EXPIRATION: return "HEARTBEAT_EXPIRATION";
    case FailureCategory::BACKEND_EXECUTION_FAILURE: return "BACKEND_EXECUTION_FAILURE";
    case FailureCategory::READINESS_REVOKED: return "READINESS_REVOKED";
    case FailureCategory::DEVICE_UNAVAILABLE: return "DEVICE_UNAVAILABLE";
    case FailureCategory::NODE_UNAVAILABLE: return "NODE_UNAVAILABLE";
    case FailureCategory::RESOURCE_CLAIM_REVOKED: return "RESOURCE_CLAIM_REVOKED";
    case FailureCategory::DEPENDENCY_INVALIDATION: return "DEPENDENCY_INVALIDATION";
    case FailureCategory::ADMIN_FAILOVER: return "ADMIN_FAILOVER";
    case FailureCategory::PLANNED_MAINTENANCE: return "PLANNED_MAINTENANCE";
    case FailureCategory::DEGRADED_HEALTH: return "DEGRADED_HEALTH";
    case FailureCategory::UNKNOWN_FAILURE: return "UNKNOWN_FAILURE";
  }
  return "UNKNOWN_FAILURE";
}

const char* to_string(Provenance p) noexcept {
  switch (p) {
    case Provenance::MEASURED: return "MEASURED";
    case Provenance::REPORTED: return "REPORTED";
    case Provenance::DERIVED: return "DERIVED";
    case Provenance::ESTIMATED: return "ESTIMATED";
    case Provenance::SYNTHETIC: return "SYNTHETIC";
    case Provenance::RECONSTRUCTED: return "RECONSTRUCTED";
    case Provenance::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

const char* to_string(ServiceOutcome o) noexcept {
  switch (o) {
    case ServiceOutcome::UNKNOWN: return "UNKNOWN";
    case ServiceOutcome::AVAILABLE: return "AVAILABLE";
    case ServiceOutcome::DEGRADED: return "DEGRADED";
    case ServiceOutcome::UNAVAILABLE: return "UNAVAILABLE";
    case ServiceOutcome::RECOVERING: return "RECOVERING";
    case ServiceOutcome::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
  }
  return "UNKNOWN";
}

const char* to_string(AttemptState s) noexcept {
  switch (s) {
    case AttemptState::REQUESTED: return "REQUESTED";
    case AttemptState::EVALUATING: return "EVALUATING";
    case AttemptState::BLOCKED: return "BLOCKED";
    case AttemptState::PREPARING: return "PREPARING";
    case AttemptState::FENCING: return "FENCING";
    case AttemptState::PROMOTING: return "PROMOTING";
    case AttemptState::CUTTING_OVER: return "CUTTING_OVER";
    case AttemptState::VERIFYING: return "VERIFYING";
    case AttemptState::COMPLETED: return "COMPLETED";
    case AttemptState::FAILED: return "FAILED";
    case AttemptState::CANCELLED: return "CANCELLED";
    case AttemptState::SUPERSEDED: return "SUPERSEDED";
    case AttemptState::RECOVERY_REQUIRED: return "RECOVERY_REQUIRED";
  }
  return "UNKNOWN";
}

const char* to_string(AssignmentState s) noexcept {
  switch (s) {
    case AssignmentState::CANDIDATE: return "CANDIDATE";
    case AssignmentState::PREPARED: return "PREPARED";
    case AssignmentState::AUTHORIZED: return "AUTHORIZED";
    case AssignmentState::ACTIVE: return "ACTIVE";
    case AssignmentState::DRAINING: return "DRAINING";
    case AssignmentState::FENCED: return "FENCED";
    case AssignmentState::RETIRED: return "RETIRED";
    case AssignmentState::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
  }
  return "UNKNOWN";
}

const char* to_string(ContinuityClass c) noexcept {
  switch (c) {
    case ContinuityClass::STATELESS_RESTART: return "STATELESS_RESTART";
    case ContinuityClass::REPLAY_SAFE_REQUESTS: return "REPLAY_SAFE_REQUESTS";
    case ContinuityClass::CHECKPOINT_RESTORE: return "CHECKPOINT_RESTORE";
    case ContinuityClass::SESSION_STATE_REBIND: return "SESSION_STATE_REBIND";
    case ContinuityClass::SESSION_RESTART_REQUIRED: return "SESSION_RESTART_REQUIRED";
    case ContinuityClass::MANUAL_RECOVERY_REQUIRED: return "MANUAL_RECOVERY_REQUIRED";
    case ContinuityClass::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

const char* to_string(DomainClass d) noexcept {
  switch (d) {
    case DomainClass::PROCESS: return "PROCESS";
    case DomainClass::DEVICE: return "DEVICE";
    case DomainClass::HOST: return "HOST";
    case DomainClass::RACK: return "RACK";
    case DomainClass::ZONE: return "ZONE";
    case DomainClass::REGION: return "REGION";
    case DomainClass::POWER: return "POWER";
    case DomainClass::NETWORK: return "NETWORK";
    case DomainClass::CUSTOM: return "CUSTOM";
    case DomainClass::UNKNOWN: return "UNKNOWN";
  }
  return "UNKNOWN";
}

const char* to_string(ExclusionReason r) noexcept {
  switch (r) {
    case ExclusionReason::NO_ELIGIBLE_TARGET: return "NO_ELIGIBLE_TARGET";
    case ExclusionReason::STALE_READINESS: return "STALE_READINESS";
    case ExclusionReason::INCOMPATIBLE_PROFILE: return "INCOMPATIBLE_PROFILE";
    case ExclusionReason::FAILED_DOMAIN: return "FAILED_DOMAIN";
    case ExclusionReason::DOMAIN_INDEPENDENCE_UNKNOWN: return "DOMAIN_INDEPENDENCE_UNKNOWN";
    case ExclusionReason::INSUFFICIENT_CAPACITY: return "INSUFFICIENT_CAPACITY";
    case ExclusionReason::COMMITMENT_CONFLICT: return "COMMITMENT_CONFLICT";
    case ExclusionReason::STATE_UNAVAILABLE: return "STATE_UNAVAILABLE";
    case ExclusionReason::STATE_TOO_OLD: return "STATE_TOO_OLD";
    case ExclusionReason::FENCING_UNPROVEN: return "FENCING_UNPROVEN";
    case ExclusionReason::ROUTE_AUTHORITY_UNAVAILABLE: return "ROUTE_AUTHORITY_UNAVAILABLE";
    case ExclusionReason::RECOVERY_BUDGET_INFEASIBLE: return "RECOVERY_BUDGET_INFEASIBLE";
    case ExclusionReason::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
    case ExclusionReason::STALE_AUTHORITY: return "STALE_AUTHORITY";
    case ExclusionReason::FENCED_BOOT: return "FENCED_BOOT";
    case ExclusionReason::UNKNOWN_REQUIRED_EVIDENCE: return "UNKNOWN_REQUIRED_EVIDENCE";
    case ExclusionReason::RESOURCES_REVOKED: return "RESOURCES_REVOKED";
    case ExclusionReason::UNREADY_ENGINE: return "UNREADY_ENGINE";
    case ExclusionReason::UNSUPPORTED_CONTINUITY: return "UNSUPPORTED_CONTINUITY";
    case ExclusionReason::POLICY_PROHIBITED_DEGRADATION: return "POLICY_PROHIBITED_DEGRADATION";
    case ExclusionReason::STALE_ROUTE_PRECONDITION: return "STALE_ROUTE_PRECONDITION";
  }
  return "UNKNOWN";
}

const char* to_string(RequestDisposition d) noexcept {
  switch (d) {
    case RequestDisposition::NOT_DISPATCHED: return "NOT_DISPATCHED";
    case RequestDisposition::DISPATCHED: return "DISPATCHED";
    case RequestDisposition::ACCEPTED_BY_TARGET: return "ACCEPTED_BY_TARGET";
    case RequestDisposition::CONFIRMED_COMPLETE: return "CONFIRMED_COMPLETE";
    case RequestDisposition::RESPONSE_COMMITTED: return "RESPONSE_COMMITTED";
    case RequestDisposition::FAILED: return "FAILED";
    case RequestDisposition::OUTCOME_UNKNOWN: return "OUTCOME_UNKNOWN";
  }
  return "UNKNOWN";
}

const char* to_string(FailbackPolicy p) noexcept {
  switch (p) {
    case FailbackPolicy::MANUAL: return "MANUAL";
    case FailbackPolicy::STAY_ON_CURRENT: return "STAY_ON_CURRENT";
    case FailbackPolicy::RETURN_AFTER_VALIDATION: return "RETURN_AFTER_VALIDATION";
  }
  return "MANUAL";
}

const char* to_string(RouteState s) noexcept {
  switch (s) {
    case RouteState::EMPTY: return "EMPTY";
    case RouteState::PENDING_INSTALL: return "PENDING_INSTALL";
    case RouteState::INSTALLED: return "INSTALLED";
    case RouteState::ACKNOWLEDGED: return "ACKNOWLEDGED";
    case RouteState::REVALIDATION_REQUIRED: return "REVALIDATION_REQUIRED";
    case RouteState::STALE: return "STALE";
  }
  return "EMPTY";
}

const char* to_string(Severity s) noexcept {
  switch (s) {
    case Severity::LOW: return "LOW";
    case Severity::MEDIUM: return "MEDIUM";
    case Severity::HIGH: return "HIGH";
    case Severity::CRITICAL: return "CRITICAL";
  }
  return "MEDIUM";
}

const char* to_string(ServingRole r) noexcept {
  switch (r) {
    case ServingRole::EXCLUSIVE_ACTIVE: return "EXCLUSIVE_ACTIVE";
    case ServingRole::PARTITIONED_SLOTS: return "PARTITIONED_SLOTS";
    case ServingRole::MULTI_ACTIVE_SLOTS: return "MULTI_ACTIVE_SLOTS";
  }
  return "EXCLUSIVE_ACTIVE";
}

const char* to_string(RtoAnchor a) noexcept {
  switch (a) {
    case RtoAnchor::FIRST_ACCEPTED_FAILURE_OBSERVATION: return "FIRST_ACCEPTED_FAILURE_OBSERVATION";
    case RtoAnchor::EXPLICIT_FAILOVER_REQUEST: return "EXPLICIT_FAILOVER_REQUEST";
    case RtoAnchor::SERVICE_CONTRACT_ANCHOR: return "SERVICE_CONTRACT_ANCHOR";
  }
  return "FIRST_ACCEPTED_FAILURE_OBSERVATION";
}

const char* to_string(RecoveryStage s) noexcept {
  switch (s) {
    case RecoveryStage::FAILURE_OBSERVATION: return "FAILURE_OBSERVATION";
    case RecoveryStage::CANDIDATE_EVALUATION: return "CANDIDATE_EVALUATION";
    case RecoveryStage::ENGINE_PREPARATION: return "ENGINE_PREPARATION";
    case RecoveryStage::STATE_RESTORE_OR_REBIND: return "STATE_RESTORE_OR_REBIND";
    case RecoveryStage::FENCING: return "FENCING";
    case RecoveryStage::ACTIVATION: return "ACTIVATION";
    case RecoveryStage::ROUTE_INSTALLATION: return "ROUTE_INSTALLATION";
    case RecoveryStage::VERIFICATION: return "VERIFICATION";
  }
  return "UNKNOWN";
}

const char* to_string(FenceEnforcement e) noexcept {
  switch (e) {
    case FenceEnforcement::COORDINATOR_ASSIGNMENT_AUTHORITY: return "COORDINATOR_ASSIGNMENT_AUTHORITY";
    case FenceEnforcement::GATEWAY_DISPATCH: return "GATEWAY_DISPATCH";
    case FenceEnforcement::WORKER_ADMISSION: return "WORKER_ADMISSION";
    case FenceEnforcement::RESULT_ACCEPTANCE: return "RESULT_ACCEPTANCE";
    case FenceEnforcement::EXTERNAL_SIDE_EFFECT_BOUNDARY: return "EXTERNAL_SIDE_EFFECT_BOUNDARY";
  }
  return "UNKNOWN";
}

const char* to_string(FenceState s) noexcept {
  switch (s) {
    case FenceState::NONE: return "NONE";
    case FenceState::REQUESTED: return "REQUESTED";
    case FenceState::ENFORCED: return "ENFORCED";
    case FenceState::UNPROVEN: return "UNPROVEN";
    case FenceState::SUPERSEDED: return "SUPERSEDED";
  }
  return "NONE";
}

const char* to_string(IdempotencyAuthority a) noexcept {
  switch (a) {
    case IdempotencyAuthority::NONE: return "NONE";
    case IdempotencyAuthority::SAFE_TO_RETRY: return "SAFE_TO_RETRY";
    case IdempotencyAuthority::DETERMINISTIC_REFERENCE: return "DETERMINISTIC_REFERENCE";
  }
  return "NONE";
}

const char* to_string(Milestone m) noexcept {
  switch (m) {
    case Milestone::INTENT_ONLY: return "INTENT_ONLY";
    case Milestone::OLD_ADMISSION_FENCED: return "OLD_ADMISSION_FENCED";
    case Milestone::REPLACEMENT_AUTHORIZED: return "REPLACEMENT_AUTHORIZED";
    case Milestone::ACTIVATION_ACKNOWLEDGED: return "ACTIVATION_ACKNOWLEDGED";
    case Milestone::ROUTE_INSTALLED: return "ROUTE_INSTALLED";
    case Milestone::VERIFICATION_COMPLETE: return "VERIFICATION_COMPLETE";
  }
  return "UNKNOWN";
}

}  // namespace failover_fabric
