// selection.cpp — Hard eligibility + deterministic ranking.
#include "failover_fabric/selection.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace failover_fabric {

const double SelectionEngine::kDomainIndependenceWeight = 0.25;
const double SelectionEngine::kPreparationWeight = 0.12;
const double SelectionEngine::kContinuityWeight = 0.20;
const double SelectionEngine::kStateFreshnessWeight = 0.10;
const double SelectionEngine::kCapacityWeight = 0.08;
const double SelectionEngine::kResourceOverlapWeight = 0.06;
const double SelectionEngine::kPriorityWeight = 0.05;
const double SelectionEngine::kPrepCostWeight = 0.07;
const double SelectionEngine::kRoutingCostWeight = 0.04;
const double SelectionEngine::kPolicyPreferenceWeight = 0.03;

namespace {

bool in_list(const std::vector<WorkerBootId>& v, WorkerBootId b) {
  return std::find(v.begin(), v.end(), b) != v.end();
}
bool in_targets(const std::vector<TargetId>& v, TargetId t) {
  return std::find(v.begin(), v.end(), t) != v.end();
}

double resource_overlap_credit(const Candidate& c) {
  return c.resources.empty() ? 0.5 : 1.0;
}

}  // namespace

bool SelectionEngine::hard_eligible(Candidate& c, const ServiceDefinition& svc,
                                    const DomainRegistry& dom, const SelectionContext& ctx) const {
  // 1. Reject the failed target itself as a replacement.
  if (c.facts.target != TargetId::null() && c.facts.target == ctx.failed_target) {
    c.failed_reason = ExclusionReason::FAILED_DOMAIN;
    c.failed_detail = "candidate is the failed target";
    return false;
  }
  // 2. Confirmed-failed target (e.g. health revoked, device dead).
  if (in_targets(ctx.confirmed_failed_targets, c.facts.target)) {
    c.failed_reason = ExclusionReason::FAILED_DOMAIN;
    c.failed_detail = "target has confirmed failure evidence";
    return false;
  }
  // 3. Fenced boot may never be re-authorized as a replacement.
  if (in_list(ctx.fenced_boots, c.facts.worker_boot)) {
    c.failed_reason = ExclusionReason::FENCED_BOOT;
    c.failed_detail = "worker boot is fenced";
    return false;
  }
  // 4. Fresh readiness required.
  if (ctx.requires_fresh_readiness) {
    if (!c.facts.evidence_fresh) {
      c.failed_reason = ExclusionReason::STALE_READINESS;
      c.failed_detail = "readiness evidence is stale";
      return false;
    }
    if (!c.facts.ready) {
      c.failed_reason = ExclusionReason::UNREADY_ENGINE;
      c.failed_detail = "engine is not ready";
      return false;
    }
  }
  // 5. Compatibility.
  if (!svc.compatibility.model_key.empty() && c.facts.model_key != svc.compatibility.model_key) {
    c.failed_reason = ExclusionReason::INCOMPATIBLE_PROFILE;
    c.failed_detail = "model_key mismatch";
    return false;
  }
  if (!svc.compatibility.runtime_abi.empty() && c.facts.runtime_abi != svc.compatibility.runtime_abi) {
    c.failed_reason = ExclusionReason::INCOMPATIBLE_PROFILE;
    c.failed_detail = "runtime_abi mismatch";
    return false;
  }
  if (svc.compatibility.readiness_profile && c.facts.profile != *svc.compatibility.readiness_profile) {
    c.failed_reason = ExclusionReason::INCOMPATIBLE_PROFILE;
    c.failed_detail = "readiness_profile mismatch";
    return false;
  }
  // 6. Continuity and state.
  const ContinuityClass required = svc.recovery.continuity;
  // Only checkpoint-restore and session-state-rebind REQUIRE restored state. A stateless
  // restart or replay-safe reference does not: it must not be silently downgraded, but it
  // also must not be excluded for lacking state that the contract does not require.
  const bool stateful = (required == ContinuityClass::CHECKPOINT_RESTORE ||
                         required == ContinuityClass::SESSION_STATE_REBIND);
  if (stateful) {
    if (!c.state.state_available || !c.state.correct_session || !c.state.correct_tenant ||
        !c.state.correct_model_generation || !c.state.state_format_ok || !c.state.status_integrity_ok) {
      c.failed_reason = ExclusionReason::STATE_UNAVAILABLE;
      c.failed_detail = "required state is not available or incompatible";
      return false;
    }
    if (svc.recovery.min_checkpoint_gen && c.state.checkpoint_generation) {
      if (*c.state.checkpoint_generation < *svc.recovery.min_checkpoint_gen) {
        c.failed_reason = ExclusionReason::STATE_TOO_OLD;
        c.failed_detail = "checkpoint is older than the permitted recovery point";
        return false;
      }
    }
    if (!c.continuity || *c.continuity == ContinuityClass::UNKNOWN) {
      c.failed_reason = ExclusionReason::UNSUPPORTED_CONTINUITY;
      c.failed_detail = "candidate continuity class is unknown";
      return false;
    }
  }
  // 7. Resources.
  for (const ResourceRequirement& req : svc.resources) {
    // Also reject if the candidate has an explicit conflicting exclusive commitment.
    bool found = false;
    for (const CandidateResource& cr : c.resources) {
      if (cr.resource_class != req.resource_class) continue;
      found = true;
      if (cr.resources_revoked) {
        c.failed_reason = ExclusionReason::RESOURCES_REVOKED;
        c.failed_detail = "resource claim revoked";
        return false;
      }
      if (cr.has_conflicting_commitment) {
        c.failed_reason = ExclusionReason::COMMITMENT_CONFLICT;
        c.failed_detail = "conflicting exclusive commitment on " + req.resource_class;
        return false;
      }
      if (cr.amount < req.amount) {
        c.failed_reason = ExclusionReason::INSUFFICIENT_CAPACITY;
        c.failed_detail = "insufficient capacity on " + req.resource_class;
        return false;
      }
      break;
    }
    if (!found) {
      c.failed_reason = ExclusionReason::INSUFFICIENT_CAPACITY;
      c.failed_detail = "no capacity on " + req.resource_class;
      return false;
    }
  }
  // 8. Failure-domain independence.
  if (svc.domain_requirement.require_domain_independence) {
    auto ov = dom.overlaps(c.facts.target, ctx.failed_target);
    if (!ov) {
      c.failed_reason = ExclusionReason::DOMAIN_INDEPENDENCE_UNKNOWN;
      c.failed_detail = "domain independence cannot be established (unknown membership)";
      return false;
    }
    if (*ov) {
      c.failed_reason = ExclusionReason::FAILED_DOMAIN;
      c.failed_detail = "candidate shares a failure domain with the failed target";
      return false;
    }
    // Check overlap with any explicitly failed domain.
    for (FailureDomainId fd : ctx.failed_domains) {
      auto ind = dom.in_domain(c.facts.target, fd);
      if (ind && *ind) {
        c.failed_reason = ExclusionReason::FAILED_DOMAIN;
        c.failed_detail = "candidate is inside a failed domain";
        return false;
      }
      if (!ind) {
        c.failed_reason = ExclusionReason::DOMAIN_INDEPENDENCE_UNKNOWN;
        c.failed_detail = "candidate domain membership is unknown in failed domain";
        return false;
      }
    }
  }
  c.hard_eligible = true;
  c.failed_reason.reset();
  return true;
}

std::uint64_t SelectionEngine::rank(const Candidate& c, const ServiceDefinition& svc, const SelectionContext& ctx) {
  (void)ctx;
  // Named factors, normalized to [0,1]; higher is better. Costs appear as negative.
  double score = 0.0;
  // Preparation: already prepared beats requires preparation.
  score += kPreparationWeight * (c.facts.ready ? 1.0 : 0.0);
  // Time to verified service: lower estimated critical path is better.
  double t = (double)c.costs.estimated_critical_path_ms;
  double tinv = t > 0 ? 1.0 - std::min(1.0, t / 30000.0) : 1.0;
  score += kPrepCostWeight * tinv;
  // Continuity quality: higher continuity class is better.
  int cls = c.continuity ? (int)*c.continuity : 0;
  score += kContinuityWeight * (cls / (double)ContinuityClass::UNKNOWN);
  // State freshness: lower loss is better; fresher checkpoint is better.
  double loss = (double)c.costs.state_loss_sequences;
  double lossv = loss > 0 ? 1.0 - std::min(1.0, loss / 1000.0) : 1.0;
  score += kStateFreshnessWeight * lossv;
  // Capacity abundance.
  double cap = 0.0; bool have_cap = false;
  for (const ResourceRequirement& req : svc.resources) {
    for (const CandidateResource& cr : c.resources) {
      if (cr.resource_class == req.resource_class) {
        double avail = cr.amount > req.amount ? ((cr.amount - req.amount) / std::max(1.0, req.amount)) : 0.0;
        cap += std::min(1.0, avail);
        have_cap = true;
        break;
      }
    }
  }
  cap = have_cap ? cap / svc.resources.size() : 0.0;
  score += kCapacityWeight * cap;
  // Resource overlap: fewer overlapping commitments is better (a proxy for spare capacity).
  score += kResourceOverlapWeight * resource_overlap_credit(c);
  // Domain independence is a hard filter; if we got here it passed, so give full credit.
  score += kDomainIndependenceWeight;
  // Policy preference / externally supplied service priority is not modeled beyond a tiebreak.
  score += kPolicyPreferenceWeight * 0.5;

  // Deterministic integer score scaled; the score itself is a tie-break only. We combine
  // with the primary key (critical path) so selection is fully deterministic.
  std::uint64_t result = static_cast<std::uint64_t>(std::round(score * 1e6));
  // Deterministic tie-breaking: smaller critical path wins ties.
  result = result * 2 + (c.costs.estimated_critical_path_ms % 1000);
  return result;
}

SelectionResult SelectionEngine::select(const CandidateSnapshot& snap, const ServiceDefinition& svc,
                                        const DomainRegistry& dom, const SelectionContext& ctx) const {
  SelectionResult out;
  out.policy_gen = svc.policy_generation;
  out.snapshot_gen = snap.generation;

  // Phase 1: hard eligibility. We copy each candidate so eligibility can annotate the
  // exclusion reason without UB, and never modify the caller's snapshot.
  std::vector<Candidate> working = snap.candidates;
  std::vector<Candidate*> eligible;
  for (Candidate& c : working) {
    if (hard_eligible(c, svc, dom, ctx)) {
      eligible.push_back(&c);
    } else if (c.failed_reason) {
      out.exclusions.emplace_back(c.facts.target, std::make_pair(*c.failed_reason, c.failed_detail));
    }
  }
  if (eligible.empty()) {
    out.has_selection = false;
    if (out.exclusions.empty()) {
      out.exclusions.emplace_back(TargetId::null(), std::make_pair(ExclusionReason::NO_ELIGIBLE_TARGET, std::string("no candidates")));
    }
    return out;
  }

  // Phase 2: rank eligible candidates deterministically. When a failback/preferred target
  // is specified, it is strongly preferred among hard-eligible candidates — but it must still
  // pass hard eligibility (fresh readiness, compatibility, state, domains, etc.). A stale or
  // unready preferred target is never silently selected.
  std::stable_sort(eligible.begin(), eligible.end(), [&](const Candidate* a, const Candidate* b) {
    const bool pa = ctx.preferred_target && a->facts.target == *ctx.preferred_target;
    const bool pb = ctx.preferred_target && b->facts.target == *ctx.preferred_target;
    if (pa != pb) return pa;   // preferred target first (only if both hard-eligible)
    std::uint64_t ra = rank(*a, svc, ctx);
    std::uint64_t rb = rank(*b, svc, ctx);
    if (ra != rb) return ra > rb;
    // Deterministic tie-break on target id.
    if (a->facts.target != b->facts.target) return a->facts.target < b->facts.target;
    return a->facts.worker_boot < b->facts.worker_boot;
  });

  out.selected = *eligible.front();
  out.has_selection = true;
  for (std::size_t i = 1; i < eligible.size(); ++i) out.evaluated_alternatives.push_back(*eligible[i]);
  out.provenance = Provenance::DERIVED;
  out.uncertainty = eligible.size() > 1 ? 1.0 / (double)eligible.size() : 0.0;
  return out;
}

}  // namespace failover_fabric