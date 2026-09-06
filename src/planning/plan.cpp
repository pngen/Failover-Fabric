// plan.cpp — FailoverPlan helpers.
#include "failover_fabric/plan.hpp"

#include <algorithm>

namespace failover_fabric {

bool FailoverPlan::reached(Milestone m) const {
  return std::find(reached_milestones.begin(), reached_milestones.end(), m) != reached_milestones.end();
}

void FailoverPlan::record_milestone(Milestone m) {
  if (!reached(m)) reached_milestones.push_back(m);
}

bool FailoverPlan::is_valid() const {
  if (plan.is_null()) return false;
  if (slot.service.is_null() || slot.slot.is_null()) return false;
  if (!selected) return false;
  // A plan cannot be valid and already at verification-complete; that means it was executed.
  if (reached(Milestone::VERIFICATION_COMPLETE)) return false;
  return true;
}

}  // namespace failover_fabric
