// cutover.hpp — The interface the coordinator uses to drive external effects.
//
// The coordinator core NEVER performs the external fence / activation / route-install /
// verification effects itself; it calls a CutoverOps adapter. This keeps the core
// independently usable (an in-process test adapter is provided) while the real
// deployment implements CutoverOps over the reference protocol to real worker and
// gateway processes. The coordinator never holds a core state lock while calling these.
#pragma once

#include <string>

#include "failover_fabric/authority.hpp"
#include "failover_fabric/candidate.hpp"
#include "failover_fabric/fence.hpp"
#include "failover_fabric/identities.hpp"
#include "failover_fabric/route.hpp"
#include "failover_fabric/service.hpp"

namespace failover_fabric {

struct FenceOutcome {
  bool applied{false};
  std::string detail;
  FenceState state{FenceState::NONE};
  bool fencing_unproven{false};
};

struct ActivateOutcome {
  bool acknowledged{false};
  std::string activation_note;
  std::string detail;
};

struct RouteOutcome {
  bool acknowledged{false};
  RouteId route{RouteId::null()};
  RouteGeneration generation;
  std::string detail;
};

struct VerifyOutcome {
  bool verified{false};
  std::string result;
  bool ambiguous{false};
  std::string detail;
};

class CutoverOps {
 public:
  virtual ~CutoverOps() = default;
  virtual FenceOutcome fence_old(const Assignment& old_assignment, WorkerBootId new_boot) = 0;
  virtual ActivateOutcome activate(const Candidate& c, const WorkerAuthorization& auth, ServiceAuthorityGeneration gen) = 0;
  virtual RouteOutcome install_route(const RouteEntry& entry, GatewayBootId boot) = 0;
  virtual VerifyOutcome verify(const RouteEntry& entry, const WorkerAuthorization& auth) = 0;
};

}  // namespace failover_fabric
