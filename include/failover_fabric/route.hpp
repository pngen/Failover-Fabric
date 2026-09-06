// route.hpp — Route entries, the route table, and generation-bound cutover.
//
// A route update request is not an acknowledged route change, and an acknowledged route
// change is not proof that every independent client observed it. The route table is the
// reference dispatch authority; installing a new route generation may only happen from
// the coordinator under the current epoch. Stale route updates are rejected.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/authority.hpp"
#include "failover_fabric/identities.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

struct RouteEntry {
  RouteId route{RouteId::null()};
  RouteGeneration generation;
  ServiceSlotKey slot;
  TargetId target{TargetId::null()};
  TargetGeneration target_generation;
  EngineIncarnationId incarnation{EngineIncarnationId::null()};
  WorkerBootId permitted_boot{WorkerBootId::null()};
  AssignmentId assignment{AssignmentId::null()};
  AssignmentGeneration assignment_generation;
  CoordinatorEpoch epoch;
  RouteState state{RouteState::EMPTY};
  GatewayBootId gateway_boot{GatewayBootId::null()};   // gateway boot that installed this route
  std::string transport;   // e.g. "tcp://127.0.0.1:PORT"
  Provenance provenance{Provenance::DERIVED};
};

// --------------------------------------------------------------------------- //
// Route table: one current route per slot, plus a bounded history.
// --------------------------------------------------------------------------- //
class RouteTable {
 public:
  // Install a route only if it is strictly newer than the current one for its slot.
  bool install(RouteEntry entry);
  // Bind the current route for a slot to a gateway boot (the gateway that now holds it),
  // without advancing the generation. A re-registered gateway re-acquires route authority.
  bool bind_gateway(ServiceSlotKey slot, GatewayBootId boot);
  // Acknowledge the current installed route for a slot from a specific gateway boot.
  bool acknowledge(ServiceSlotKey slot, RouteGeneration gen, GatewayBootId boot);

  std::optional<RouteEntry> current(ServiceSlotKey slot) const;
  std::vector<RouteEntry> history(ServiceSlotKey slot, std::size_t max) const;
  std::vector<RouteEntry> all_routes() const;
  std::size_t size() const noexcept { return current_.size(); }

  // Revalidation: mark current routes for a gateway boot as needing fresh authority.
  void mark_revalidation_required();
  std::size_t revalidation_required_count() const noexcept { return revalidation_count_; }

 private:
  struct SlotRoute { RouteEntry current; std::vector<RouteEntry> history; };
  std::vector<SlotRoute> current_;
  std::size_t revalidation_count_{0};
  SlotRoute* find_(ServiceSlotKey slot);
  const SlotRoute* find_(ServiceSlotKey slot) const;
};

}  // namespace failover_fabric
