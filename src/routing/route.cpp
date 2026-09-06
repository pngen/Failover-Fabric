// route.cpp — RouteTable implementation.
#include "failover_fabric/route.hpp"

#include <algorithm>

namespace failover_fabric {

RouteTable::SlotRoute* RouteTable::find_(ServiceSlotKey slot) {
  for (SlotRoute& s : current_) if (s.current.slot == slot) return &s;
  return nullptr;
}

const RouteTable::SlotRoute* RouteTable::find_(ServiceSlotKey slot) const {
  for (const SlotRoute& s : current_) if (s.current.slot == slot) return &s;
  return nullptr;
}

bool RouteTable::install(RouteEntry entry) {
  // Reject strict generation regression: a route may only replace one strictly newer.
  if (entry.slot.service.is_null()) return false;
  SlotRoute* sr = find_(entry.slot);
  if (sr) {
    if (entry.generation.value() <= sr->current.generation.value()) return false;
    if (entry.epoch.value() < sr->current.epoch.value()) return false;
    // Preserve history (bounded by caller policy elsewhere).
    sr->history.push_back(std::move(sr->current));
    sr->current = std::move(entry);
    return true;
  }
  current_.push_back(SlotRoute{entry, {}});
  return true;
}

bool RouteTable::acknowledge(ServiceSlotKey slot, RouteGeneration gen, GatewayBootId boot) {
  SlotRoute* sr = find_(slot);
  if (!sr) return false;
  // The boot that acknowledges must match the route's permitted gateway boot id.
  if (sr->current.generation != gen) return false;
  if (boot != sr->current.gateway_boot) return false;
  if (sr->current.state != RouteState::INSTALLED && sr->current.state != RouteState::PENDING_INSTALL) return false;
  sr->current.state = RouteState::ACKNOWLEDGED;
  return true;
}

std::optional<RouteEntry> RouteTable::current(ServiceSlotKey slot) const {
  const SlotRoute* sr = find_(slot);
  if (!sr) return std::nullopt;
  if (sr->current.state == RouteState::EMPTY) return std::nullopt;
  return sr->current;
}

std::vector<RouteEntry> RouteTable::history(ServiceSlotKey slot, std::size_t max) const {
  const SlotRoute* sr = find_(slot);
  if (!sr) return {};
  std::vector<RouteEntry> out = sr->history;
  if (out.size() > max) out.resize(max);
  return out;
}

std::vector<RouteEntry> RouteTable::all_routes() const {
  std::vector<RouteEntry> out;
  for (const SlotRoute& s : current_) if (s.current.state != RouteState::EMPTY) out.push_back(s.current);
  return out;
}

void RouteTable::mark_revalidation_required() {
  for (SlotRoute& s : current_) {
    if (s.current.state == RouteState::ACKNOWLEDGED || s.current.state == RouteState::INSTALLED ||
        s.current.state == RouteState::PENDING_INSTALL) {
      s.current.state = RouteState::REVALIDATION_REQUIRED;
      ++revalidation_count_;
    }
  }
}

}  // namespace failover_fabric
