// evidence.cpp — EvidenceStore implementation.
#include "failover_fabric/evidence.hpp"

#include <algorithm>

namespace failover_fabric {

EvidenceStore::Container::iterator EvidenceStore::find_(FailureEventId id) {
  auto it = id_index_.find(id);
  return it != id_index_.end() ? events_.begin() + (std::ptrdiff_t)it->second : events_.end();
}

EvidenceStore::Container::const_iterator EvidenceStore::find_(FailureEventId id) const {
  auto it = id_index_.find(id);
  return it != id_index_.end() ? events_.begin() + (std::ptrdiff_t)it->second : events_.end();
}

bool EvidenceStore::publish(FailureEvent ev) {
  // An old healthy observation must never erase a newer failure: we keep cleared events
  // inspectable, and a CLEARED event is ordered after the failure it clears; a later
  // CONFIRMED failure re-marking the event supersedes a prior CLEARED.
  auto it = find_(ev.event_id);
  if (it != events_.end()) {
    // A newer observation for the same id replaces the earlier one. A CONFIRMED failure
    // always wins over a CLEARED; a CLEARED must NOT erase a CONFIRMED failure observed later.
    const FailureEvent& old = it->event;
    bool newer = (ev.received.seq > old.received.seq) ||
                 (ev.received.seq == old.received.seq && ev.received.wall_ns >= old.received.wall_ns);
    if (newer && (old.status == EvidenceStatus::CONFIRMED && ev.status == EvidenceStatus::CLEARED)) {
      return false;  // do not let a stale clear erase a confirmed failure
    }
    if (newer) {
      it->event = ev;
      it->cleared = (ev.status == EvidenceStatus::CLEARED);
      return true;
    }
    return false;  // stale
  }
  id_index_[ev.event_id] = events_.size();
  if (ev.target) target_index_[*ev.target].push_back(events_.size());
  events_.push_back(Entry{ev, ev.status == EvidenceStatus::CLEARED});
  return true;
}

void EvidenceStore::clear(FailureEventId id) {
  auto it = find_(id);
  if (it != events_.end()) it->cleared = true;
}

std::optional<FailureEvent> EvidenceStore::latest_failure_for(TargetId target) const {
  std::optional<FailureEvent> result;
  if (auto it = target_index_.find(target); it != target_index_.end()) {
    for (std::size_t idx : it->second) {
      const Entry& e = events_[idx];
      if (e.cleared) continue;
      if (e.event.status == EvidenceStatus::CLEARED || e.event.status == EvidenceStatus::STALE) continue;
      if (!result || e.event.received.seq >= result->received.seq) result = e.event;
    }
  }
  return result;
}

std::vector<FailureEvent> EvidenceStore::all() const {
  std::vector<FailureEvent> out;
  for (const Entry& e : events_) out.push_back(e.event);
  return out;
}

std::vector<FailureEvent> EvidenceStore::failures_for(TargetId target, std::size_t max) const {
  std::vector<FailureEvent> out;
  if (auto it = target_index_.find(target); it != target_index_.end()) {
    for (std::size_t idx : it->second) {
      const Entry& e = events_[idx];
      if (e.cleared) continue;
      if (e.event.status == EvidenceStatus::CLEARED || e.event.status == EvidenceStatus::STALE) continue;
      out.push_back(e.event);
    }
  }
  if (out.size() > max) out.resize(max);
  return out;
}

}  // namespace failover_fabric