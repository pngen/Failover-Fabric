// evidence.cpp — EvidenceStore implementation.
#include "failover_fabric/evidence.hpp"

#include <algorithm>

namespace failover_fabric {

EvidenceStore::Container::iterator EvidenceStore::find_(FailureEventId id) {
  return std::find_if(events_.begin(), events_.end(), [&](const Entry& e) { return e.event.event_id == id; });
}

EvidenceStore::Container::const_iterator EvidenceStore::find_(FailureEventId id) const {
  return std::find_if(events_.begin(), events_.end(), [&](const Entry& e) { return e.event.event_id == id; });
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
  events_.push_back(Entry{ev, ev.status == EvidenceStatus::CLEARED});
  return true;
}

void EvidenceStore::clear(FailureEventId id) {
  auto it = find_(id);
  if (it != events_.end()) it->cleared = true;
}

std::optional<FailureEvent> EvidenceStore::latest_failure_for(TargetId target) const {
  std::optional<FailureEvent> result;
  for (const Entry& e : events_) {
    if (e.cleared) continue;
    if (!e.event.target || *e.event.target != target) continue;
    // Confirm/1..; only confirmed or high-severity suspected count as a live failure.
    if (e.event.status == EvidenceStatus::CLEARED || e.event.status == EvidenceStatus::STALE) continue;
    if (!result || e.event.received.seq >= result->received.seq) result = e.event;
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
  for (const Entry& e : events_) {
    if (e.cleared) continue;
    if (!e.event.target || *e.event.target != target) continue;
    if (e.event.status == EvidenceStatus::CLEARED || e.event.status == EvidenceStatus::STALE) continue;
    out.push_back(e.event);
  }
  if (out.size() > max) out.resize(max);
  return out;
}

}  // namespace failover_fabric
