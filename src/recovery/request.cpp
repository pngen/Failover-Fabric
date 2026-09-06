// request.cpp — RequestTracker implementation.
#include "failover_fabric/request.hpp"

#include <algorithm>

namespace failover_fabric {

std::vector<RequestTracker::Entry>::iterator RequestTracker::find_(RequestId id) {
  return std::find_if(records_.begin(), records_.end(), [&](const Entry& e) { return e.rec.request == id; });
}

std::vector<RequestTracker::Entry>::const_iterator RequestTracker::find_(RequestId id) const {
  return std::find_if(records_.begin(), records_.end(), [&](const Entry& e) { return e.rec.request == id; });
}

void RequestTracker::record(RequestRecord r) {
  auto it = find_(r.request);
  if (it != records_.end()) it->rec = r;
  else records_.push_back(Entry{r, false});
}

std::optional<RequestRecord> RequestTracker::find(RequestId id) const {
  auto it = find_(id);
  if (it == records_.end()) return std::nullopt;
  return it->rec;
}

RequestTracker::LateOutcome RequestTracker::classify_late_result(RequestId id, const WorkerAuthorization&) {
  auto it = find_(id);
  if (it == records_.end()) { ++ambiguous_; return LateOutcome::CLASSIFIED_AMBIGUOUS; }
  const RequestRecord& r = it->rec;
  // No dispatch record: cannot tie the result to authorized work.
  if (!r.dispatched_auth) { ++ambiguous_; return LateOutcome::CLASSIFIED_AMBIGUOUS; }
  // A current response was already committed for this request: a late result is stale.
  if (r.disposition == RequestDisposition::RESPONSE_COMMITTED) { ++rejected_late_; return LateOutcome::REJECTED_STALE; }
  // The request was in flight when authority moved: its final outcome is genuinely unknown.
  ++ambiguous_;
  return LateOutcome::CLASSIFIED_AMBIGUOUS;
}

}  // namespace failover_fabric
