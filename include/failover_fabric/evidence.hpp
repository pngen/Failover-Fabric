// evidence.hpp — Typed failure evidence.
//
// A socket disconnect does not prove the worker stopped executing. An OS process-exit
// observation proves that process exited; it does not prove an entire node or GPU
// failed. Every event binds its source and boot, affected target/domain, generation,
// observation sequence, observation/receipt time, clock domain, severity, confidence,
// provenance, evidence type, and supporting detail.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/identities.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

struct FailureEvent {
  FailureEventId event_id{FailureEventId::null()};
  FailureGeneration generation;
  SourceId source{SourceId::null()};
  SourceBootId source_boot{SourceBootId::null()};
  FailureCategory category{FailureCategory::UNKNOWN_FAILURE};
  EvidenceStatus status{EvidenceStatus::UNKNOWN};
  Provenance provenance{Provenance::REPORTED};
  Severity severity{Severity::MEDIUM};
  double confidence{1.0};               // [0,1]
  std::optional<TargetId> target;
  std::optional<TargetGeneration> target_generation;
  std::optional<FailureDomainId> domain;
  Timestamp observed;
  Timestamp received;
  std::string mechanism;               // the stated observation mechanism
  std::string detail;
  std::vector<FailureDomainId> affected_domains;
  bool revalidates_existing{false};    // true if this updates a prior event

  bool has_identity() const noexcept { return !target || !source.is_null(); }
};

// --------------------------------------------------------------------------- //
// Evidence store: ordered by receipt, keyed by event id. Rejects stale evidence
// according to explicit freshness policy and never lets an old healthy observation
// erase a newer failure.
// --------------------------------------------------------------------------- //
class EvidenceStore {
 public:
  // Returns true if `ev` was accepted (not stale / not superseded).
  bool publish(FailureEvent ev);

  // Latest confirmed-not-cleared failure affecting a target (by evidence id order).
  std::optional<FailureEvent> latest_failure_for(TargetId target) const;
  std::vector<FailureEvent> failures_for(TargetId target, std::size_t max) const;
  std::vector<FailureEvent> all() const;
  std::size_t size() const noexcept { return events_.size(); }

  // Clear an earlier event (e.g. health restored). Older events remain inspectable.
  void clear(FailureEventId id);

 private:
  struct Entry {
    FailureEvent event;
    bool cleared{false};
  };
  using Container = std::vector<Entry>;
  Container events_;
  Container::iterator find_(FailureEventId id);
  Container::const_iterator find_(FailureEventId id) const;
};

}  // namespace failover_fabric
