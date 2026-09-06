// request.hpp — Request records, disposition, and ambiguity classification.
//
// Service cutover is separate from in-flight request disposition. A connection loss
// after dispatch may produce OUTCOME_UNKNOWN; the runtime must not fabricate final
// success. Non-idempotent or externally effectful work is NOT auto-retried absent
// explicit idempotency/replay authority. Exactly-once is never claimed from deduplicated
// responses; late old-target results are rejected unless the request-authority contract
// permits them.
#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "failover_fabric/authority.hpp"
#include "failover_fabric/identities.hpp"

namespace failover_fabric {

enum class IdempotencyAuthority : std::uint8_t {
  NONE,                 // not idempotent; must not be auto-retried
  SAFE_TO_RETRY,        // caller/protocol explicitly permits retry
  DETERMINISTIC_REFERENCE, // the reference workload is deterministic & replay-safe
};
const char* to_string(IdempotencyAuthority a) noexcept;

struct RequestRecord {
  RequestId request{RequestId::null()};
  ExecutionId execution{ExecutionId::null()};
  ExecutionGeneration execution_generation;
  ServiceSlotKey slot;
  RequestDisposition disposition{RequestDisposition::NOT_DISPATCHED};
  bool idempotent{false};
  IdempotencyAuthority authority{IdempotencyAuthority::NONE};
  bool externally_effectful{false};
  std::optional<WorkerAuthorization> dispatched_auth;
  std::string client_note;
  std::string outcome;
};

// --------------------------------------------------------------------------- //
// Request tracker. A late old-target result is rejected unless the dispatcher's
// response-commit contract says otherwise. Ambiguous and rejected late outcomes are
// tracked separately from successful recovery.
// --------------------------------------------------------------------------- //
class RequestTracker {
 public:
  void record(RequestRecord r);
  std::optional<RequestRecord> find(RequestId id) const;

  // Returns ACCEPTED when the late result is for the current authorization.
  enum class LateOutcome { ACCEPTED, REJECTED_STALE, CLASSIFIED_AMBIGUOUS };
  LateOutcome classify_late_result(RequestId id, const WorkerAuthorization& result_auth);

  // A dispatched request whose response was withheld by the target has an unknown final
  // outcome: it is never fabricated as success, and an explicit OUTCOME_UNKNOWN is recorded.
  void set_outcome_unknown(RequestId id);

  std::size_t ambiguous_count() const noexcept { return ambiguous_; }
  std::size_t rejected_late_count() const noexcept { return rejected_late_; }
  std::size_t size() const noexcept { return records_.size(); }

 private:
  struct Entry { RequestRecord rec; bool committed{false}; };
  std::vector<Entry> records_;
  std::size_t ambiguous_{0};
  std::size_t rejected_late_{0};
  std::vector<Entry>::iterator find_(RequestId id);
  std::vector<Entry>::const_iterator find_(RequestId id) const;
};

}  // namespace failover_fabric
