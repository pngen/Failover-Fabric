// persistence.hpp — Versioned, integrity-checked durable state.
//
// The format is a single self-describing blob: magic, version, a byte count, then a
// deterministic (magic-version-bound-dependent) payload followed by a checksum. Decoding
// rejects bad magic, unsupported version, truncation, integrity mismatch, oversized
// counts, invalid enums, generation regression, duplicate exclusive assignments, broken
// references, impossible milestones, malformed domain relationships, invalid cost/time
// values, integer overflow and trailing garbage. Loading happens into a validated
// temporary before it is allowed to replace live state.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/candidate.hpp"
#include "failover_fabric/domains.hpp"
#include "failover_fabric/evidence.hpp"
#include "failover_fabric/fence.hpp"
#include "failover_fabric/plan.hpp"
#include "failover_fabric/route.hpp"
#include "failover_fabric/service.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

// The serializable runtime state snapshot. Not a live representation: it is the durable
// record used to rebuild runtime state after a restorthat must be revalidated before use.
struct PersistenceSnapshot {
  std::uint32_t format_version{1};
  CoordinatorEpoch epoch;
  PolicyGeneration policy_generation;
  ServiceAuthorityGeneration last_authority_gen;
  std::vector<ServiceDefinition> services;
  std::vector<Assignment> assignments;
  std::vector<DomainDecl> domains;
  std::vector<DomainMembership> memberships;
  std::vector<FailureEvent> evidence;
  std::vector<RouteEntry> routes;
  std::vector<WorkerBootId> fenced_boots;
};

namespace persistence {

// Encode with integrity check. Throws on non-finite/invalid fields.
std::vector<std::uint8_t> encode(const PersistenceSnapshot& s);

// Decode into a validated temporary. Throws std::runtime_error on any corruption.
PersistenceSnapshot decode(const std::vector<std::uint8_t>& blob);

// Return a list of human-readable validation problems; empty means valid.
std::vector<std::string> validate(const std::vector<std::uint8_t>& blob);

// Convenience: write atomically (write to temp, fsync-close, rename).
void save_file(const std::string& path, const PersistenceSnapshot& s);
PersistenceSnapshot load_file(const std::string& path);
std::vector<std::string> validate_file(const std::string& path);

}  // namespace persistence
}  // namespace failover_fabric
