// domains.hpp — Failure domains and shared-risk relationships.
//
// Failure-domain identity is explicit. Domains may be nested in a tree but may also
// overlap in arbitrary ways (a target may share multiple correlated domains). The
// domain index exposes declared memberships and shared-risk, and lets eligibility
// reject candidates excluded by current failure evidence and service policy. Unknown
// domain information must NOT silently satisfy an independence requirement.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "failover_fabric/identities.hpp"
#include "failover_fabric/types.hpp"

namespace failover_fabric {

struct DomainDecl {
  FailureDomainId id{FailureDomainId::null()};
  FailureDomainGeneration generation;
  DomainClass cls{DomainClass::UNKNOWN};
  std::string name;
  std::optional<FailureDomainId> parent;   // for hierarchical domains
  Provenance provenance{Provenance::REPORTED};
};

// A target declares which failure domains it belongs to.
struct DomainMembership {
  TargetId target{TargetId::null()};
  FailureDomainId domain{FailureDomainId::null()};
  bool known{true};   // false => membership is unknown, never satisfies independence
};

// --------------------------------------------------------------------------- //
// Domain registry: domains + memberships, with shared-risk queries.
// --------------------------------------------------------------------------- //
class DomainRegistry {
 public:
  void declare(DomainDecl d);
  void set_membership(DomainMembership m);
  void mark_membership_unknown(TargetId t, FailureDomainId d);

  std::optional<DomainDecl> find(FailureDomainId id) const;
  std::vector<FailureDomainId> domains_of(TargetId t) const;
  std::vector<FailureDomainId> ancestors_of(FailureDomainId id) const;

  // Do two targets overlap in ANY declared domain? For an unknown membership result
  // (i.e. independence cannot be confirmed) return nullopt.
  std::optional<bool> overlaps(TargetId a, TargetId b) const;
  // True if `t` is in (or under) the given domain, known or not.
  std::optional<bool> in_domain(TargetId t, FailureDomainId d) const;

  static std::vector<FailureDomainId> concrete_ancestors(const std::vector<FailureDomainId>& chain);

  std::vector<DomainDecl> all_domains() const;
  std::vector<DomainMembership> all_memberships() const;
  std::size_t domain_count() const noexcept { return domains_.size(); }

 private:
  std::vector<DomainDecl> domains_;
  std::vector<DomainMembership> memberships_;
  const DomainDecl* find_(FailureDomainId id) const;
};

}  // namespace failover_fabric
