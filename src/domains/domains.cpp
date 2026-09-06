// domains.cpp — DomainRegistry implementation (declared memberships, shared risk).
#include "failover_fabric/domains.hpp"

#include <algorithm>

namespace failover_fabric {

const DomainDecl* DomainRegistry::find_(FailureDomainId id) const {
  for (const DomainDecl& d : domains_) if (d.id == id) return &d;
  return nullptr;
}

void DomainRegistry::declare(DomainDecl d) {
  if (d.id.is_null()) return;
  for (DomainDecl& e : domains_) {
    if (e.id == d.id) {
      if (d.generation.value() >= e.generation.value()) e = d;
      return;
    }
  }
  domains_.push_back(d);
}

void DomainRegistry::set_membership(DomainMembership m) {
  for (DomainMembership& e : memberships_) {
    if (e.target == m.target && e.domain == m.domain) { e = m; return; }
  }
  memberships_.push_back(m);
}

void DomainRegistry::mark_membership_unknown(TargetId t, FailureDomainId d) {
  set_membership(DomainMembership{t, d, false});
}

std::optional<DomainDecl> DomainRegistry::find(FailureDomainId id) const {
  const DomainDecl* d = find_(id);
  if (!d) return std::nullopt;
  return *d;
}

std::vector<FailureDomainId> DomainRegistry::domains_of(TargetId t) const {
  std::vector<FailureDomainId> out;
  for (const DomainMembership& m : memberships_) if (m.target == t && m.known) out.push_back(m.domain);
  return out;
}

std::vector<FailureDomainId> DomainRegistry::ancestors_of(FailureDomainId id) const {
  std::vector<FailureDomainId> out;
  std::optional<FailureDomainId> cur = id;
  std::size_t guard = 0;
  while (cur && guard++ < 64) {
    const DomainDecl* d = find_(*cur);
    if (!d) break;
    out.push_back(d->id);
    cur = d->parent;
  }
  return out;
}

std::optional<bool> DomainRegistry::overlaps(TargetId a, TargetId b) const {
  std::vector<FailureDomainId> closure_a, closure_b;
  bool any_unknown_a = false, any_unknown_b = false;
  for (const DomainMembership& m : memberships_) {
    if (m.target == a) {
      if (!m.known) { any_unknown_a = true; continue; }
      auto anc = ancestors_of(m.domain);
      closure_a.insert(closure_a.end(), anc.begin(), anc.end());
    } else if (m.target == b) {
      if (!m.known) { any_unknown_b = true; continue; }
      auto anc = ancestors_of(m.domain);
      closure_b.insert(closure_b.end(), anc.begin(), anc.end());
    }
  }
  for (FailureDomainId d : closure_a) {
    if (std::find(closure_b.begin(), closure_b.end(), d) != closure_b.end()) return true;
  }
  if (any_unknown_a || any_unknown_b) return std::nullopt;
  return false;
}

std::optional<bool> DomainRegistry::in_domain(TargetId t, FailureDomainId d) const {
  bool declared_member = false;
  bool declared_unknown = false;
  for (const DomainMembership& m : memberships_) {
    if (m.target != t) continue;
    if (m.domain == d) {
      if (!m.known) declared_unknown = true;
      else declared_member = true;
    }
  }
  // An explicit unknown membership means we cannot decide. A target with no declared
  // membership in the domain is NOT a member (declared memberships are authoritative); it
  // is only unknown when the membership is explicitly marked unknown.
  if (declared_unknown) return std::nullopt;
  if (declared_member) return true;
  return false;
}

std::vector<DomainDecl> DomainRegistry::all_domains() const { return domains_; }
std::vector<DomainMembership> DomainRegistry::all_memberships() const { return memberships_; }

std::vector<FailureDomainId> DomainRegistry::concrete_ancestors(const std::vector<FailureDomainId>& chain) {
  return chain;
}

}  // namespace failover_fabric
