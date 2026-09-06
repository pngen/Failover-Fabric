// persistence.cpp — versioned, integrity-checked durable state.
#include "failover_fabric/persistence.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

#include <cstdio>

namespace failover_fabric {
namespace persistence {

namespace {

// Bounds for hostile lengths/counts.
constexpr std::uint64_t kMaxBlob = 1ull << 30;      // 1 GiB
constexpr std::uint32_t kMaxCount = 1u << 22;        // 4M elements
constexpr std::uint32_t kMaxString = 1u << 22;

std::uint64_t fnv1a(const std::uint8_t* data, std::size_t n, std::uint64_t h = 1469598103934665603ull) {
  for (std::size_t i = 0; i < n; ++i) { h ^= data[i]; h *= 1099511628211ull; }
  return h;
}

void put_u32(std::vector<std::uint8_t>& b, std::uint32_t v) {
  b.push_back((std::uint8_t)(v & 0xff)); b.push_back((std::uint8_t)((v >> 8) & 0xff));
  b.push_back((std::uint8_t)((v >> 16) & 0xff)); b.push_back((std::uint8_t)((v >> 24) & 0xff));
}
void put_u64(std::vector<std::uint8_t>& b, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) b.push_back((std::uint8_t)((v >> (8 * i)) & 0xff));
}
void put_u8(std::vector<std::uint8_t>& b, std::uint8_t v) { b.push_back(v); }
void put_bool(std::vector<std::uint8_t>& b, bool v) { b.push_back(v ? 1 : 0); }
void put_double(std::vector<std::uint8_t>& b, double v) {
  if (!std::isfinite(v)) throw std::runtime_error("non-finite value in persistence");
  std::uint64_t u; std::memcpy(&u, &v, 8); put_u64(b, u);
}
void put_string(std::vector<std::uint8_t>& b, const std::string& s) {
  if (s.size() > kMaxString) throw std::runtime_error("string too large");
  put_u32(b, (std::uint32_t)s.size());
  b.insert(b.end(), s.begin(), s.end());
}
template <typename T, typename S>
void put_opt(std::vector<std::uint8_t>& b, const std::optional<T>& v, S ser) {
  put_bool(b, v.has_value()); if (v) ser(b, *v);
}

struct Reader {
  const std::uint8_t* p; std::size_t n; std::size_t i = 0;
  Reader(const std::uint8_t* d, std::size_t sz) : p(d), n(sz) {}
  void need(std::size_t k) { if (i + k > n) throw std::runtime_error("truncated persistence blob"); }
  std::uint8_t u8() { need(1); return p[i++]; }
  std::uint32_t u32() { need(4); std::uint32_t v = 0; for (int k = 0; k < 4; ++k) v |= (std::uint32_t)p[i++] << (8 * k); return v; }
  std::uint64_t u64() { need(8); std::uint64_t v = 0; for (int k = 0; k < 8; ++k) v |= (std::uint64_t)p[i++] << (8 * k); return v; }
  bool boolean() { std::uint8_t v = u8(); if (v > 1) throw std::runtime_error("invalid bool"); return v == 1; }
  double dbl() { std::uint64_t u = u64(); double v; std::memcpy(&v, &u, 8); if (!std::isfinite(v)) throw std::runtime_error("non-finite value"); return v; }
  std::string str() { std::uint32_t len = u32(); if (len > kMaxString) throw std::runtime_error("string too large"); need(len); std::string s((const char*)p + i, len); i += len; return s; }
  std::uint32_t count() { std::uint32_t c = u32(); if (c > kMaxCount) throw std::runtime_error("count too large"); return c; }
};
template <typename T, typename D>
std::optional<T> opt(Reader& r, D deser) { bool b = r.boolean(); if (!b) return std::nullopt; return deser(r); }

// generic id/gen serialization via value()
void encode_id(std::vector<std::uint8_t>& b, std::uint64_t v) { put_u64(b, v); }
template <typename IdLike>
void put_id(std::vector<std::uint8_t>& b, const IdLike& id) { put_u64(b, id.value()); }
template <typename GenLike>
void put_gen(std::vector<std::uint8_t>& b, const GenLike& g) { put_u64(b, g.value()); }

}  // namespace
namespace {
// enum-range validation helpers (throw on invalid enum bytes)
void chk_ev_status(std::uint8_t v){ if(v>(std::uint8_t)EvidenceStatus::UNKNOWN) throw std::runtime_error("invalid EvidenceStatus"); }
void chk_cat(std::uint8_t v){ if(v>(std::uint8_t)FailureCategory::UNKNOWN_FAILURE) throw std::runtime_error("invalid FailureCategory"); }
void chk_prov(std::uint8_t v){ if(v>(std::uint8_t)Provenance::UNKNOWN) throw std::runtime_error("invalid Provenance"); }
void chk_conf(std::uint8_t v){ if(v>(std::uint8_t)Severity::CRITICAL) throw std::runtime_error("invalid Severity"); }
void chk_outcome(std::uint8_t v){ if(v>(std::uint8_t)ServiceOutcome::REVALIDATION_REQUIRED) throw std::runtime_error("invalid ServiceOutcome"); }
void chk_attempt(std::uint8_t v){ if(v>(std::uint8_t)AttemptState::RECOVERY_REQUIRED) throw std::runtime_error("invalid AttemptState"); }
void chk_asgn(std::uint8_t v){ if(v>(std::uint8_t)AssignmentState::REVALIDATION_REQUIRED) throw std::runtime_error("invalid AssignmentState"); }
void chk_cont(std::uint8_t v){ if(v>(std::uint8_t)ContinuityClass::UNKNOWN) throw std::runtime_error("invalid ContinuityClass"); }
void chk_dom(std::uint8_t v){ if(v>(std::uint8_t)DomainClass::UNKNOWN) throw std::runtime_error("invalid DomainClass"); }
void chk_excl(std::uint8_t v){ if(v>(std::uint8_t)ExclusionReason::STALE_ROUTE_PRECONDITION) throw std::runtime_error("invalid ExclusionReason"); }
void chk_disp(std::uint8_t v){ if(v>(std::uint8_t)RequestDisposition::OUTCOME_UNKNOWN) throw std::runtime_error("invalid RequestDisposition"); }
void chk_fb(std::uint8_t v){ if(v>(std::uint8_t)FailbackPolicy::RETURN_AFTER_VALIDATION) throw std::runtime_error("invalid FailbackPolicy"); }
void chk_route(std::uint8_t v){ if(v>(std::uint8_t)RouteState::STALE) throw std::runtime_error("invalid RouteState"); }
void chk_role(std::uint8_t v){ if(v>(std::uint8_t)ServingRole::MULTI_ACTIVE_SLOTS) throw std::runtime_error("invalid ServingRole"); }
void chk_rto(std::uint8_t v){ if(v>(std::uint8_t)RtoAnchor::SERVICE_CONTRACT_ANCHOR) throw std::runtime_error("invalid RtoAnchor"); }
void chk_fence(std::uint8_t v){ if(v>(std::uint8_t)FenceState::SUPERSEDED) throw std::runtime_error("invalid FenceState"); }
void chk_milestone(std::uint8_t v){ if(v>(std::uint8_t)Milestone::VERIFICATION_COMPLETE) throw std::runtime_error("invalid Milestone"); }

void enc_ts(std::vector<std::uint8_t>& b, const Timestamp& t){ put_u64(b,(std::uint64_t)t.wall_ns); put_u64(b,t.seq); }
Timestamp dec_ts(Reader& r){ Timestamp t; t.wall_ns=(std::int64_t)r.u64(); t.seq=r.u64(); return t; }

template<class Id> Id dec_id(Reader& r){ return Id((std::uint64_t)r.u64()); }
template<class Gen> Gen dec_gen(Reader& r){ return Gen((std::uint64_t)r.u64()); }
}  // namespace
namespace {
void enc_slotkey(std::vector<std::uint8_t>& b, const ServiceSlotKey& s){ put_id(b,s.service); put_id(b,s.slot); }
ServiceSlotKey dec_slotkey(Reader& r){ return ServiceSlotKey{dec_id<ServiceId>(r), dec_id<ServiceSlotId>(r)}; }

void enc_compat(std::vector<std::uint8_t>& b, const CompatibilityRequirement& c){
  put_string(b,c.model_key); put_string(b,c.runtime_abi);
  put_opt(b,c.readiness_profile,[](auto& bb, const ReadinessProfileId& v){ put_id(bb,v); });
  put_u32(b,(std::uint32_t)c.dependencies.size()); for(auto& x:c.dependencies) put_id(b,x);
  put_u32(b,(std::uint32_t)c.required_capabilities.size()); for(auto& x:c.required_capabilities) put_id(b,x);
}
CompatibilityRequirement dec_compat(Reader& r){
  CompatibilityRequirement c; c.model_key=r.str(); c.runtime_abi=r.str();
  c.readiness_profile=opt<ReadinessProfileId>(r,[](Reader& rr){ return dec_id<ReadinessProfileId>(rr); });
  auto nd=r.count(); for(std::uint32_t i=0;i<nd;i++) c.dependencies.push_back(dec_id<DependencyId>(r));
  auto nc=r.count(); for(std::uint32_t i=0;i<nc;i++) c.required_capabilities.push_back(dec_id<CompatibilityId>(r));
  return c;
}
void enc_domreq(std::vector<std::uint8_t>& b, const DomainRequirement& d){
  put_bool(b,d.require_domain_independence); put_u32(b,d.min_distinct_hosts); put_u32(b,d.min_distinct_devices);
  put_u32(b,(std::uint32_t)d.forbidden_overlap_classes.size()); for(auto x:d.forbidden_overlap_classes) put_u8(b,(std::uint8_t)x);
}
DomainRequirement dec_domreq(Reader& r){
  DomainRequirement d; d.require_domain_independence=r.boolean(); d.min_distinct_hosts=r.u32(); d.min_distinct_devices=r.u32();
  auto n=r.count(); for(std::uint32_t i=0;i<n;i++){ auto v=r.u8(); chk_dom(v); d.forbidden_overlap_classes.push_back((DomainClass)v); }
  return d;
}
void enc_resreq(std::vector<std::uint8_t>& b, const ResourceRequirement& r){ put_string(b,r.resource_class); put_double(b,r.amount); put_bool(b,r.exactly_one_target); }
ResourceRequirement dec_resreq(Reader& r){ ResourceRequirement x; x.resource_class=r.str(); x.amount=r.dbl(); x.exactly_one_target=r.boolean(); return x; }
void enc_reco(std::vector<std::uint8_t>& b, const RecoveryObjective& o){
  put_u64(b,o.rto_ms); put_u8(b,(std::uint8_t)o.rto_anchor); put_u8(b,(std::uint8_t)o.continuity); put_u64(b,o.max_state_loss_sequences);
  put_opt(b,o.min_checkpoint_gen,[](auto& bb,const CheckpointGeneration& v){ put_gen(bb,v); }); put_bool(b,o.requires_verified_recovery);
}
RecoveryObjective dec_reco(Reader& r){
  RecoveryObjective o; o.rto_ms=r.u64(); auto a=r.u8(); chk_rto(a); o.rto_anchor=(RtoAnchor)a; auto c=r.u8(); chk_cont(c); o.continuity=(ContinuityClass)c;
  o.max_state_loss_sequences=r.u64(); o.min_checkpoint_gen=opt<CheckpointGeneration>(r,[](Reader& rr){ return dec_gen<CheckpointGeneration>(rr); }); o.requires_verified_recovery=r.boolean(); return o;
}
void enc_afp(std::vector<std::uint8_t>& b, const AntiFlappingPolicy& p){ put_u32(b,p.max_auto_attempts); put_u32(b,p.cooldown_ms); put_u32(b,p.hysteresis_ms); put_bool(b,p.manual_intervention_required); }
AntiFlappingPolicy dec_afp(Reader& r){ AntiFlappingPolicy p; p.max_auto_attempts=r.u32(); p.cooldown_ms=r.u32(); p.hysteresis_ms=r.u32(); p.manual_intervention_required=r.boolean(); return p; }

void enc_svc(std::vector<std::uint8_t>& b, const ServiceDefinition& s){
  put_id(b,s.service); put_gen(b,s.generation); put_gen(b,s.config_generation); put_string(b,s.name); put_u8(b,(std::uint8_t)s.role);
  put_u32(b,s.slots.first); put_u32(b,s.slots.count); enc_compat(b,s.compatibility); enc_domreq(b,s.domain_requirement);
  put_u32(b,(std::uint32_t)s.resources.size()); for(auto& x:s.resources) enc_resreq(b,x);
  enc_reco(b,s.recovery); put_u8(b,(std::uint8_t)s.failback); enc_afp(b,s.anti_flapping);
  put_u32(b,(std::uint32_t)s.required_state.size()); for(auto& x:s.required_state) put_id(b,x);
  put_u8(b,(std::uint8_t)s.provenance); put_gen(b,s.policy_generation);
}
ServiceDefinition dec_svc(Reader& r){
  ServiceDefinition s; s.service=dec_id<ServiceId>(r); s.generation=dec_gen<ServiceGeneration>(r); s.config_generation=dec_gen<ServiceConfigGeneration>(r);
  s.name=r.str(); auto ro=r.u8(); chk_role(ro); s.role=(ServingRole)ro; s.slots.first=r.u32(); s.slots.count=r.u32();
  s.compatibility=dec_compat(r); s.domain_requirement=dec_domreq(r);
  auto nr=r.count(); for(std::uint32_t i=0;i<nr;i++) s.resources.push_back(dec_resreq(r));
  s.recovery=dec_reco(r); auto fb=r.u8(); chk_fb(fb); s.failback=(FailbackPolicy)fb; s.anti_flapping=dec_afp(r);
  auto ns=r.count(); for(std::uint32_t i=0;i<ns;i++) s.required_state.push_back(dec_id<StateId>(r));
  auto pv=r.u8(); chk_prov(pv); s.provenance=(Provenance)pv; s.policy_generation=dec_gen<PolicyGeneration>(r);
  return s;
}
void enc_assign(std::vector<std::uint8_t>& b, const Assignment& a){
  put_id(b,a.id); put_gen(b,a.generation); enc_slotkey(b,a.slot); put_id(b,a.target); put_gen(b,a.target_generation);
  put_id(b,a.replica); put_gen(b,a.replica_generation); put_id(b,a.engine); put_id(b,a.engine_incarnation);
  put_id(b,a.readiness_profile); put_gen(b,a.authority_generation); put_id(b,a.activation); put_gen(b,a.activation_generation);
  put_id(b,a.route); put_gen(b,a.route_generation); put_id(b,a.worker_boot);
  put_u32(b,(std::uint32_t)a.resource_claims.size()); for(auto& x:a.resource_claims) put_id(b,x);
  put_u32(b,(std::uint32_t)a.continuity_state.size()); for(auto& x:a.continuity_state) put_id(b,x);
  put_u8(b,(std::uint8_t)a.state); put_bool(b,a.provisory); put_u8(b,(std::uint8_t)a.provenance);
}
Assignment dec_assign(Reader& r){
  Assignment a; a.id=dec_id<AssignmentId>(r); a.generation=dec_gen<AssignmentGeneration>(r); a.slot=dec_slotkey(r);
  a.target=dec_id<TargetId>(r); a.target_generation=dec_gen<TargetGeneration>(r); a.replica=dec_id<ReplicaId>(r); a.replica_generation=dec_gen<ReplicaGeneration>(r);
  a.engine=dec_id<EngineId>(r); a.engine_incarnation=dec_id<EngineIncarnationId>(r); a.readiness_profile=dec_id<ReadinessProfileId>(r);
  a.authority_generation=dec_gen<ServiceAuthorityGeneration>(r); a.activation=dec_id<ActivationId>(r); a.activation_generation=dec_gen<ActivationGeneration>(r);
  a.route=dec_id<RouteId>(r); a.route_generation=dec_gen<RouteGeneration>(r); a.worker_boot=dec_id<WorkerBootId>(r);
  auto nr=r.count(); for(std::uint32_t i=0;i<nr;i++) a.resource_claims.push_back(dec_id<ResourceClaimId>(r));
  auto ns=r.count(); for(std::uint32_t i=0;i<ns;i++) a.continuity_state.push_back(dec_id<StateId>(r));
  auto st=r.u8(); chk_asgn(st); a.state=(AssignmentState)st; a.provisory=r.boolean(); auto pv=r.u8(); chk_prov(pv); a.provenance=(Provenance)pv;
  return a;
}
void enc_domdecl(std::vector<std::uint8_t>& b, const DomainDecl& d){ put_id(b,d.id); put_gen(b,d.generation); put_u8(b,(std::uint8_t)d.cls); put_string(b,d.name); put_opt(b,d.parent,[](auto& bb,const FailureDomainId& v){ put_id(bb,v); }); put_u8(b,(std::uint8_t)d.provenance); }
DomainDecl dec_domdecl(Reader& r){
  DomainDecl d; d.id=dec_id<FailureDomainId>(r); d.generation=dec_gen<FailureDomainGeneration>(r); auto c=r.u8(); chk_dom(c); d.cls=(DomainClass)c; d.name=r.str();
  d.parent=opt<FailureDomainId>(r,[](Reader& rr){ return dec_id<FailureDomainId>(rr); }); auto pv=r.u8(); chk_prov(pv); d.provenance=(Provenance)pv; return d;
}
void enc_memb(std::vector<std::uint8_t>& b, const DomainMembership& m){ put_id(b,m.target); put_id(b,m.domain); put_bool(b,m.known); }
DomainMembership dec_memb(Reader& r){ DomainMembership m; m.target=dec_id<TargetId>(r); m.domain=dec_id<FailureDomainId>(r); m.known=r.boolean(); return m; }
void enc_ev(std::vector<std::uint8_t>& b, const FailureEvent& e){
  put_id(b,e.event_id); put_gen(b,e.generation); put_id(b,e.source); put_id(b,e.source_boot); put_u8(b,(std::uint8_t)e.category);
  put_u8(b,(std::uint8_t)e.status); put_u8(b,(std::uint8_t)e.provenance); put_u8(b,(std::uint8_t)e.severity); put_double(b,e.confidence);
  put_opt(b,e.target,[](auto& bb,const TargetId& v){ put_id(bb,v); });
  put_opt(b,e.target_generation,[](auto& bb,const TargetGeneration& v){ put_gen(bb,v); });
  put_opt(b,e.domain,[](auto& bb,const FailureDomainId& v){ put_id(bb,v); });
  enc_ts(b,e.observed); enc_ts(b,e.received); put_string(b,e.mechanism); put_string(b,e.detail);
  put_u32(b,(std::uint32_t)e.affected_domains.size()); for(auto& x:e.affected_domains) put_id(b,x); put_bool(b,e.revalidates_existing);
}
FailureEvent dec_ev(Reader& r){
  FailureEvent e; e.event_id=dec_id<FailureEventId>(r); e.generation=dec_gen<FailureGeneration>(r); e.source=dec_id<SourceId>(r); e.source_boot=dec_id<SourceBootId>(r);
  auto ca=r.u8(); chk_cat(ca); e.category=(FailureCategory)ca; auto st=r.u8(); chk_ev_status(st); e.status=(EvidenceStatus)st;
  auto pv=r.u8(); chk_prov(pv); e.provenance=(Provenance)pv; auto se=r.u8(); chk_conf(se); e.severity=(Severity)se; e.confidence=r.dbl();
  e.target=opt<TargetId>(r,[](Reader& rr){ return dec_id<TargetId>(rr); });
  e.target_generation=opt<TargetGeneration>(r,[](Reader& rr){ return dec_gen<TargetGeneration>(rr); });
  e.domain=opt<FailureDomainId>(r,[](Reader& rr){ return dec_id<FailureDomainId>(rr); });
  e.observed=dec_ts(r); e.received=dec_ts(r); e.mechanism=r.str(); e.detail=r.str();
  auto nd=r.count(); for(std::uint32_t i=0;i<nd;i++) e.affected_domains.push_back(dec_id<FailureDomainId>(r));
  e.revalidates_existing=r.boolean(); return e;
}
void enc_route(std::vector<std::uint8_t>& b, const RouteEntry& e){
  put_id(b,e.route); put_gen(b,e.generation); enc_slotkey(b,e.slot); put_id(b,e.target); put_gen(b,e.target_generation);
  put_id(b,e.incarnation); put_id(b,e.permitted_boot); put_id(b,e.assignment); put_gen(b,e.assignment_generation);
  put_u64(b,e.epoch.value()); put_id(b,e.epoch.id()); put_id(b,e.epoch.boot()); put_id(b,e.gateway_boot); put_u8(b,(std::uint8_t)e.state); put_string(b,e.transport); put_u8(b,(std::uint8_t)e.provenance);
}
RouteEntry dec_route(Reader& r){
  RouteEntry e; e.route=dec_id<RouteId>(r); e.generation=dec_gen<RouteGeneration>(r); e.slot=dec_slotkey(r); e.target=dec_id<TargetId>(r); e.target_generation=dec_gen<TargetGeneration>(r);
  e.incarnation=dec_id<EngineIncarnationId>(r); e.permitted_boot=dec_id<WorkerBootId>(r); e.assignment=dec_id<AssignmentId>(r); e.assignment_generation=dec_gen<AssignmentGeneration>(r);
  e.epoch=CoordinatorEpoch(r.u64(), dec_id<CoordinatorId>(r), dec_id<CoordinatorId>(r));
  e.gateway_boot=dec_id<GatewayBootId>(r);
  auto st=r.u8(); chk_route(st); e.state=(RouteState)st; e.transport=r.str(); auto pv=r.u8(); chk_prov(pv); e.provenance=(Provenance)pv; return e;
}
void enc_epoch(std::vector<std::uint8_t>& b, const CoordinatorEpoch& e){ put_u64(b,e.value()); put_id(b,e.id()); put_id(b,e.boot()); }
CoordinatorEpoch dec_epoch(Reader& r){ return CoordinatorEpoch(r.u64(), dec_id<CoordinatorId>(r), dec_id<CoordinatorId>(r)); }
}  // namespace
// --------------------------------------------------------------------------- //
// Top-level encode / decode / validate
// --------------------------------------------------------------------------- //
namespace {
constexpr std::uint8_t kMagic[8] = { 'F','F','R','S','T','0','0','1' };
constexpr std::uint32_t kVersion = 1;
}

std::vector<std::uint8_t> encode(const PersistenceSnapshot& s) {
  if (s.format_version != kVersion) throw std::runtime_error("unsupported format version");
  std::vector<std::uint8_t> payload;
  put_u32(payload, s.format_version);
  enc_epoch(payload, s.epoch);
  put_gen(payload, s.policy_generation);
  put_gen(payload, s.last_authority_gen);
  put_u32(payload, (std::uint32_t)s.services.size());
  for (const auto& x : s.services) enc_svc(payload, x);
  put_u32(payload, (std::uint32_t)s.assignments.size());
  for (const auto& x : s.assignments) enc_assign(payload, x);
  put_u32(payload, (std::uint32_t)s.domains.size());
  for (const auto& x : s.domains) enc_domdecl(payload, x);
  put_u32(payload, (std::uint32_t)s.memberships.size());
  for (const auto& x : s.memberships) enc_memb(payload, x);
  put_u32(payload, (std::uint32_t)s.evidence.size());
  for (const auto& x : s.evidence) enc_ev(payload, x);
  put_u32(payload, (std::uint32_t)s.routes.size());
  for (const auto& x : s.routes) enc_route(payload, x);
  put_u32(payload, (std::uint32_t)s.fenced_boots.size());
  for (const auto& x : s.fenced_boots) put_id(payload, x);

  std::vector<std::uint8_t> blob;
  blob.insert(blob.end(), kMagic, kMagic + 8);
  put_u32(blob, kVersion);
  put_u64(blob, (std::uint64_t)payload.size());
  blob.insert(blob.end(), payload.begin(), payload.end());
  std::uint64_t crc = fnv1a(blob.data(), blob.size());
  put_u64(blob, crc);
  return blob;
}

PersistenceSnapshot decode(const std::vector<std::uint8_t>& blob) {
  if (blob.size() < 8 + 4 + 8 + 8 + 8) throw std::runtime_error("persistence blob too short");
  if (std::memcmp(blob.data(), kMagic, 8) != 0) throw std::runtime_error("bad persistence magic");
  Reader hdr(blob.data() + 8, blob.size() - 8);
  std::uint32_t ver = hdr.u32();
  if (ver != kVersion) throw std::runtime_error("unsupported persistence version");
  std::uint64_t plen = hdr.u64();
  if (plen > kMaxBlob) throw std::runtime_error("persistence payload too large");
  const std::size_t payload_offset = 8 + hdr.i;             // payload begins in the blob
  if (payload_offset + plen + 8 != blob.size()) throw std::runtime_error("persistence length mismatch");
  std::uint64_t crc_stored = 0;
  for (int k = 0; k < 8; ++k) crc_stored |= (std::uint64_t)blob[payload_offset + plen + k] << (8 * k);
  // checksum covers magic+version(u32)+plen(u64)+payload
  std::uint64_t crc_calc = fnv1a(blob.data(), payload_offset + plen);
  if (crc_calc != crc_stored) throw std::runtime_error("persistence checksum mismatch");

  Reader r(blob.data() + payload_offset, plen);
  PersistenceSnapshot s;
  s.format_version = r.u32();
  if (s.format_version != kVersion) throw std::runtime_error("unsupported snapshot format_version");
  s.epoch = dec_epoch(r);
  s.policy_generation = dec_gen<PolicyGeneration>(r);
  s.last_authority_gen = dec_gen<ServiceAuthorityGeneration>(r);
  auto nsvc = r.count();
  for (std::uint32_t i = 0; i < nsvc; ++i) s.services.push_back(dec_svc(r));
  auto nasg = r.count();
  for (std::uint32_t i = 0; i < nasg; ++i) s.assignments.push_back(dec_assign(r));
  auto ndom = r.count();
  for (std::uint32_t i = 0; i < ndom; ++i) s.domains.push_back(dec_domdecl(r));
  auto nmem = r.count();
  for (std::uint32_t i = 0; i < nmem; ++i) s.memberships.push_back(dec_memb(r));
  auto nev = r.count();
  for (std::uint32_t i = 0; i < nev; ++i) s.evidence.push_back(dec_ev(r));
  auto nrt = r.count();
  for (std::uint32_t i = 0; i < nrt; ++i) s.routes.push_back(dec_route(r));
  auto nfb = r.count();
  for (std::uint32_t i = 0; i < nfb; ++i) s.fenced_boots.push_back(dec_id<WorkerBootId>(r));
  if (r.i != r.n) throw std::runtime_error("trailing garbage in persistence payload");

  // Semantic validation.
  std::set<ServiceId> svc_ids;
  for (const auto& d : s.services) {
    if (!svc_ids.insert(d.service).second) throw std::runtime_error("duplicate service identity");
  }
  for (const auto& d : s.services) {
    if (d.slots.count == 0) throw std::runtime_error("service has zero slots");
  }
  auto count_active = [&](ServiceSlotKey key) {
    std::size_t n = 0;
    for (const auto& a : s.assignments) if (a.slot == key && a.state == AssignmentState::ACTIVE) ++n;
    return n;
  };
  std::set<ServiceSlotKey> seen_active;
  for (const auto& a : s.assignments) {
    if (!svc_ids.count(a.slot.service)) throw std::runtime_error("assignment references unknown service");
    if (a.state == AssignmentState::ACTIVE) {
      if (!seen_active.insert(a.slot).second) throw std::runtime_error("duplicate exclusive active assignment");
    }
    if (a.authority_generation.is_null()) throw std::runtime_error("null authority generation");
  }
  return s;
}

std::vector<std::string> validate(const std::vector<std::uint8_t>& blob) {
  std::vector<std::string> problems;
  try { decode(blob); } catch (const std::exception& e) { problems.push_back(e.what()); }
  return problems;
}

void save_file(const std::string& path, const PersistenceSnapshot& s) {
  std::vector<std::uint8_t> blob = encode(s);
  std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot open persistence file for write: " + path);
    out.write((const char*)blob.data(), (std::streamsize)blob.size());
    out.flush();
    if (!out.good()) throw std::runtime_error("persistence write failed");
  }
  std::remove(path.c_str());
  if (std::rename(tmp.c_str(), path.c_str()) != 0) throw std::runtime_error("persistence rename failed");
}

PersistenceSnapshot load_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) throw std::runtime_error("cannot open persistence file: " + path);
  std::streamsize size = in.tellg();
  if (size < 0) throw std::runtime_error("cannot stat persistence file");
  in.seekg(0, std::ios::beg);
  std::vector<std::uint8_t> blob((std::size_t)size);
  in.read((char*)blob.data(), size);
  if (!in) throw std::runtime_error("short read on persistence file");
  return decode(blob);
}

std::vector<std::string> validate_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) return {"cannot open persistence file: " + path};
  std::streamsize size = in.tellg();
  if (size < 0) return {"cannot stat persistence file"};
  in.seekg(0, std::ios::beg);
  std::vector<std::uint8_t> blob((std::size_t)size);
  in.read((char*)blob.data(), size);
  if (!in) return {"short read on persistence file"};
  return validate(blob);
}

}  // namespace persistence
}  // namespace failover_fabric