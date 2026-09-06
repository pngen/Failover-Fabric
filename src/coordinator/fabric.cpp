// fabric.cpp — FailoverFabric implementation: the authoritative coordinator core.
#include "failover_fabric/failover_fabric.hpp"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <shared_mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "failover_fabric/persistence.hpp"

namespace failover_fabric {

// --------------------------------------------------------------------------- //
// Impl
// --------------------------------------------------------------------------- //
struct FailoverFabric::Impl {
  RuntimeConfig cfg;
  mutable std::shared_mutex mtx;

  std::vector<ServiceDefinition> services;
  std::shared_ptr<Clock> clock;

  struct SlotState {
    ServiceSlotKey slot;
    std::optional<Assignment> current;
    std::vector<Assignment> history;
  };
  std::vector<SlotState> slots;

  struct TargetRecord {
    TargetId id{TargetId::null()};
    std::string transport;
    std::optional<CandidateFacts> facts;
    bool has_state{false};
    CandidateState state;
    std::vector<CandidateResource> resources;
  };
  std::vector<TargetRecord> targets;

  EvidenceStore evidence;
  DomainRegistry domains;
  RouteTable routes;
  RequestTracker requests;

  CoordinatorEpoch epoch;
  PolicyGeneration policy_gen;
  ServiceAuthorityGeneration last_authority_gen;
  mutable CandidateSnapshotGeneration snapshot_counter;
  AssignmentId assignment_counter{AssignmentId::min_id()};
  AssignmentGeneration assignment_gen_counter;
  FailoverAttemptId attempt_counter{FailoverAttemptId::min_id()};
  FailoverAttemptGeneration attempt_gen_counter;
  FailoverPlanId plan_counter{FailoverPlanId::min_id()};
  FailoverPlanGeneration plan_gen_counter;
  RouteId route_counter{RouteId::min_id()};
  RouteGeneration route_gen_counter;
  ActivationId activation_counter{ActivationId::min_id()};
  ActivationGeneration activation_gen_counter;
  GatewayBootId gateway_boot;
  CoordinatorId coordinator_id{CoordinatorId::min_id()};

  std::vector<WorkerBootId> fenced_boots;
  std::vector<AttemptState> attempt_states;
  std::uint64_t receipt_seq_{0};

  explicit Impl(RuntimeConfig c) : cfg(std::move(c)), clock(std::make_shared<SteadyClock>()),
      last_authority_gen(ServiceAuthorityGeneration::first()) {}
  explicit Impl() : Impl(RuntimeConfig{}) {}

  SlotState* find_slot(ServiceSlotKey s) { for (SlotState& x : slots) if (x.slot == s) return &x; return nullptr; }
  const SlotState* find_slot(ServiceSlotKey s) const { for (const SlotState& x : slots) if (x.slot == s) return &x; return nullptr; }
  TargetRecord* find_target(TargetId t) { for (TargetRecord& x : targets) if (x.id == t) return &x; return nullptr; }
  const TargetRecord* find_target(TargetId t) const { for (const TargetRecord& x : targets) if (x.id == t) return &x; return nullptr; }
  const ServiceDefinition* find_service(ServiceId id) const { for (const ServiceDefinition& s : services) if (s.service == id) return &s; return nullptr; }
  const ServiceDefinition* service_for_slot(ServiceSlotKey slot) const { return find_service(slot.service); }

  AssignmentId next_assignment_id() { AssignmentId n = assignment_counter; assignment_counter = assignment_counter.next(); return n; }
  bool boot_fenced(WorkerBootId b) const { return std::find(fenced_boots.begin(), fenced_boots.end(), b) != fenced_boots.end(); }
  void add_fenced_boot(WorkerBootId b) { if (!b.is_null() && !boot_fenced(b)) fenced_boots.push_back(b); }

  SelectionContext build_context(ServiceSlotKey slot) const;
  CandidateSnapshot snapshot_locked(ServiceSlotKey slot) const;
};

namespace {
ContinuityClass infer_continuity(const CandidateState& st, const CandidateFacts& facts) {
  if (st.state_available && st.correct_session && st.correct_tenant && st.correct_model_generation &&
      st.state_format_ok && st.status_integrity_ok) {
    if (st.checkpoint_generation) return ContinuityClass::CHECKPOINT_RESTORE;
    if (st.state_generation) return ContinuityClass::SESSION_STATE_REBIND;
  }
  if (facts.ready) return ContinuityClass::REPLAY_SAFE_REQUESTS;
  return ContinuityClass::STATELESS_RESTART;
}
std::uint64_t crit_path_ms(const Candidate& c) {
  return std::max(c.costs.estimated_prep_ms, c.costs.state_restore_ms) + 20 + 20 + 20 + 100;  // fence+activate+route+verify
}
WorkerAuthorization make_auth(const CoordinatorEpoch& epoch, const Assignment& a, const Candidate& c,
                              RequestId req, ExecutionId ex, const std::string& op) {
  WorkerAuthorization w;
  w.epoch = epoch; w.slot = a.slot; w.assignment = a.id; w.assignment_generation = a.generation;
  w.route_generation = a.route_generation; w.incarnation = c.facts.engine_incarnation;
  w.request = req; w.execution = ex; w.operation = op; w.worker_boot = c.facts.worker_boot;
  return w;
}
}  // namespace

FailoverFabric::FailoverFabric() : impl_(std::make_unique<Impl>()) {}
FailoverFabric::FailoverFabric(RuntimeConfig cfg) : impl_(std::make_unique<Impl>(std::move(cfg))) {}
FailoverFabric::~FailoverFabric() = default;
FailoverFabric::FailoverFabric(FailoverFabric&&) noexcept = default;
FailoverFabric& FailoverFabric::operator=(FailoverFabric&&) noexcept = default;

// --------------------------------------------------------------------------- //
// Control plane
// --------------------------------------------------------------------------- //
void FailoverFabric::set_coordinator_identity(CoordinatorId id, std::uint64_t epoch) {
  std::unique_lock lk(impl_->mtx);
  impl_->coordinator_id = id;
  impl_->epoch = CoordinatorEpoch(epoch, id, id);
}
CoordinatorEpoch FailoverFabric::current_epoch() const noexcept {
  std::shared_lock lk(impl_->mtx);
  return impl_->epoch;
}
void FailoverFabric::set_gateway_boot(GatewayBootId boot) {
  std::unique_lock lk(impl_->mtx);
  impl_->gateway_boot = boot;
}
GatewayBootId FailoverFabric::gateway_boot() const noexcept {
  std::shared_lock lk(impl_->mtx);
  return impl_->gateway_boot;
}
// --------------------------------------------------------------------------- //
// Service contract
// --------------------------------------------------------------------------- //
void FailoverFabric::create_service(ServiceDefinition def) {
  std::unique_lock lk(impl_->mtx);
  if (impl_->services.size() >= impl_->cfg.max_services) throw CapacityExceeded("max_services exceeded");
  for (const ServiceDefinition& s : impl_->services) {
    if (s.service == def.service) throw std::invalid_argument("service already exists with same identity");
  }
  impl_->services.push_back(std::move(def));
}

std::optional<ServiceDefinition> FailoverFabric::get_service(ServiceId service) const {
  std::shared_lock lk(impl_->mtx);
  const ServiceDefinition* s = impl_->find_service(service);
  if (!s) return std::nullopt;
  return *s;
}

std::vector<ServiceDefinition> FailoverFabric::services() const {
  std::shared_lock lk(impl_->mtx);
  return impl_->services;
}

std::vector<ServiceSlotKey> FailoverFabric::slots(ServiceId service) const {
  std::shared_lock lk(impl_->mtx);
  const ServiceDefinition* svc = impl_->find_service(service);
  if (!svc) return {};
  std::vector<ServiceSlotKey> out;
  for (std::uint32_t i = 0; i < svc->slots.count; ++i) out.push_back(ServiceSlotKey{service, ServiceSlotId(svc->slots.first + i)});
  return out;
}

// --------------------------------------------------------------------------- //
// Assignments
// --------------------------------------------------------------------------- //
void FailoverFabric::set_initial_assignment(Assignment a) {
  std::unique_lock lk(impl_->mtx);
  if (a.slot.service.is_null() || a.slot.slot.is_null()) throw std::invalid_argument("assignment slot is null");
  Impl::SlotState* st = impl_->find_slot(a.slot);
  if (st && st->current) throw std::logic_error("slot already has a current assignment");
  if (!st) { impl_->slots.push_back(Impl::SlotState{a.slot, std::nullopt, {}}); st = &impl_->slots.back(); }
  a.state = AssignmentState::ACTIVE;
  a.provisory = false;
  if (a.authority_generation.is_null()) a.authority_generation = impl_->last_authority_gen;
  st->current = std::move(a);
}

std::optional<Assignment> FailoverFabric::current_assignment(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  const Impl::SlotState* st = impl_->find_slot(slot);
  if (!st || !st->current) return std::nullopt;
  return st->current;
}

std::vector<Assignment> FailoverFabric::assignment_history(ServiceSlotKey slot, std::size_t max) const {
  std::shared_lock lk(impl_->mtx);
  const Impl::SlotState* st = impl_->find_slot(slot);
  if (!st) return {};
  std::vector<Assignment> out = st->history;
  if (out.size() > max) out.resize(max);
  return out;
}

// --------------------------------------------------------------------------- //
// Domains
// --------------------------------------------------------------------------- //
void FailoverFabric::declare_domain(DomainDecl d) { impl_->domains.declare(d); }
void FailoverFabric::set_membership(DomainMembership m) { impl_->domains.set_membership(m); }
const DomainRegistry& FailoverFabric::domains() const noexcept { return impl_->domains; }

// --------------------------------------------------------------------------- //
// Candidate registration
// --------------------------------------------------------------------------- //
void FailoverFabric::register_target(TargetId id, std::string transport) {
  std::unique_lock lk(impl_->mtx);
  if (impl_->targets.size() >= impl_->cfg.max_targets) throw CapacityExceeded("max_targets exceeded");
  if (!impl_->find_target(id)) impl_->targets.push_back(Impl::TargetRecord{id, std::move(transport), std::nullopt, false, {}, {}});
}

void FailoverFabric::publish_candidate_facts(CandidateFacts f) {
  std::unique_lock lk(impl_->mtx);
  Impl::TargetRecord* t = impl_->find_target(f.target);
  if (!t) throw std::invalid_argument("target not registered");
  if (!t->facts || f.readiness_generation.value() >= t->facts->readiness_generation.value()) {
    f.evidence_fresh = true;
    t->facts = std::move(f);
  }
}

void FailoverFabric::publish_candidate_state(TargetId t, CandidateState s) {
  std::unique_lock lk(impl_->mtx);
  Impl::TargetRecord* r = impl_->find_target(t);
  if (!r) throw std::invalid_argument("target not registered");
  r->state = std::move(s);
  r->has_state = true;
}

void FailoverFabric::publish_candidate_resources(TargetId t, std::vector<CandidateResource> r) {
  std::unique_lock lk(impl_->mtx);
  Impl::TargetRecord* rec = impl_->find_target(t);
  if (!rec) throw std::invalid_argument("target not registered");
  rec->resources = std::move(r);
}

// --------------------------------------------------------------------------- //
// Failure evidence
// --------------------------------------------------------------------------- //
void FailoverFabric::publish_failure(FailureEvent ev) {
  std::unique_lock lk(impl_->mtx);
  if (ev.received.seq == 0) ev.received.seq = ++impl_->receipt_seq_;
  impl_->evidence.publish(std::move(ev));
}
void FailoverFabric::clear_failure(FailureEventId id) {
  std::unique_lock lk(impl_->mtx);
  impl_->evidence.clear(id);
}
std::vector<FailureEvent> FailoverFabric::failures_for(TargetId t, std::size_t max) const {
  std::shared_lock lk(impl_->mtx);
  return impl_->evidence.failures_for(t, max);
}
// --------------------------------------------------------------------------- //
// Snapshot + selection context (lock-free; callers hold the lock)
// --------------------------------------------------------------------------- //
CandidateSnapshot FailoverFabric::Impl::snapshot_locked(ServiceSlotKey slot) const {
  CandidateSnapshot snap;
  snapshot_counter = snapshot_counter.next();
  snap.generation = snapshot_counter;
  snap.slot = slot;
  const ServiceDefinition* svc = service_for_slot(slot);
  const std::string want_model = svc ? svc->compatibility.model_key : std::string();
  const std::string want_abi = svc ? svc->compatibility.runtime_abi : std::string();
  for (const TargetRecord& t : targets) {
    if (!t.facts) continue;
    // A candidate snapshot must be scoped to the service: only targets matching this
    // service's compatibility are eligible, so do not build every target as a candidate.
    // Scope a candidate snapshot to the service when the candidate is bound to one.
    if (t.facts->service != ServiceId::null()) { if (!(t.facts->service == slot.service)) continue; }
    else { if (!want_model.empty() && t.facts->model_key != want_model) continue; if (!want_abi.empty() && t.facts->runtime_abi != want_abi) continue; }
    Candidate c;
    c.facts = *t.facts;
    c.state = t.state;
    c.resources = t.resources;
    c.domains = domains.domains_of(t.id);
    for (FailureDomainId d : c.domains) {
      auto anc = domains.ancestors_of(d);
      c.domains.insert(c.domains.end(), anc.begin(), anc.end());
    }
    c.continuity = infer_continuity(t.state, *t.facts);
    c.costs.estimated_prep_ms = t.facts->ready ? 0 : 5000;
    c.costs.state_restore_ms = (t.has_state && t.state.state_available) ? 200 : 0;
    c.costs.estimated_cutover_ms = 50;
    c.costs.estimated_critical_path_ms = crit_path_ms(c);
    c.costs.state_loss_sequences = (t.has_state && t.state.state_available) ? 0 : 1000;
    c.costs.cost_estimated = true;
    snap.candidates.push_back(std::move(c));
  }
  if (snap.candidates.size() > cfg.max_candidates_per_snapshot) snap.candidates.resize(cfg.max_candidates_per_snapshot);
  return snap;
}

SelectionContext FailoverFabric::Impl::build_context(ServiceSlotKey slot) const {
  SelectionContext ctx;
  ctx.now_receipt_seq = receipt_seq_;
  if (const SlotState* st = find_slot(slot)) if (st->current) ctx.failed_target = st->current->target;
  for (const FailureEvent& e : evidence.all()) {
    if (e.status == EvidenceStatus::CONFIRMED || e.status == EvidenceStatus::UNKNOWN) {
      if (e.target) ctx.confirmed_failed_targets.push_back(*e.target);
    }
  }
  for (TargetId t : ctx.confirmed_failed_targets) {
    for (FailureDomainId d : domains.domains_of(t)) ctx.failed_domains.push_back(d);
  }
  auto uniq = [](auto& v) { std::sort(v.begin(), v.end()); v.erase(std::unique(v.begin(), v.end()), v.end()); };
  uniq(ctx.confirmed_failed_targets);
  uniq(ctx.failed_domains);
  ctx.fenced_boots = fenced_boots;
  if (auto r = routes.current(slot)) ctx.current_route_gen = r->generation;
  return ctx;
}

CandidateSnapshot FailoverFabric::snapshot(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  return impl_->snapshot_locked(slot);
}

// --------------------------------------------------------------------------- //
// Selection
// --------------------------------------------------------------------------- //
SelectionResult FailoverFabric::select(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  const ServiceDefinition* svc = impl_->service_for_slot(slot);
  if (!svc) return {};
  CandidateSnapshot snap = impl_->snapshot_locked(slot);
  SelectionContext ctx = impl_->build_context(slot);
  return SelectionEngine().select(snap, *svc, impl_->domains, ctx);
}

std::vector<Candidate> FailoverFabric::eligible_candidates(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  const ServiceDefinition* svc = impl_->service_for_slot(slot);
  if (!svc) return {};
  CandidateSnapshot snap = impl_->snapshot_locked(slot);
  SelectionContext ctx = impl_->build_context(slot);
  SelectionResult r = SelectionEngine().select(snap, *svc, impl_->domains, ctx);
  std::vector<Candidate> out;
  if (r.selected) out.push_back(*r.selected);
  out.insert(out.end(), r.evaluated_alternatives.begin(), r.evaluated_alternatives.end());
  return out;
}

// --------------------------------------------------------------------------- //
// Planning (does NOT mutate authoritative assignment)
// --------------------------------------------------------------------------- //
std::optional<FailoverPlan> FailoverFabric::plan_failover(ServiceSlotKey slot) {
  std::shared_lock lk(impl_->mtx);
  const ServiceDefinition* svc = impl_->service_for_slot(slot);
  if (!svc) return std::nullopt;
  CandidateSnapshot snap = impl_->snapshot_locked(slot);
  SelectionContext ctx = impl_->build_context(slot);
  SelectionResult sr = SelectionEngine().select(snap, *svc, impl_->domains, ctx);
  if (!sr.has_selection || !sr.selected) return std::nullopt;
  FailoverPlan plan;
  plan.plan = impl_->plan_counter.next();
  plan.generation = impl_->plan_gen_counter.next();
  plan.slot = slot;
  if (const Impl::SlotState* st = impl_->find_slot(slot)) if (st->current) plan.source_assignment = st->current;
  plan.triggering_evidence = impl_->evidence.failures_for(ctx.failed_target, 4);
  plan.snapshot = snap;
  plan.selected = sr.selected;
  plan.continuity = sr.selected->continuity ? *sr.selected->continuity : ContinuityClass::UNKNOWN;
  plan.policy_gen = svc->policy_generation;
  plan.fencing_requirement = FencingRequirement{true, true, true, true, false,
      "reference coordinator/gateway/worker gates enforce generation-bound authority"};
  plan.recovery_objective = svc->recovery;
  if (auto r = impl_->routes.current(slot)) { plan.requested_route = r->route; plan.requested_route_generation = r->generation.next(); }
  else { plan.requested_route = impl_->route_counter.next(); plan.requested_route_generation = impl_->route_gen_counter.next(); }
  plan.record_milestone(Milestone::INTENT_ONLY);
  return plan;
}
// --------------------------------------------------------------------------- //
// Transactional cutover
// --------------------------------------------------------------------------- //
AttemptResult FailoverFabric::execute_plan(const FailoverPlan& plan, CutoverOps& ops) {
  const ServiceSlotKey slot = plan.slot;
  AttemptResult result;
  std::optional<Assignment> old_assign;
  Assignment new_assign;
  ServiceDefinition svc;
  Candidate selected;
  bool valid = false;
  std::string invalid_reason;
  {
    std::unique_lock lk(impl_->mtx);
    result.attempt = impl_->attempt_counter.next();
    result.generation = impl_->attempt_gen_counter.next();
    result.state = AttemptState::REQUESTED;
    const ServiceDefinition* svcp = impl_->service_for_slot(slot);
    if (!svcp) invalid_reason = "service not registered";
    else if (!plan.selected) invalid_reason = "plan has no selected candidate";
    else {
      svc = *svcp;
      if (const Impl::SlotState* st = impl_->find_slot(slot)) if (st->current) old_assign = st->current;
      if (plan.source_assignment) {
        if (!old_assign || old_assign->id != plan.source_assignment->id ||
            old_assign->generation != plan.source_assignment->generation) invalid_reason = "source assignment changed since planning";
      }
      if (invalid_reason.empty()) {
        CandidateSnapshot snap = impl_->snapshot_locked(slot);
        SelectionContext ctx = impl_->build_context(slot);
        bool still = false;
        for (const Candidate& c : snap.candidates) {
          if (c.facts.target == plan.selected->facts.target && c.facts.worker_boot == plan.selected->facts.worker_boot) {
            Candidate cc = c;
            if (SelectionEngine().hard_eligible(cc, svc, impl_->domains, ctx)) { still = true; selected = c; }
            break;
          }
        }
        if (!still) invalid_reason = "selected candidate failed fresh revalidation";
      }
      if (invalid_reason.empty()) {
        new_assign.id = impl_->next_assignment_id();
        new_assign.generation = impl_->assignment_gen_counter.next();
        new_assign.slot = slot;
        new_assign.target = selected.facts.target;
        new_assign.target_generation = selected.facts.target_generation;
        new_assign.replica = selected.facts.replica;
        new_assign.replica_generation = selected.facts.replica_generation;
        new_assign.engine = selected.facts.engine;
        new_assign.engine_incarnation = selected.facts.engine_incarnation;
        new_assign.readiness_profile = selected.facts.profile;
        new_assign.authority_generation = impl_->last_authority_gen.next();
        new_assign.activation = impl_->activation_counter.next();
        new_assign.activation_generation = impl_->activation_gen_counter.next();
        new_assign.route = plan.requested_route;
        new_assign.route_generation = plan.requested_route_generation;
        new_assign.worker_boot = selected.facts.worker_boot;
        new_assign.state = AssignmentState::AUTHORIZED;
        new_assign.provisory = true;
        for (const CandidateResource& cr : selected.resources) if (cr.resource_class == "cuda_device") { new_assign.resource_claims.push_back(ResourceClaimId::min_id()); break; }
        valid = true;
      }
    }
    if (!valid) {
      impl_->attempt_states.push_back(AttemptState::FAILED);
      result.state = AttemptState::FAILED;
      result.detail = invalid_reason;
      return result;
    }
    impl_->attempt_states.push_back(AttemptState::EVALUATING);
    result.reached = Milestone::INTENT_ONLY;
  }

  const auto t0 = impl_->clock->now();

  // ---- Fence old admission ----
  FenceOutcome fo;
  if (old_assign) fo = ops.fence_old(*old_assign, selected.facts.worker_boot);
  else { fo.applied = true; fo.detail = "no old assignment to fence"; }
  {
    std::unique_lock lk(impl_->mtx);
    if (fo.fencing_unproven) { impl_->attempt_states.push_back(AttemptState::FAILED); result.state = AttemptState::FAILED; result.detail = "FENCING_UNPROVEN: cannot enforce fencing at a mandated boundary"; return result; }
    if (!fo.applied) { impl_->attempt_states.push_back(AttemptState::FAILED); result.state = AttemptState::FAILED; result.detail = "fence not applied: " + fo.detail; return result; }
    if (old_assign) impl_->add_fenced_boot(old_assign->worker_boot);
    result.reached = Milestone::OLD_ADMISSION_FENCED;
    result.state = AttemptState::FENCING;
  }

  // ---- Authorize + activate replacement ----
  WorkerAuthorization auth = make_auth(impl_->epoch, new_assign, selected, RequestId(1), ExecutionId(1), "serve");
  ActivateOutcome ao = ops.activate(selected, auth, new_assign.authority_generation);
  {
    std::unique_lock lk(impl_->mtx);
    if (!ao.acknowledged) { impl_->attempt_states.push_back(AttemptState::RECOVERY_REQUIRED); result.state = AttemptState::RECOVERY_REQUIRED; result.detail = "activation not acknowledged: " + ao.detail; return result; }
    result.reached = Milestone::REPLACEMENT_AUTHORIZED;
    result.state = AttemptState::PROMOTING;
    result.reached = Milestone::ACTIVATION_ACKNOWLEDGED;
    result.state = AttemptState::CUTTING_OVER;
  }

  // ---- Install route ----
  RouteEntry re;
  re.route = new_assign.route;
  re.generation = new_assign.route_generation;
  re.slot = slot;
  re.target = new_assign.target;
  re.target_generation = new_assign.target_generation;
  re.incarnation = selected.facts.engine_incarnation;
  re.permitted_boot = selected.facts.worker_boot;
  re.assignment = new_assign.id;
  re.assignment_generation = new_assign.generation;
  re.epoch = impl_->epoch;
  re.gateway_boot = impl_->gateway_boot;
  re.state = RouteState::ACKNOWLEDGED;
  re.transport = "pipes/" + std::to_string(selected.facts.target.value());
  RouteOutcome ro = ops.install_route(re, impl_->gateway_boot);
  {
    std::unique_lock lk(impl_->mtx);
    if (!ro.acknowledged) { impl_->attempt_states.push_back(AttemptState::RECOVERY_REQUIRED); result.state = AttemptState::RECOVERY_REQUIRED; result.detail = "route not acknowledged: " + ro.detail; return result; }
    impl_->routes.install(re);
    result.reached = Milestone::ROUTE_INSTALLED;
    result.state = AttemptState::VERIFYING;
  }

  // ---- Verify through the route ----
  VerifyOutcome vo = ops.verify(re, auth);
  {
    std::unique_lock lk(impl_->mtx);
    if (!vo.verified) { impl_->attempt_states.push_back(AttemptState::RECOVERY_REQUIRED); result.state = AttemptState::RECOVERY_REQUIRED; result.detail = "verification failed: " + vo.detail; return result; }
    Impl::SlotState* st = impl_->find_slot(slot);
    if (!st) { impl_->slots.push_back(Impl::SlotState{slot, std::nullopt, {}}); st = &impl_->slots.back(); }
    // Supersession guard: if a newer attempt already committed a different current
    // assignment, this attempt must not commit (compensation never clears a newer attempt).
    if (old_assign) {
      if (st->current && !(st->current->id == old_assign->id && st->current->generation == old_assign->generation)) {
        impl_->attempt_states.push_back(AttemptState::SUPERSEDED);
        result.state = AttemptState::SUPERSEDED;
        result.detail = "attempt superseded by a newer promotion";
        return result;
      }
    }
    if (st->current) { st->current->state = AssignmentState::FENCED; st->history.push_back(*st->current); }
    else if (old_assign) { Assignment o = *old_assign; o.state = AssignmentState::FENCED; st->history.push_back(o); }
    if (old_assign) impl_->add_fenced_boot(old_assign->worker_boot);
    new_assign.state = AssignmentState::ACTIVE;
    new_assign.provisory = false;
    st->current = new_assign;
    impl_->last_authority_gen = new_assign.authority_generation;
    result.reached = Milestone::VERIFICATION_COMPLETE;
    result.state = AttemptState::COMPLETED;
    result.recovery_verified = true;
    result.committed_assignment = new_assign;
    impl_->attempt_states.push_back(AttemptState::COMPLETED);
  }
  auto t1 = impl_->clock->now();
  result.elapsed_ms = (std::uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  return result;
}
// --------------------------------------------------------------------------- //
// Requests
// --------------------------------------------------------------------------- //
void FailoverFabric::record_request(RequestRecord r) { std::unique_lock lk(impl_->mtx); impl_->requests.record(std::move(r)); }
RequestTracker::LateOutcome FailoverFabric::classify_late_result(RequestId id, const WorkerAuthorization& auth) {
  std::unique_lock lk(impl_->mtx);
  return impl_->requests.classify_late_result(id, auth);
}
std::size_t FailoverFabric::ambiguous_count() const noexcept { std::shared_lock lk(impl_->mtx); return impl_->requests.ambiguous_count(); }
std::size_t FailoverFabric::rejected_late_count() const noexcept { std::shared_lock lk(impl_->mtx); return impl_->requests.rejected_late_count(); }

// --------------------------------------------------------------------------- //
// Gateway authorization (coordinator validation per dispatch / result commit)
// --------------------------------------------------------------------------- //
GateDecision FailoverFabric::dispatch_allowed_locked(const WorkerAuthorization& auth) const {
  const Impl::SlotState* st = impl_->find_slot(auth.slot);
  if (!st || !st->current) return GateDecision{false, "no current assignment for slot", true};
  const Assignment& cur = *st->current;
  if (auth.epoch != impl_->epoch) return GateDecision{false, "stale coordinator epoch", true};
  if (auth.assignment != cur.id || auth.assignment_generation != cur.generation)
    return GateDecision{false, "stale assignment generation", true};
  if (auth.route_generation != cur.route_generation) return GateDecision{false, "stale route generation", true};
  // Conservative restart: a route that requires revalidation must not authorize dispatch.
  if (auto rt = impl_->routes.current(auth.slot)) {
    if (rt->state == RouteState::REVALIDATION_REQUIRED || rt->state == RouteState::EMPTY ||
        rt->state == RouteState::STALE) {
      return GateDecision{false, "route requires revalidation", true};
    }
  }
  if (auth.worker_boot != cur.worker_boot) return GateDecision{false, "worker boot not current", true};
  if (impl_->boot_fenced(auth.worker_boot)) return GateDecision{false, "fenced worker boot", true};
  if (cur.state != AssignmentState::ACTIVE) return GateDecision{false, "assignment not active", true};
  return GateDecision{true, "authorized", false};
}

GateDecision FailoverFabric::authorize_dispatch(const WorkerAuthorization& auth) const {
  std::shared_lock lk(impl_->mtx);
  return dispatch_allowed_locked(auth);
}

GateDecision FailoverFabric::authorize_result(const WorkerAuthorization& auth) const {
  std::shared_lock lk(impl_->mtx);
  GateDecision d = dispatch_allowed_locked(auth);
  if (!d.allowed) return d;
  auto rec = impl_->requests.find(auth.request);
  if (!rec) return GateDecision{false, "unknown request", true};
  if (rec->disposition == RequestDisposition::RESPONSE_COMMITTED) return GateDecision{false, "response already committed", true};
  return GateDecision{true, "authorized result", false};
}

// --------------------------------------------------------------------------- //
// Routes
// --------------------------------------------------------------------------- //
bool FailoverFabric::install_route(RouteEntry entry, GatewayBootId boot) {
  std::unique_lock lk(impl_->mtx);
  if (entry.epoch != impl_->epoch) return false;
  entry.state = RouteState::ACKNOWLEDGED;
  entry.gateway_boot = boot;
  return impl_->routes.install(std::move(entry));
}
std::optional<RouteEntry> FailoverFabric::current_route(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  auto r = impl_->routes.current(slot);
  if (r && r->state == RouteState::EMPTY) return std::nullopt;
  return r;
}
std::vector<RouteEntry> FailoverFabric::route_history(ServiceSlotKey slot, std::size_t max) const {
  std::shared_lock lk(impl_->mtx);
  return impl_->routes.history(slot, max);
}
void FailoverFabric::mark_routes_revalidation() { std::unique_lock lk(impl_->mtx); impl_->routes.mark_revalidation_required(); }
std::size_t FailoverFabric::revalidation_required_count() const noexcept { std::shared_lock lk(impl_->mtx); return impl_->routes.revalidation_required_count(); }

// --------------------------------------------------------------------------- //
// Introspection
// --------------------------------------------------------------------------- //
ServiceOutcome FailoverFabric::service_outcome(ServiceSlotKey slot) const {
  std::shared_lock lk(impl_->mtx);
  const Impl::SlotState* st = impl_->find_slot(slot);
  if (!st || !st->current) return ServiceOutcome::UNAVAILABLE;
  auto r = impl_->routes.current(slot);
  if (r && r->state == RouteState::REVALIDATION_REQUIRED) return ServiceOutcome::REVALIDATION_REQUIRED;
  if (st->current->state == AssignmentState::ACTIVE && r && r->state == RouteState::ACKNOWLEDGED) {
    if (impl_->evidence.latest_failure_for(st->current->target)) return ServiceOutcome::DEGRADED;
    return ServiceOutcome::AVAILABLE;
  }
  if (st->current->state == AssignmentState::DRAINING || st->current->state == AssignmentState::REVALIDATION_REQUIRED)
    return ServiceOutcome::REVALIDATION_REQUIRED;
  return ServiceOutcome::UNAVAILABLE;
}

std::vector<AttemptState> FailoverFabric::attempt_history() const { std::shared_lock lk(impl_->mtx); return impl_->attempt_states; }

// --------------------------------------------------------------------------- //
// Persistence
// --------------------------------------------------------------------------- //
void FailoverFabric::save(const std::string& path) const {
  PersistenceSnapshot snap;
  {
    std::shared_lock lk(impl_->mtx);
    snap.epoch = impl_->epoch;
    snap.policy_generation = impl_->policy_gen;
    snap.last_authority_gen = impl_->last_authority_gen;
    snap.services = impl_->services;
    for (const Impl::SlotState& st : impl_->slots) {
      if (st.current) snap.assignments.push_back(*st.current);
      snap.assignments.insert(snap.assignments.end(), st.history.begin(), st.history.end());
    }
    snap.domains = impl_->domains.all_domains();
    snap.memberships = impl_->domains.all_memberships();
    snap.evidence = impl_->evidence.all();
    snap.routes = impl_->routes.all_routes();
    snap.fenced_boots = impl_->fenced_boots;
  }
  persistence::save_file(path, snap);
}

void FailoverFabric::load(const std::string& path) {
  PersistenceSnapshot snap = persistence::load_file(path);
  std::unique_lock lk(impl_->mtx);
  impl_->services = std::move(snap.services);
  impl_->epoch = snap.epoch;
  impl_->policy_gen = snap.policy_generation;
  impl_->last_authority_gen = snap.last_authority_gen;
  impl_->fenced_boots = std::move(snap.fenced_boots);
  impl_->slots.clear();
  for (Assignment& a : snap.assignments) {
    Impl::SlotState* st = impl_->find_slot(a.slot);
    if (!st) { impl_->slots.push_back(Impl::SlotState{a.slot, std::nullopt, {}}); st = &impl_->slots.back(); }
    if (a.state == AssignmentState::ACTIVE) st->current = std::move(a);
    else st->history.push_back(std::move(a));
  }
  impl_->domains = DomainRegistry();
  for (DomainDecl& d : snap.domains) impl_->domains.declare(d);
  for (DomainMembership& m : snap.memberships) impl_->domains.set_membership(m);
  impl_->evidence = EvidenceStore();
  for (FailureEvent& e : snap.evidence) impl_->evidence.publish(std::move(e));
  impl_->routes = RouteTable();
  for (RouteEntry& r : snap.routes) impl_->routes.install(std::move(r));
  impl_->routes.mark_revalidation_required();
  for (Impl::TargetRecord& t : impl_->targets) if (t.facts) { t.facts->ready = false; t.facts->evidence_fresh = false; }
}

std::vector<std::string> FailoverFabric::validate_file(const std::string& path) { return persistence::validate_file(path); }

}  // namespace failover_fabric