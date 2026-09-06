// coordinator_main.cpp — reference coordinator built on the Conn transport.
#include "net/socket_util.hpp"
#include "net/common.hpp"
#include "net/conn.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

using namespace failover_fabric;
using net::Conn;
using sock = net::socket_t;
using net::kInvalidSocket;

namespace {
std::atomic<bool> g_stop{false};
std::mutex g_allmu;
std::vector<std::shared_ptr<Conn>> g_all;
std::uint64_t g_next_gen{1};
std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }
std::mutex g_pmu;
struct Peer {
  std::uint64_t gen{0};
  std::uint8_t role{0};
  std::uint64_t target{0};
  std::uint64_t boot{0};
  std::uint16_t req_port{0};
  std::shared_ptr<Conn> conn;
};
std::map<std::uint64_t, std::shared_ptr<Peer>> g_workers;   // target -> peer
std::map<std::uint64_t, std::shared_ptr<Peer>> g_gateways;  // gateway boot -> peer
std::shared_ptr<Conn> worker_conn(std::uint64_t target) {
  std::lock_guard<std::mutex> lk(g_pmu);
  auto it = g_workers.find(target);
  return it != g_workers.end() ? it->second->conn : nullptr;
}
std::uint16_t worker_req_port(std::uint64_t target) {
  std::lock_guard<std::mutex> lk(g_pmu);
  auto it = g_workers.find(target);
  return it != g_workers.end() ? it->second->req_port : 0;
}
std::shared_ptr<Conn> any_gateway_conn() {
  std::lock_guard<std::mutex> lk(g_pmu);
  return g_gateways.empty() ? nullptr : g_gateways.begin()->second->conn;
}
}  // namespace

struct SocketOps : CutoverOps {
  FailoverFabric* fab;
  FenceOutcome fence_old(const Assignment& old, WorkerBootId) override {
    FenceOutcome fo; fo.applied = true; fo.state = FenceState::ENFORCED;
    auto c = worker_conn(old.target.value());
    if (c && c->alive()) { PayloadWriter w; w.u64(ex::fid::target, old.target.value());
      c->send(MsgType::FENCE_ASSIGNMENT, 9101, fab->current_epoch().value(), w.finish()); }
    fo.detail = "old worker fenced";
    return fo;
  }
  ActivateOutcome activate(const Candidate& cand, const WorkerAuthorization&, ServiceAuthorityGeneration) override {
    ActivateOutcome ao;
    auto c = worker_conn(cand.facts.target.value());
    if (!c || !c->alive()) { ao.detail = "worker not reachable"; return ao; }
    PayloadWriter w; w.u64(ex::fid::target, cand.facts.target.value());
    w.u64(ex::fid::boot, cand.facts.worker_boot.value());
    w.u64(ex::fid::incarnation, cand.facts.engine_incarnation.value());
    auto resp = c->request(MsgType::ACTIVATE_TARGET, 9102, fab->current_epoch().value(), w.finish());
    if (resp && resp->type == MsgType::ACTIVATE_RESULT) { PayloadReader rr(resp->payload); ao.acknowledged = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok); }
    if (!ao.acknowledged) ao.detail = "no activation acknowledgment";
    return ao;
  }
  RouteOutcome install_route(const RouteEntry& e, GatewayBootId boot) override {
    RouteOutcome ro;
    std::shared_ptr<Conn> c;
    { std::lock_guard<std::mutex> lk(g_pmu); auto it = g_gateways.find(boot.value()); if (it != g_gateways.end()) c = it->second->conn; }
    if (!c || !c->alive()) { ro.detail = "gateway not reachable"; return ro; }
    RouteEntry re = e;
    re.transport = "tcp://127.0.0.1:" + std::to_string(worker_req_port(e.target.value()));
    PayloadWriter w; ex::encode_route_entry(w, re);
    auto resp = c->request(MsgType::INSTALL_ROUTE, 9103, fab->current_epoch().value(), w.finish());
    if (resp && resp->type == MsgType::ROUTE_ACK) { PayloadReader rr(resp->payload); ro.acknowledged = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok); }
    if (!ro.acknowledged) ro.detail = "no route acknowledgment";
    return ro;
  }
  VerifyOutcome verify(const RouteEntry&, const WorkerAuthorization&) override {
    VerifyOutcome vo;
    std::shared_ptr<Conn> c;
    { std::lock_guard<std::mutex> lk(g_pmu); if (!g_gateways.empty()) c = g_gateways.begin()->second->conn; }
    if (!c || !c->alive()) { vo.detail = "gateway not reachable"; return vo; }
    PayloadWriter w; w.u64(ex::fid::input_a, 7); w.u64(ex::fid::input_b, 11); w.str(ex::fid::operation, "verify");
    auto resp = c->request(MsgType::VERIFY_SERVICE, 9104, fab->current_epoch().value(), w.finish());
    if (resp && resp->type == MsgType::VERIFY_SERVICE) { PayloadReader rr(resp->payload); vo.verified = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok);
      if (vo.verified && rr.has(ex::fid::payload)) { auto b = rr.bytes(ex::fid::payload); vo.result.assign((const char*)b.data(), b.size()); } }
    if (!vo.verified) vo.detail = "verification rejected";
    return vo;
  }
};

int main(int argc, char** argv) {
  std::uint16_t port = 0;
  std::string load_path;
  for (int i = 1; i < argc - 1; ++i) {
    if (std::string(argv[i]) == "--port") port = (std::uint16_t)parse_u64(argv[i + 1]);
    else if (std::string(argv[i]) == "--load") load_path = argv[i + 1];
  }
  FailoverFabric fabric(RuntimeConfig{});
  if (!load_path.empty()) {
    try { fabric.load(load_path); } catch (const std::exception& e) { std::fprintf(stderr, "load failed: %s\n", e.what()); return 2; }
    // Advanced epoch durably on restart; the persisted epoch is never reused as current.
    auto old = fabric.current_epoch();
    fabric.set_coordinator_identity(CoordinatorId(1), old.value() + 1);
    std::fprintf(stderr, "coordinator restarted epoch=%llu revalidation=%zu\n", (unsigned long long)fabric.current_epoch().value(), fabric.revalidation_required_count());
  } else {
    fabric.set_coordinator_identity(CoordinatorId(1), 1);
  }
  sock listen = net::tcp_listen(port);
  if (listen == kInvalidSocket) { std::fprintf(stderr, "coordinator: cannot bind\n"); return 1; }
  std::printf("PORT %u\n", (unsigned)net::tcp_listen_port(listen)); std::fflush(stdout);

  while (!g_stop.load()) {
    sock c = net::tcp_accept(listen);
    if (c == kInvalidSocket) continue;
    auto p = std::make_shared<Peer>();
    p->gen = g_next_gen++;
    p->conn = std::make_shared<Conn>(c, p->gen, [&fabric, p](const Frame& f) {
      try {
        if (f.type == MsgType::HELLO) {
          PayloadReader r(f.payload);
          if (r.has(ex::fid::role)) p->role = r.u8(ex::fid::role);
          if (r.has(ex::fid::target)) p->target = r.u64(ex::fid::target);
          if (r.has(ex::fid::boot)) p->boot = r.u64(ex::fid::boot);
          if (r.has(ex::fid::req_port)) p->req_port = (std::uint16_t)r.u64(ex::fid::req_port);
          if (p->role == ex::kRoleWorker) { std::lock_guard<std::mutex> lk(g_pmu); g_workers[p->target] = p; }
          else if (p->role == ex::kRoleGateway) { std::lock_guard<std::mutex> lk(g_pmu); g_gateways[p->boot] = p; fabric.set_gateway_boot(GatewayBootId(p->boot));
            // Push current routes to a (re)registered gateway so it can serve after restart.
            for (auto sv : fabric.services()) { ServiceSlotKey sl{sv.service, ServiceSlotId(1)}; if (auto rt = fabric.current_route(sl)) { PayloadWriter rw; ex::encode_route_entry(rw, *rt); p->conn->send(MsgType::INSTALL_ROUTE, 2121, fabric.current_epoch().value(), rw.finish()); } } }
        }
        else if (f.type == MsgType::PUBLISH_READINESS) {
          PayloadReader r(f.payload);
          TargetId tgt(r.u64(ex::fid::target)); WorkerBootId b(r.u64(ex::fid::boot));
          bool ready = r.bool_(ex::fid::ready);
          std::string model = r.has(ex::fid::model) ? r.str(ex::fid::model) : "ref-det-sequence-v1";
          std::string abi = r.has(ex::fid::abi) ? r.str(ex::fid::abi) : "ff-ref-v1";
          double cap = r.has(ex::fid::capacity) ? r.f64(ex::fid::capacity) : 1.0;
          std::string transport = "tcp://127.0.0.1:" + std::to_string(p->req_port);
          fabric.register_target(tgt, transport);
          CandidateFacts cf; cf.target = tgt; cf.target_generation = TargetGeneration::first();
          cf.replica = ReplicaId(tgt.value()); cf.engine = EngineId(tgt.value()); cf.engine_incarnation = EngineIncarnationId(tgt.value());
          cf.worker = WorkerId(tgt.value()); cf.worker_boot = b; cf.profile = ReadinessProfileId(1);
          cf.model_key = model; cf.runtime_abi = abi; cf.ready = ready; cf.activation_eligible = ready; cf.evidence_fresh = true;
          fabric.publish_candidate_facts(cf); fabric.publish_candidate_state(tgt, CandidateState{});
          fabric.publish_candidate_resources(tgt, { CandidateResource{"cpu", cap, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, (std::uint64_t)cap} });
        }
        else if (f.type == MsgType::REGISTER) {
          PayloadReader r(f.payload);
          ServiceId sid(r.u64(ex::fid::service)); TargetId act(r.u64(ex::fid::target)); WorkerBootId ab(r.u64(ex::fid::boot));
          ServiceDefinition svc; svc.service = sid; svc.generation = ServiceGeneration::first();
          svc.config_generation = ServiceConfigGeneration::first(); svc.name = "ref"; svc.role = ServingRole::EXCLUSIVE_ACTIVE;
          svc.compatibility.model_key = "ref-det-sequence-v1"; svc.compatibility.runtime_abi = "ff-ref-v1";
          svc.domain_requirement.require_domain_independence = false;
          svc.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
          svc.recovery.rto_ms = 10000; svc.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
          svc.recovery.requires_verified_recovery = true; svc.failback = FailbackPolicy::MANUAL; svc.policy_generation = PolicyGeneration::first();
          try {
            fabric.create_service(svc);
            ServiceSlotKey slot{sid, ServiceSlotId(1)};
            Assignment a; a.id = AssignmentId(1); a.generation = AssignmentGeneration::first(); a.slot = slot;
            a.target = act; a.target_generation = TargetGeneration::first(); a.worker_boot = ab;
            a.authority_generation = ServiceAuthorityGeneration::first(); a.state = AssignmentState::ACTIVE;
            fabric.set_initial_assignment(a);
            auto wc = worker_conn(act.value());
            if (wc && wc->alive()) { PayloadWriter aw; aw.u64(ex::fid::target, act.value()); aw.u64(ex::fid::boot, ab.value());
              wc->request(MsgType::ACTIVATE_TARGET, 9110, fabric.current_epoch().value(), aw.finish()); }
            // Install the initial route to the gateway so it can serve before any failover.
            {
              auto gc = any_gateway_conn();
              if (gc && gc->alive()) {
                RouteEntry re; re.route = RouteId(1); re.generation = RouteGeneration::first();
                re.slot = {sid, ServiceSlotId(1)}; re.target = act; re.target_generation = TargetGeneration::first();
                re.incarnation = EngineIncarnationId(act.value()); re.permitted_boot = ab;
                re.assignment = AssignmentId(1); re.assignment_generation = AssignmentGeneration::first();
                re.epoch = fabric.current_epoch(); re.gateway_boot = fabric.gateway_boot(); re.state = RouteState::ACKNOWLEDGED;
                re.transport = "tcp://127.0.0.1:" + std::to_string(worker_req_port(act.value()));
                PayloadWriter rw; ex::encode_route_entry(rw, re);
                gc->request(MsgType::INSTALL_ROUTE, 9111, fabric.current_epoch().value(), rw.finish());
                fabric.install_route(re, fabric.gateway_boot());
              }
            }
            PayloadWriter w; w.bool_(ex::fid::ok, true);
            p->conn->send(MsgType::REGISTER, f.msg_id, f.epoch, w.finish());
          } catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); p->conn->send(MsgType::ERROR_MSG, f.msg_id, f.epoch, w.finish()); }
        }
        else if (f.type == MsgType::EXECUTE_FAILOVER) {
          PayloadReader r(f.payload);
          ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
          auto plan = fabric.plan_failover(slot);
          if (!plan) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,"no eligible replacement"); p->conn->send(MsgType::EXECUTE_FAILOVER, f.msg_id, f.epoch, w.finish()); return; }
          SocketOps ops; ops.fab = &fabric;
          AttemptResult ar = fabric.execute_plan(*plan, ops);
          PayloadWriter w; w.bool_(ex::fid::ok, ar.state == AttemptState::COMPLETED && ar.recovery_verified);
          w.u8(ex::fid::state, (std::uint8_t)ar.state); w.str(ex::fid::detail, ar.detail);
          if (auto cur = fabric.current_assignment(slot)) w.u64(ex::fid::target, cur->target.value());
          p->conn->send(MsgType::EXECUTE_FAILOVER, f.msg_id, f.epoch, w.finish());
        }
        else if (f.type == MsgType::PUBLISH_FAILURE) {
          PayloadReader r(f.payload);
          TargetId tgt(r.u64(ex::fid::target)); FailureCategory cat = (FailureCategory)r.u8(ex::fid::category);
          FailureEvent ev; ev.event_id = FailureEventId(1000 + (int)f.msg_id); ev.generation = FailureGeneration::first();
          ev.status = EvidenceStatus::CONFIRMED; ev.category = cat; ev.target = tgt; ev.source = SourceId(1);
          ev.received.seq = r.u64(ex::fid::seq);
          fabric.publish_failure(ev);
          PayloadWriter w; w.bool_(ex::fid::ok, true); p->conn->send(MsgType::PUBLISH_FAILURE, f.msg_id, f.epoch, w.finish());
        }
        else if (f.type == MsgType::QUERY_ROUTE) {
          PayloadReader r(f.payload); ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
          PayloadWriter w; w.bool_(ex::fid::ok, fabric.current_assignment(slot).has_value());
          if (auto cur = fabric.current_assignment(slot)) w.u64(ex::fid::target, cur->target.value());
          w.bool_(ex::fid::state, fabric.revalidation_required_count() > 0);
          w.u64(ex::fid::seq, fabric.ambiguous_count());
          p->conn->send(MsgType::QUERY_ROUTE, f.msg_id, f.epoch, w.finish());
        }
        else if (f.type == MsgType::AUTHORIZE_REQUEST) {
          PayloadReader r(f.payload);
          ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
          RequestId req(r.u64(ex::fid::request_id));
          PayloadWriter w;
          auto cur = fabric.current_assignment(slot);
          if (!cur || cur->state != AssignmentState::ACTIVE) { w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,"no current active assignment"); }
          else {
            w.bool_(ex::fid::ok, true);
            auto ep = fabric.current_epoch();
            w.u64(ex::fid::epoch, ep.value()); w.u64(ex::fid::epoch_id, ep.id().value()); w.u64(ex::fid::epoch_boot, ep.boot().value());
            w.u64(ex::fid::slot, slot.slot.value()); w.u64(ex::fid::assignment_id, cur->id.value());
            w.u64(ex::fid::assignment_gen, cur->generation.value()); w.u64(ex::fid::route_gen, cur->route_generation.value());
            w.u64(ex::fid::boot, cur->worker_boot.value()); w.u64(ex::fid::incarnation, cur->engine_incarnation.value());
            w.u64(ex::fid::request_id, req.value()); w.u64(ex::fid::execution_id, req.value()); w.str(ex::fid::operation, "serve");
            WorkerAuthorization auth; auth.epoch = ep; auth.slot = slot; auth.assignment = cur->id;
            auth.assignment_generation = cur->generation; auth.route_generation = cur->route_generation;
            auth.incarnation = cur->engine_incarnation; auth.request = req; auth.execution = ExecutionId(req.value());
            auth.operation = "serve"; auth.worker_boot = cur->worker_boot;
            RequestRecord rec; rec.request = req; rec.execution = ExecutionId(req.value());
            rec.execution_generation = ExecutionGeneration::first(); rec.slot = slot;
            rec.disposition = RequestDisposition::DISPATCHED; rec.idempotent = false; rec.authority = IdempotencyAuthority::NONE;
            rec.externally_effectful = false; rec.dispatched_auth = auth; fabric.record_request(rec);
          }
          p->conn->send(MsgType::AUTHORIZE_REQUEST, f.msg_id, f.epoch, w.finish());
        }
        else if (f.type == MsgType::AUTHORIZE_RESULT) {
          PayloadReader r(f.payload);
          ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
          WorkerAuthorization auth;
          auth.epoch = CoordinatorEpoch(r.u64(ex::fid::epoch), CoordinatorId(r.u64(ex::fid::epoch_id)), CoordinatorId(r.u64(ex::fid::epoch_boot)));
          auth.slot = slot; auth.assignment = AssignmentId(r.u64(ex::fid::assignment_id));
          auth.assignment_generation = AssignmentGeneration(r.u64(ex::fid::assignment_gen));
          auth.route_generation = RouteGeneration(r.u64(ex::fid::route_gen));
          auth.worker_boot = WorkerBootId(r.u64(ex::fid::boot));
          auth.incarnation = EngineIncarnationId(r.u64(ex::fid::incarnation));
          auth.request = RequestId(r.u64(ex::fid::request_id)); auth.execution = ExecutionId(r.u64(ex::fid::execution_id));
          GateDecision d = fabric.authorize_result(auth);
          PayloadWriter w; w.bool_(ex::fid::ok, d.allowed); if (!d.allowed) w.str(ex::fid::detail, d.reason);
          p->conn->send(MsgType::AUTHORIZE_RESULT, f.msg_id, f.epoch, w.finish());
        }
        else if (f.type == MsgType::SAVE) {
          PayloadReader r(f.payload); std::string path = r.str(ex::fid::detail);
          try { fabric.save(path); PayloadWriter w; w.bool_(ex::fid::ok,true); p->conn->send(MsgType::SAVE, f.msg_id, f.epoch, w.finish()); }
          catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); p->conn->send(MsgType::ERROR_MSG, f.msg_id, f.epoch, w.finish()); }
        }
        else if (f.type == MsgType::SHUTDOWN) { g_stop.store(true); }
      } catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); p->conn->send(MsgType::ERROR_MSG, f.msg_id, f.epoch, w.finish()); }
    }, [&fabric](std::uint64_t) {});
    p->conn->start();
    { std::lock_guard<std::mutex> lk(g_allmu); g_all.push_back(p->conn); }
  }
  net::close_socket(listen);
  { std::lock_guard<std::mutex> lk(g_allmu); for (auto& c : g_all) c->shutdown(); }
  net::cleanup();
  return 0;
}