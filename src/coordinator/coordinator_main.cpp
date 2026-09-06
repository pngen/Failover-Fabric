// coordinator_main.cpp — the reference coordinator process.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <failover_fabric/failover_fabric.hpp>

#include <atomic>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

namespace {
std::atomic<bool> g_stop{false};
std::map<std::uint64_t, std::pair<sock, std::uint16_t>> g_workers;   // target -> (control socket, req_port)
std::map<std::uint64_t, sock> g_gateways;                              // gateway boot -> control socket
std::mutex g_workers_mutex;
std::mutex g_gateway_mutex;
std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }
}

int main(int argc, char** argv) {
  std::uint16_t port = 0;
  for (int i = 1; i < argc - 1; ++i) if (std::string(argv[i]) == "--port") port = (std::uint16_t)parse_u64(argv[i + 1]);
  FailoverFabric fabric(RuntimeConfig{});
  sock listen = net::tcp_listen(port);
  if (listen == kInvalidSocket) { std::fprintf(stderr, "coordinator: cannot bind\n"); return 1; }
  std::printf("PORT %u\n", (unsigned)net::tcp_listen_port(listen));
  std::fflush(stdout);

  auto handle = [&](sock c) {
    auto hello = net::recv_frame(c);
    if (!hello) { net::close_socket(c); return; }
    std::uint64_t boot = 0, target = 0, req_port = 0; std::uint8_t role = 0;
    {
      PayloadReader r(hello->payload);
      if (r.has(ex::fid::boot)) boot = r.u64(ex::fid::boot);
      if (r.has(ex::fid::target)) target = r.u64(ex::fid::target);
      if (r.has(ex::fid::req_port)) req_port = r.u64(ex::fid::req_port);
      if (r.has(ex::fid::role)) role = r.u8(ex::fid::role);
    }
    if (role == ex::kRoleWorker) {
      // Read the worker's one-shot PUBLISH_READINESS, then keep its control socket for
      // on-demand operations (no persistent reader that would steal operation replies).
      {
        auto rd = net::recv_frame(c);
        if (rd && rd->type == MsgType::PUBLISH_READINESS) {
          PayloadReader r(rd->payload);
          TargetId tgt(r.u64(ex::fid::target)); WorkerBootId b(r.u64(ex::fid::boot));
          bool ready = r.bool_(ex::fid::ready);
          std::string model = r.has(ex::fid::model) ? r.str(ex::fid::model) : "ref-det-sequence-v1";
          std::string abi = r.has(ex::fid::abi) ? r.str(ex::fid::abi) : "ff-ref-v1";
          double cap = r.has(ex::fid::capacity) ? r.f64(ex::fid::capacity) : 1.0;
          std::string transport = "tcp://127.0.0.1:" + std::to_string(req_port);
          try {
            fabric.register_target(tgt, transport);
            CandidateFacts cf; cf.target = tgt; cf.target_generation = TargetGeneration::first();
            cf.replica = ReplicaId(tgt.value()); cf.engine = EngineId(tgt.value());
            cf.engine_incarnation = EngineIncarnationId(tgt.value());
            cf.worker = WorkerId(tgt.value()); cf.worker_boot = b; cf.profile = ReadinessProfileId(1);
            cf.model_key = model; cf.runtime_abi = abi; cf.ready = ready; cf.activation_eligible = ready; cf.evidence_fresh = true;
            fabric.publish_candidate_facts(cf);
            fabric.publish_candidate_state(tgt, CandidateState{});
            fabric.publish_candidate_resources(tgt, { CandidateResource{"cpu", cap, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, (std::uint64_t)cap} });
          } catch (...) {}
        }
      }
      { std::lock_guard<std::mutex> lk(g_workers_mutex); g_workers[target] = {c, (std::uint16_t)req_port}; }
      return;
    }
    if (role == ex::kRoleGateway) {
      { std::lock_guard<std::mutex> lk(g_gateway_mutex); g_gateways[boot] = c; }
      fabric.set_gateway_boot(GatewayBootId(boot));
      return;
    }

    while (!g_stop.load()) {
      auto f = net::recv_frame(c);
      if (!f) break;
      const MsgType mt = f->type;
      if (mt == MsgType::SHUTDOWN) { g_stop.store(true); break; }
      try {
      if (mt == MsgType::PUBLISH_READINESS) {
        PayloadReader r(f->payload);
        TargetId tgt(r.u64(ex::fid::target)); WorkerBootId b(r.u64(ex::fid::boot));
        bool ready = r.bool_(ex::fid::ready);
        std::string model = r.has(ex::fid::model) ? r.str(ex::fid::model) : "ref-det-sequence-v1";
        std::string abi = r.has(ex::fid::abi) ? r.str(ex::fid::abi) : "ff-ref-v1";
        double cap = r.has(ex::fid::capacity) ? r.f64(ex::fid::capacity) : 1.0;
        std::string transport = "tcp://127.0.0.1:" + std::to_string([&]{ std::lock_guard<std::mutex> lk(g_workers_mutex); auto it=g_workers.find(tgt.value()); return it!=g_workers.end()?it->second.second:0; }());
        try {
          fabric.register_target(tgt, transport);
          CandidateFacts cf; cf.target = tgt; cf.target_generation = TargetGeneration::first();
          cf.replica = ReplicaId(tgt.value()); cf.engine = EngineId(tgt.value());
          cf.engine_incarnation = EngineIncarnationId(tgt.value());
          cf.worker = WorkerId(tgt.value()); cf.worker_boot = b; cf.profile = ReadinessProfileId(1);
          cf.model_key = model; cf.runtime_abi = abi; cf.ready = ready; cf.activation_eligible = ready; cf.evidence_fresh = true;
          fabric.publish_candidate_facts(cf);
          fabric.publish_candidate_state(tgt, CandidateState{});
          fabric.publish_candidate_resources(tgt, { CandidateResource{"cpu", cap, CapacityGeneration::first(), {}, false, std::nullopt, ReservationGeneration::null(), false, (std::uint64_t)cap} });
          PayloadWriter w; w.bool_(ex::fid::ok, true);
          ex::send_msg(c, MsgType::PUBLISH_READINESS, f->msg_id, f->epoch, w.finish());
        } catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); ex::send_msg(c, MsgType::ERROR_MSG, f->msg_id, f->epoch, w.finish()); }
      }
      else if (mt == MsgType::PUBLISH_FAILURE) {
        PayloadReader r(f->payload);
        TargetId tgt(r.u64(ex::fid::target)); FailureCategory cat = (FailureCategory)r.u8(ex::fid::category);
        FailureEvent ev; ev.event_id = FailureEventId(1); ev.generation = FailureGeneration::first();
        ev.status = EvidenceStatus::CONFIRMED; ev.category = cat; ev.target = tgt; ev.source = SourceId(1);
        ev.received.seq = r.u64(ex::fid::seq);
        fabric.publish_failure(ev);
        PayloadWriter w; w.bool_(ex::fid::ok, true);
        ex::send_msg(c, MsgType::PUBLISH_FAILURE, f->msg_id, f->epoch, w.finish());
      }
      else if (mt == MsgType::REGISTER) {
        PayloadReader r(f->payload);
        ServiceId sid(r.u64(ex::fid::service)); TargetId act(r.u64(ex::fid::target)); WorkerBootId ab(r.u64(ex::fid::boot));
        ServiceDefinition svc; svc.service = sid; svc.generation = ServiceGeneration::first();
        svc.config_generation = ServiceConfigGeneration::first(); svc.name = "ref"; svc.role = ServingRole::EXCLUSIVE_ACTIVE;
        svc.compatibility.model_key = "ref-det-sequence-v1"; svc.compatibility.runtime_abi = "ff-ref-v1";
        svc.domain_requirement.require_domain_independence = false;
        svc.resources.push_back(ResourceRequirement{"cpu", 1.0, true});
        svc.recovery.rto_ms = 10000; svc.recovery.continuity = ContinuityClass::REPLAY_SAFE_REQUESTS;
        svc.recovery.requires_verified_recovery = true; svc.failback = FailbackPolicy::MANUAL;
        svc.policy_generation = PolicyGeneration::first();
        try {
          fabric.create_service(svc);
          ServiceSlotKey slot{sid, ServiceSlotId(1)};
          Assignment a; a.id = AssignmentId(1); a.generation = AssignmentGeneration::first(); a.slot = slot;
          a.target = act; a.target_generation = TargetGeneration::first(); a.worker_boot = ab;
          a.authority_generation = ServiceAuthorityGeneration::first(); a.state = AssignmentState::ACTIVE;
          fabric.set_initial_assignment(a);
          // Install the initial route to the gateway so it can serve before any failover.
          {
            sock gs = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_gateway_mutex); if (!g_gateways.empty()) gs = g_gateways.begin()->second; }
            if (gs != kInvalidSocket) {
              RouteEntry re; re.route = RouteId(1); re.generation = RouteGeneration::first();
              re.slot = {sid, ServiceSlotId(1)}; re.target = act; re.target_generation = TargetGeneration::first();
              re.incarnation = EngineIncarnationId(act.value()); re.permitted_boot = ab;
              re.assignment = AssignmentId(1); re.assignment_generation = AssignmentGeneration::first();
              re.epoch = fabric.current_epoch(); re.gateway_boot = fabric.gateway_boot();
              std::uint16_t wport = 0;
              { std::lock_guard<std::mutex> lk(g_workers_mutex); auto it = g_workers.find(act.value()); if (it != g_workers.end()) wport = it->second.second; }
              re.transport = "tcp://127.0.0.1:" + std::to_string(wport);
              re.state = RouteState::ACKNOWLEDGED;
              PayloadWriter rw; ex::encode_route_entry(rw, re);
              if (ex::send_msg(gs, MsgType::INSTALL_ROUTE, 9003, fabric.current_epoch().value(), rw.finish())) {
                auto rlp = net::recv_frame(gs);
                if (rlp && rlp->type == MsgType::ROUTE_ACK) fabric.install_route(re, fabric.gateway_boot());
              }
            }
          }
          // Activate worker A so it serves the initial assignment.
          {
            sock aws = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_workers_mutex); auto it = g_workers.find(act.value()); if (it != g_workers.end()) aws = it->second.first; }
            if (aws != kInvalidSocket) {
              PayloadWriter aw; aw.u64(ex::fid::target, act.value()); aw.u64(ex::fid::boot, ab.value()); aw.u64(ex::fid::incarnation, act.value());
              if (ex::send_msg(aws, MsgType::ACTIVATE_TARGET, 9010, fabric.current_epoch().value(), aw.finish())) {
                auto resp = net::recv_frame(aws);
                if (resp && resp->type == MsgType::ACTIVATE_RESULT) { PayloadReader qr(resp->payload); if (qr.has(ex::fid::ok)) qr.bool_(ex::fid::ok); }
              }
            }
          }
          PayloadWriter w; w.bool_(ex::fid::ok, true);
          ex::send_msg(c, MsgType::REGISTER, f->msg_id, f->epoch, w.finish());
        } catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); ex::send_msg(c, MsgType::ERROR_MSG, f->msg_id, f->epoch, w.finish()); }
      }
      else if (mt == MsgType::EXECUTE_FAILOVER) {
        PayloadReader r(f->payload);
        ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
        auto plan = fabric.plan_failover(slot);
        if (!plan) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,"no eligible replacement"); ex::send_msg(c, MsgType::EXECUTE_FAILOVER, f->msg_id, f->epoch, w.finish()); continue; }
        struct SocketOps : CutoverOps {
          FailoverFabric* fab;
          FenceOutcome fence_old(const Assignment& old, WorkerBootId) override {
            FenceOutcome fo; fo.applied = true; fo.state = FenceState::ENFORCED;
            sock ws = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_workers_mutex); auto it = g_workers.find(old.target.value()); if (it != g_workers.end()) ws = it->second.first; }
            if (ws != kInvalidSocket) { PayloadWriter w; w.u64(ex::fid::target, old.target.value()); ex::send_msg(ws, MsgType::FENCE_ASSIGNMENT, 9001, fab->current_epoch().value(), w.finish()); }
            fo.detail = "old worker fenced"; return fo;
          }
          ActivateOutcome activate(const Candidate& c, const WorkerAuthorization&, ServiceAuthorityGeneration) override {
            ActivateOutcome ao; sock ws = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_workers_mutex); auto it = g_workers.find(c.facts.target.value()); if (it != g_workers.end()) ws = it->second.first; }
            if (ws == kInvalidSocket) { ao.detail = "worker not reachable"; return ao; }
            PayloadWriter w; w.u64(ex::fid::target, c.facts.target.value()); w.u64(ex::fid::boot, c.facts.worker_boot.value());
            w.u64(ex::fid::incarnation, c.facts.engine_incarnation.value());
            if (ex::send_msg(ws, MsgType::ACTIVATE_TARGET, 9002, fab->current_epoch().value(), w.finish())) {
              auto resp = net::recv_frame(ws);
              if (resp && resp->type == MsgType::ACTIVATE_RESULT) { PayloadReader rr(resp->payload); ao.acknowledged = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok); }
            }
            if (!ao.acknowledged) ao.detail = "no activation acknowledgment";
            return ao;
          }
          RouteOutcome install_route(const RouteEntry& e, GatewayBootId boot) override {
            RouteOutcome ro; sock gs = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_gateway_mutex); auto it = g_gateways.find(boot.value()); if (it != g_gateways.end()) gs = it->second; }
            if (gs == kInvalidSocket) { ro.detail = "gateway not reachable"; return ro; }
            RouteEntry re = e;
            { std::lock_guard<std::mutex> lk(g_workers_mutex); auto it = g_workers.find(e.target.value()); if (it != g_workers.end()) re.transport = "tcp://127.0.0.1:" + std::to_string(it->second.second); }
            PayloadWriter w; ex::encode_route_entry(w, re);
            if (ex::send_msg(gs, MsgType::INSTALL_ROUTE, 9003, fab->current_epoch().value(), w.finish())) {
              auto resp = net::recv_frame(gs);
              if (resp && resp->type == MsgType::ROUTE_ACK) { PayloadReader rr(resp->payload); ro.acknowledged = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok); }
            }
            if (!ro.acknowledged) ro.detail = "no route acknowledgment";
            return ro;
          }
          VerifyOutcome verify(const RouteEntry&, const WorkerAuthorization&) override {
            VerifyOutcome vo; sock gs = kInvalidSocket;
            { std::lock_guard<std::mutex> lk(g_gateway_mutex); if (!g_gateways.empty()) gs = g_gateways.begin()->second; }
            if (gs == kInvalidSocket) { vo.detail = "gateway not reachable"; return vo; }
            PayloadWriter w; w.u64(ex::fid::input_a, 7); w.u64(ex::fid::input_b, 11); w.str(ex::fid::operation, "verify");
            if (ex::send_msg(gs, MsgType::VERIFY_SERVICE, 9004, fab->current_epoch().value(), w.finish())) {
              auto resp = net::recv_frame(gs);
              if (resp && resp->type == MsgType::VERIFY_SERVICE) {
                PayloadReader rr(resp->payload);
                vo.verified = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok);
                if (vo.verified && rr.has(ex::fid::payload)) { auto b = rr.bytes(ex::fid::payload); vo.result.assign((const char*)b.data(), b.size()); }
              }
            }
            if (!vo.verified) vo.detail = "verification rejected";
            return vo;
          }
        };
        SocketOps ops; ops.fab = &fabric;
        AttemptResult ar = fabric.execute_plan(*plan, ops);
        PayloadWriter w; w.bool_(ex::fid::ok, ar.state == AttemptState::COMPLETED && ar.recovery_verified);
        w.u8(ex::fid::state, (std::uint8_t)ar.state); w.str(ex::fid::detail, ar.detail);
        if (auto cur = fabric.current_assignment(slot)) w.u64(ex::fid::target, cur->target.value());
        ex::send_msg(c, MsgType::EXECUTE_FAILOVER, f->msg_id, f->epoch, w.finish());
      }
      else if (mt == MsgType::AUTHORIZE_REQUEST) {
        PayloadReader r(f->payload);
        ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
        RequestId req(r.u64(ex::fid::request_id));
        PayloadWriter w;
        auto cur = fabric.current_assignment(slot);
        if (!cur || cur->state != AssignmentState::ACTIVE) { w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,"no current active assignment");}
        else {
          w.bool_(ex::fid::ok, true);
          w.u64(ex::fid::epoch, fabric.current_epoch().value());
          w.u64(ex::fid::slot, slot.slot.value());
          w.u64(ex::fid::assignment_id, cur->id.value());
          w.u64(ex::fid::assignment_gen, cur->generation.value());
          w.u64(ex::fid::route_gen, cur->route_generation.value());
          w.u64(ex::fid::boot, cur->worker_boot.value());
          w.u64(ex::fid::incarnation, cur->engine_incarnation.value());
          w.u64(ex::fid::request_id, req.value());
          w.u64(ex::fid::execution_id, req.value());
          w.str(ex::fid::operation, "serve");
        }
        ex::send_msg(c, MsgType::AUTHORIZE_REQUEST, f->msg_id, f->epoch, w.finish());
      }
      else if (mt == MsgType::AUTHORIZE_RESULT) {
        PayloadReader r(f->payload);
        ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
        WorkerAuthorization auth;
        auth.epoch = CoordinatorEpoch(r.u64(ex::fid::epoch), CoordinatorId(1), CoordinatorId(1));
        auth.slot = slot;
        auth.assignment = AssignmentId(r.u64(ex::fid::assignment_id));
        auth.assignment_generation = AssignmentGeneration(r.u64(ex::fid::assignment_gen));
        auth.route_generation = RouteGeneration(r.u64(ex::fid::route_gen));
        auth.worker_boot = WorkerBootId(r.u64(ex::fid::boot));
        auth.incarnation = EngineIncarnationId(r.u64(ex::fid::incarnation));
        auth.request = RequestId(r.u64(ex::fid::request_id));
        auth.execution = ExecutionId(r.u64(ex::fid::execution_id));
        GateDecision d = fabric.authorize_result(auth);
        PayloadWriter w; w.bool_(ex::fid::ok, d.allowed); if (!d.allowed) w.str(ex::fid::detail, d.reason);
        ex::send_msg(c, MsgType::AUTHORIZE_RESULT, f->msg_id, f->epoch, w.finish());
      }
      else if (mt == MsgType::QUERY_ROUTE) {
        PayloadReader r(f->payload); ServiceId sid(r.u64(ex::fid::service)); ServiceSlotKey slot{sid, ServiceSlotId(1)};
        PayloadWriter w; w.bool_(ex::fid::ok, fabric.current_assignment(slot).has_value());
        if (auto cur = fabric.current_assignment(slot)) w.u64(ex::fid::target, cur->target.value());
        ex::send_msg(c, MsgType::QUERY_ROUTE, f->msg_id, f->epoch, w.finish());
      }
      else if (mt == MsgType::SAVE) {
        PayloadReader r(f->payload); std::string path = r.str(ex::fid::detail);
        try { fabric.save(path); PayloadWriter w; w.bool_(ex::fid::ok,true); ex::send_msg(c, MsgType::SAVE, f->msg_id, f->epoch, w.finish()); }
        catch (const std::exception& e) { PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail,e.what()); ex::send_msg(c, MsgType::ERROR_MSG, f->msg_id, f->epoch, w.finish()); }
      }
      } catch (const std::exception& e) {
        PayloadWriter w; w.bool_(ex::fid::ok,false); w.str(ex::fid::detail, e.what());
        ex::send_msg(c, MsgType::ERROR_MSG, f->msg_id, f->epoch, w.finish());
        std::fprintf(stderr, "coordinator handler: %s\n", e.what());
      }
    }
    net::close_socket(c);
  };

  while (!g_stop.load()) {
    sock c = net::tcp_accept(listen);
    if (c == kInvalidSocket) continue;
    std::thread(handle, c).detach();
  }
  net::close_socket(listen); net::cleanup();
  return 0;
}