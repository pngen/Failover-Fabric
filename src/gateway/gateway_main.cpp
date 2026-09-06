// gateway_main.cpp — the reference request gateway. It consults the coordinator for
// per-dispatch and per-result authority, then dispatches to the current worker.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <failover_fabric/protocol.hpp>

#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

namespace {
std::map<std::uint64_t, RouteEntry> g_routes;   // slot -> route
std::uint64_t g_epoch = 0;
std::mutex g_route_mutex;
sock g_rpc = kInvalidSocket;
std::mutex g_rpc_mutex;

std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }

std::string worker_transport_for(std::uint64_t slot) {
  std::lock_guard<std::mutex> lk(g_route_mutex);
  auto it = g_routes.find(slot);
  if (it == g_routes.end()) return "";
  return it->second.transport;
}

// Connect to worker, send a compute request, read the result. Returns "" on failure.
std::string compute_on_worker(RouteEntry& route, std::uint64_t a, std::uint64_t b, bool& ok) {
  ok = false;
  std::string host = "127.0.0.1"; std::uint16_t port = 0;
  auto p = route.transport.find(":");
  if (p != std::string::npos && route.transport.rfind("tcp://", 0) == 0) {
    host = route.transport.substr(6, p - 6); port = (std::uint16_t)parse_u64(route.transport.c_str() + p + 1);
  }
  sock ws = net::tcp_connect(host, port);
  if (ws == kInvalidSocket) return "";
  PayloadWriter w; w.u64(ex::fid::input_a, a); w.u64(ex::fid::input_b, b);
  w.u64(ex::fid::boot, route.permitted_boot.value()); w.str(ex::fid::operation, "serve");
  bool sent = ex::send_msg(ws, MsgType::VERIFY_SERVICE, 1001, g_epoch, w.finish());
  std::string result;
  if (sent) {
    auto resp = net::recv_frame(ws);
    if (resp && resp->type == MsgType::EXECUTION_RESULT) { PayloadReader rr(resp->payload); ok = rr.has(ex::fid::ok) && rr.bool_(ex::fid::ok); if (ok && rr.has(ex::fid::payload)) { auto bv = rr.bytes(ex::fid::payload); result.assign((const char*)bv.data(), bv.size()); } }
  }
  net::close_socket(ws);
  return result;
}

void handle_client(sock c) {
  auto f = net::recv_frame(c);
  if (!f) { net::close_socket(c); return; }
  // Parse workload.
  std::uint64_t a = 0, b = 0; std::uint64_t slot = 1, service = 1;
  { PayloadReader r(f->payload); if (r.has(ex::fid::input_a)) a = r.u64(ex::fid::input_a); if (r.has(ex::fid::input_b)) b = r.u64(ex::fid::input_b);
    if (r.has(ex::fid::slot)) slot = r.u64(ex::fid::slot); if (r.has(ex::fid::service)) service = r.u64(ex::fid::service); }
  // Authorize dispatch with the coordinator.
  PayloadWriter ar; ar.u64(ex::fid::service, service); ar.u64(ex::fid::slot, slot); ar.u64(ex::fid::request_id, f->msg_id);
  PayloadWriter resp; resp.str(ex::fid::detail, "unknown");
  std::uint64_t epoch=0, asg=0, asggen=0, rgen=0, boot=0, inc=0;
  bool auth_ok = false;
  {
    std::lock_guard<std::mutex> lk(g_rpc_mutex);
    if (ex::send_msg(g_rpc, MsgType::AUTHORIZE_REQUEST, f->msg_id, g_epoch, ar.finish())) {
      auto rr = net::recv_frame(g_rpc);
      if (rr && rr->type == MsgType::AUTHORIZE_REQUEST) { PayloadReader q(rr->payload); auth_ok = q.has(ex::fid::ok) && q.bool_(ex::fid::ok);
        if (auth_ok) { epoch=q.u64(ex::fid::epoch); asg=q.u64(ex::fid::assignment_id); asggen=q.u64(ex::fid::assignment_gen); rgen=q.u64(ex::fid::route_gen); boot=q.u64(ex::fid::boot); inc=q.u64(ex::fid::incarnation); } }
    }
  }
  if (!auth_ok) { resp.bool_(ex::fid::ok, false); resp.str(ex::fid::detail, "dispatch not authorized"); ex::send_msg(c, MsgType::EXECUTION_RESULT, f->msg_id, g_epoch, resp.finish()); net::close_socket(c); return; }
  // Dispatch to the current worker.
  RouteEntry route; bool have_route = false;
  { std::lock_guard<std::mutex> lk(g_route_mutex); auto it = g_routes.find(slot); if (it != g_routes.end()) { route = it->second; have_route = true; } }
  if (!have_route) { resp.bool_(ex::fid::ok,false); resp.str(ex::fid::detail,"no route installed"); ex::send_msg(c, MsgType::EXECUTION_RESULT, f->msg_id, g_epoch, resp.finish()); net::close_socket(c); return; }
  bool wok = false;
  std::string result = compute_on_worker(route, a, b, wok);
  // Authorize result with the coordinator.
  PayloadWriter ar2; ar2.u64(ex::fid::service, service); ar2.u64(ex::fid::slot, slot);
  ar2.u64(ex::fid::epoch, epoch); ar2.u64(ex::fid::assignment_id, asg); ar2.u64(ex::fid::assignment_gen, asggen);
  ar2.u64(ex::fid::route_gen, rgen); ar2.u64(ex::fid::boot, boot); ar2.u64(ex::fid::incarnation, inc);
  ar2.u64(ex::fid::request_id, f->msg_id); ar2.u64(ex::fid::execution_id, f->msg_id);
  bool res_ok = false;
  { std::lock_guard<std::mutex> lk(g_rpc_mutex);
    if (ex::send_msg(g_rpc, MsgType::AUTHORIZE_RESULT, f->msg_id, g_epoch, ar2.finish())) {
      auto rr = net::recv_frame(g_rpc);
      if (rr && rr->type == MsgType::AUTHORIZE_RESULT) { PayloadReader q(rr->payload); res_ok = q.has(ex::fid::ok) && q.bool_(ex::fid::ok); } } }
  bool parity = wok && result == std::to_string(ex::reference_result(a, b));
  resp.bool_(ex::fid::ok, res_ok && parity);
  resp.bool_(ex::fid::state, parity);   // parity_ok
  std::vector<std::uint8_t> rb(result.begin(), result.end());
  if (!rb.empty()) resp.bytes(ex::fid::payload, rb);
  ex::send_msg(c, MsgType::EXECUTION_RESULT, f->msg_id, g_epoch, resp.finish());
  net::close_socket(c);
}

}  // namespace

int main(int argc, char** argv) {
  std::string coord_host = "127.0.0.1"; std::uint16_t coord_port = 0; std::uint16_t port = 0; std::uint64_t boot = 1;
  for (int i = 1; i < argc - 1; ++i) {
    std::string k = argv[i];
    if (k == "--coordinator") { auto p = std::string(argv[i+1]); auto c = p.find(':'); coord_host = p.substr(0, c); coord_port = (std::uint16_t)parse_u64(p.c_str()+c+1); }
    else if (k == "--port") port = (std::uint16_t)parse_u64(argv[i+1]);
    else if (k == "--boot") boot = parse_u64(argv[i+1]);
  }
  sock listen = net::tcp_listen(port);
  if (listen == kInvalidSocket) { std::fprintf(stderr, "gateway: cannot bind\n"); return 1; }
  std::printf("PORT %u\n", (unsigned)net::tcp_listen_port(listen)); std::fflush(stdout);

  // Register control socket.
  sock ctrl = net::tcp_connect(coord_host, coord_port);
  if (ctrl == kInvalidSocket) { std::fprintf(stderr, "gateway: cannot connect to coordinator\n"); return 1; }
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleGateway); h.u64(ex::fid::boot, boot); ex::send_msg(ctrl, MsgType::HELLO, 1, 0, h.finish()); }
  // RPC socket for authority.
  g_rpc = net::tcp_connect(coord_host, coord_port);
  if (g_rpc == kInvalidSocket) { std::fprintf(stderr, "gateway: cannot open rpc socket\n"); return 1; }
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, boot); ex::send_msg(g_rpc, MsgType::HELLO, 2, 0, h.finish()); }

  // Control-receive thread for INSTALL_ROUTE / VERIFY_SERVICE pushes.
  std::thread ctrl_thread([&] {
    while (true) {
      auto f = net::recv_frame(ctrl);
      if (!f) break;
      g_epoch = f->epoch;
      if (f->type == MsgType::INSTALL_ROUTE) {
        PayloadReader r(f->payload);
        RouteEntry re = ex::decode_route_entry(r);
        { std::lock_guard<std::mutex> lk(g_route_mutex); g_routes[re.slot.slot.value()] = re; }
        PayloadWriter w; w.bool_(ex::fid::ok, true);
        ex::send_msg(ctrl, MsgType::ROUTE_ACK, f->msg_id, f->epoch, w.finish());
      } else if (f->type == MsgType::VERIFY_SERVICE) {
        PayloadReader r(f->payload);
        std::uint64_t a = r.u64(ex::fid::input_a), b = r.u64(ex::fid::input_b);
        RouteEntry route; bool have = false; std::uint64_t slot = 1;
        { std::lock_guard<std::mutex> lk(g_route_mutex); auto it = g_routes.begin(); if (it != g_routes.end()) { route = it->second; have = true; slot = it->first; } }
        bool wok = false; std::string result;
        if (have) result = compute_on_worker(route, a, b, wok);
        bool parity = wok && result == std::to_string(ex::reference_result(a, b));
        PayloadWriter w; w.bool_(ex::fid::ok, parity);
        std::vector<std::uint8_t> rb(result.begin(), result.end()); if (!rb.empty()) w.bytes(ex::fid::payload, rb);
        ex::send_msg(ctrl, MsgType::VERIFY_SERVICE, f->msg_id, f->epoch, w.finish());
      }
    }
  });

  // Accept client connections on the request port.
  while (true) {
    sock c = net::tcp_accept(listen);
    if (c == kInvalidSocket) continue;
    std::thread(handle_client, c).detach();
  }
  net::close_socket(listen);
  return 0;
}