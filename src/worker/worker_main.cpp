// worker_main.cpp — the reference CPU worker. Registers, warms up, and serves the
// deterministic reference workload; can be killed as a real OS process during failover.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }

int main(int argc, char** argv) {
  std::string coord_host = "127.0.0.1"; std::uint16_t coord_port = 0, port = 0;
  std::uint64_t target = 1, boot = 1;
  for (int i = 1; i < argc - 1; ++i) {
    std::string k = argv[i];
    if (k == "--coordinator") { auto p = std::string(argv[i+1]); auto c = p.find(':'); coord_host = p.substr(0, c); coord_port = (std::uint16_t)parse_u64(p.c_str()+c+1); }
    else if (k == "--port") port = (std::uint16_t)parse_u64(argv[i+1]);
    else if (k == "--target") target = parse_u64(argv[i+1]);
    else if (k == "--boot") boot = parse_u64(argv[i+1]);
  }
  sock listen = net::tcp_listen(port);
  if (listen == kInvalidSocket) { std::fprintf(stderr, "worker: cannot bind request port\n"); return 1; }
  std::uint16_t actual_port = net::tcp_listen_port(listen);
  std::printf("PORT %u\n", (unsigned)actual_port); std::fflush(stdout);

  sock ctrl = net::tcp_connect(coord_host, coord_port);
  if (ctrl == kInvalidSocket) { std::fprintf(stderr, "worker: cannot connect to coordinator\n"); return 1; }
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleWorker); h.u64(ex::fid::target, target); h.u64(ex::fid::boot, boot); h.u64(ex::fid::req_port, actual_port); ex::send_msg(ctrl, MsgType::HELLO, 1, 0, h.finish()); }

  std::atomic<bool> active{false};
  std::uint64_t my_boot = boot;

  // Publish readiness.
  { PayloadWriter w; w.u64(ex::fid::target, target); w.u64(ex::fid::boot, boot); w.bool_(ex::fid::ready, true);
    w.str(ex::fid::model, "ref-det-sequence-v1"); w.str(ex::fid::abi, "ff-ref-v1"); w.f64(ex::fid::capacity, 2.0);
    ex::send_msg(ctrl, MsgType::PUBLISH_READINESS, 2, 0, w.finish()); }

  // Control loop: ACTIVATE / FENCE.
  std::thread ctrl_thread([&] {
    while (true) {
      auto f = net::recv_frame(ctrl);
      if (!f) break;
      if (f->type == MsgType::ACTIVATE_TARGET) {
        active.store(true);
        PayloadWriter w; w.bool_(ex::fid::ok, true);
        ex::send_msg(ctrl, MsgType::ACTIVATE_RESULT, f->msg_id, f->epoch, w.finish());
      } else if (f->type == MsgType::FENCE_ASSIGNMENT) {
        // Respect fencing: this worker no longer serves new work on the governed path.
        active.store(false);
      }
    }
  });

  // Request loop: one request per connection.
  while (true) {
    sock c = net::tcp_accept(listen);
    if (c == kInvalidSocket) continue;
    std::thread([&, c] {
      auto f = net::recv_frame(c);
      std::uint64_t a = 0, b = 0, req_boot = 0;
      if (f) { PayloadReader r(f->payload); if (r.has(ex::fid::input_a)) a = r.u64(ex::fid::input_a); if (r.has(ex::fid::input_b)) b = r.u64(ex::fid::input_b); if (r.has(ex::fid::boot)) req_boot = r.u64(ex::fid::boot); }
      PayloadWriter w;
      // Admission: only an active worker whose own boot matches the requested authority may serve.
      if (active.load() && req_boot == my_boot) {
        std::uint64_t res = ex::reference_result(a, b);
        std::string rs = std::to_string(res);
        w.bool_(ex::fid::ok, true);
        std::vector<std::uint8_t> rb(rs.begin(), rs.end()); w.bytes(ex::fid::payload, rb);
      } else {
        w.bool_(ex::fid::ok, false); w.str(ex::fid::detail, "not active or stale boot");
      }
      ex::send_msg(c, MsgType::EXECUTION_RESULT, f ? f->msg_id : 0, f ? f->epoch : 0, w.finish());
      net::close_socket(c);
    }).detach();
  }
  return 0;
}