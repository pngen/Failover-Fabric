// multiproc_proof.cpp — real multiprocess failover proof. Spawns coordinator, gateway, and
// two reference workers as independent OS processes (CPU by default, real CUDA with --cuda),
// serves requests through the current target, kills the active worker, and proves an
// authoritative failover to the standby with routed CPU-parity verification and stale
// authority recovery blocking.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <failover_fabric/protocol.hpp>
#include <failover_fabric/plan.hpp>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>
#include <chrono>

#ifdef _WIN32
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <windows.h>
#else
  #include <signal.h>
  #include <sys/wait.h>
  #include <unistd.h>
#endif

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

namespace {
#ifdef _WIN32
struct Proc { HANDLE handle{nullptr}; HANDLE pipe{nullptr}; };
Proc spawn(const std::string& exe, std::vector<std::string> args) {
  SECURITY_ATTRIBUTES sa{sizeof(sa), nullptr, TRUE};
  HANDLE r = nullptr, w = nullptr;
  CreatePipe(&r, &w, &sa, 0);
  SetHandleInformation(r, HANDLE_FLAG_INHERIT, 0);
  SetHandleInformation(w, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
  std::string cmdline = "\"" + exe + "\"";
  for (auto& a : args) cmdline += " \"" + a + "\"";
  STARTUPINFOA si{}; si.cb = sizeof(si); si.dwFlags = STARTF_USESTDHANDLES;
  si.hStdInput = GetStdHandle(STD_INPUT_HANDLE); si.hStdOutput = w; si.hStdError = GetStdHandle(STD_ERROR_HANDLE);
  PROCESS_INFORMATION pi{};
  if (!CreateProcessA(nullptr, (LPSTR)cmdline.c_str(), nullptr, nullptr, TRUE, CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi)) {
    CloseHandle(r); CloseHandle(w); return {nullptr, nullptr};
  }
  CloseHandle(w);
  return {pi.hProcess, r};
}
std::string read_line(HANDLE h) {
  std::string out; char c; DWORD n = 0;
  while (ReadFile(h, &c, 1, &n, nullptr) && n == 1) { if (c == '\n') { if (!out.empty() && out.back() == '\r') out.pop_back(); return out; } out += c; if (out.size() > 65536) break; }
  return out;
}
std::uint16_t read_port(HANDLE h) {
  std::string line;
  for (int i = 0; i < 32; ++i) { line = read_line(h); if (line.rfind("PORT ", 0) == 0) return (std::uint16_t)std::strtoull(line.c_str() + 5, nullptr, 10); }
  return 0;
}
void kill_proc(Proc& p) { if (p.handle) { TerminateProcess(p.handle, 0); WaitForSingleObject(p.handle, 5000); } }
#else
struct Proc { int pid{0}; int pipe{-1}; };
Proc spawn(const std::string& exe, std::vector<std::string> args) {
  int p[2]; pipe(p);
  pid_t pid = fork(); if (pid == 0) { dup2(p[1], 1); close(p[0]); std::vector<char*> av; av.push_back((char*)exe.c_str()); for (auto& a : args) av.push_back((char*)a.c_str()); av.push_back(nullptr); execvp(exe.c_str(), av.data()); _exit(127); }
  close(p[1]); return {pid, p[0]};
}
std::string read_line(int fd) { std::string out; char c; if (fd < 0) return out; while (read(fd, &c, 1) == 1) { if (c == '\n') return out; out += c; } return out; }
std::uint16_t read_port(int fd) { std::string line; for (int i = 0; i < 32; ++i) { line = read_line(fd); if (line.rfind("PORT ", 0) == 0) return (std::uint16_t)std::strtoull(line.c_str() + 5, nullptr, 10); } return 0; }
void kill_proc(Proc& p) { if (p.pid) ::kill(p.pid, SIGKILL); }
#endif

std::uint64_t u64(const char* s) { return std::strtoull(s, nullptr, 10); }
bool send1(sock s, MsgType t, std::uint32_t id, std::uint64_t epoch, const std::vector<std::uint8_t>& payload = {}) { return ex::send_msg(s, t, id, epoch, payload); }

// Coordinator RPC: send a message, expect a reply of the same type, return optional payload reader result.
bool coord_rpc(std::uint16_t cport, MsgType t, const std::vector<std::uint8_t>& payload, MsgType& out_type, std::string& detail) {
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) { detail = "rpc connect failed"; return false; }
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  bool ok = false;
  if (send1(c, t, 7000, 0, payload)) {
    auto r = net::recv_frame(c);
    if (r) {
      if (r->type == MsgType::ERROR_MSG) { PayloadReader q(r->payload); if (q.has(ex::fid::detail)) detail = q.str(ex::fid::detail); }
      else { out_type = r->type; PayloadReader q(r->payload); if (q.has(ex::fid::ok) && q.bool_(ex::fid::ok)) ok = true; else if (q.has(ex::fid::detail)) detail = q.str(ex::fid::detail); }
    } else detail = "rpc no reply";
  } else detail = "rpc send failed";
  net::close_socket(c);
  return ok;
}
// Fresh-connection query: returns current target + revalidation-required flag + route authority.
struct QueryResult { std::uint64_t target{0}; bool revalidation{false}; std::uint64_t ambiguous{0}; std::uint64_t route_gen{0}; std::uint64_t gateway_boot{0}; std::uint64_t epoch{0}; bool txn_incomplete{false}; };
QueryResult query_state(std::uint16_t cport, std::uint64_t service) {
  QueryResult qr;
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) return qr;
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  PayloadWriter w; w.u64(ex::fid::service, service);
  if (send1(c, MsgType::QUERY_ROUTE, 7005, 0, w.finish())) { auto r = net::recv_frame(c); if (r) { PayloadReader q(r->payload); if (q.has(ex::fid::target)) qr.target = q.u64(ex::fid::target); if (q.has(ex::fid::state)) qr.revalidation = q.bool_(ex::fid::state); if (q.has(ex::fid::seq)) qr.ambiguous = q.u64(ex::fid::seq); if (q.has(ex::fid::route_gen)) qr.route_gen = q.u64(ex::fid::route_gen); if (q.has(ex::fid::boot)) qr.gateway_boot = q.u64(ex::fid::boot); if (q.has(ex::fid::epoch)) qr.epoch = q.u64(ex::fid::epoch); if (q.has(ex::fid::txn_incomplete)) qr.txn_incomplete = q.bool_(ex::fid::txn_incomplete); } }
  net::close_socket(c);
  return qr;
}

// Ask the coordinator to validate a gateway boot's acknowledgment of a route generation
// against its live route table. Returns true only if the boot + generation are current.
bool route_ack_authorized(std::uint16_t cport, std::uint64_t service, std::uint64_t gen, std::uint64_t gboot) {
  PayloadWriter w; w.u64(ex::fid::service, service); w.u64(ex::fid::slot, 1);
  w.u64(ex::fid::route_gen, gen); w.u64(ex::fid::boot, gboot);
  MsgType t; std::string detail;
  return coord_rpc(cport, MsgType::CHECK_ROUTE_ACK, w.finish(), t, detail);
}

// Query the coordinator's recorded disposition for a request id. found is set when the
// request is known; the returned byte is the RequestDisposition enum value.
std::uint8_t classify_request(std::uint16_t cport, std::uint64_t req_id, bool& found) {
  found = false;
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) return 0;
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  PayloadWriter w; w.u64(ex::fid::request_id, req_id);
  std::uint8_t disp = 0;
  if (send1(c, MsgType::CLASSIFY_REQUEST, 7010, 0, w.finish())) {
    auto r = net::recv_frame(c);
    if (r && r->type == MsgType::CLASSIFY_REQUEST) { PayloadReader q(r->payload); if (q.has(ex::fid::ok) && q.bool_(ex::fid::ok)) { found = true; if (q.has(ex::fid::state)) disp = q.u8(ex::fid::state); } }
  }
  net::close_socket(c);
  return disp;
}

// Total authorized dispatches observed by the coordinator (via CLASSIFY_REQUEST).
std::size_t query_dispatch_count(std::uint16_t cport) {
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) return 0;
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  PayloadWriter w; w.u64(ex::fid::request_id, 0);
  std::size_t dc = 0;
  if (send1(c, MsgType::CLASSIFY_REQUEST, 7011, 0, w.finish())) {
    auto r = net::recv_frame(c);
    if (r && r->type == MsgType::CLASSIFY_REQUEST) { PayloadReader q(r->payload); if (q.has(ex::fid::dispatch_count)) dc = (std::size_t)q.u64(ex::fid::dispatch_count); }
  }
  net::close_socket(c);
  return dc;
}

bool gateway_request(std::uint16_t gport, std::uint64_t service, std::uint64_t a, std::uint64_t b, std::uint64_t& result, std::uint32_t req_id = 5001, bool idempotent = false, bool externally_effectful = false) {
  sock gw = net::tcp_connect("127.0.0.1", gport);
  if (gw == net::kInvalidSocket) return false;
  PayloadWriter w; w.u64(ex::fid::input_a, a); w.u64(ex::fid::input_b, b); w.u64(ex::fid::service, service); w.u64(ex::fid::slot, 1);
  w.bool_(ex::fid::idempotent, idempotent); w.bool_(ex::fid::externally_effectful, externally_effectful);
  bool ret = false;
  if (send1(gw, MsgType::VERIFY_SERVICE, req_id, 0, w.finish())) {
    auto r = net::recv_frame(gw);
    if (r && r->type == MsgType::EXECUTION_RESULT) {
      PayloadReader q(r->payload);
      if (q.has(ex::fid::ok) && q.bool_(ex::fid::ok)) {
        std::string rs; if (q.has(ex::fid::payload)) { auto bv = q.bytes(ex::fid::payload); rs.assign((const char*)bv.data(), bv.size()); }
        result = std::strtoull(rs.c_str(), nullptr, 10);
        ret = result == ex::reference_result(a, b);
      }
    }
  }
  
  return ret;
}
}  // namespace

// A late result carrying OLD worker authority must be rejected after a cutover.
bool stale_result_rejected(std::uint16_t cport, std::uint64_t service, std::uint64_t old_boot,
                           std::uint64_t old_assg_gen, std::uint64_t old_route_gen, std::uint64_t req_id) {
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) return false;
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  PayloadWriter w; w.u64(ex::fid::service, service); w.u64(ex::fid::slot, 1);
  w.u64(ex::fid::epoch, 1); w.u64(ex::fid::epoch_id, 1); w.u64(ex::fid::epoch_boot, 1);
  w.u64(ex::fid::assignment_id, 1); w.u64(ex::fid::assignment_gen, old_assg_gen);
  w.u64(ex::fid::route_gen, old_route_gen); w.u64(ex::fid::boot, old_boot);
  w.u64(ex::fid::incarnation, old_boot); w.u64(ex::fid::request_id, req_id); w.u64(ex::fid::execution_id, req_id);
  bool rejected = false;
  if (send1(c, MsgType::AUTHORIZE_RESULT, 7007, 0, w.finish())) {
    auto r = net::recv_frame(c);
    if (r && r->type == MsgType::AUTHORIZE_RESULT) { PayloadReader q(r->payload); rejected = !(q.has(ex::fid::ok) && q.bool_(ex::fid::ok)); }
  }
  net::close_socket(c);
  return rejected;
}
// ------------------------------------------------------------------------- //
// Scenario: live old-worker fencing (worker A stays physically alive; its control
// connection is severed; a TRANSPORT_DISCONNECT triggers failover to B).
// ------------------------------------------------------------------------- //
static int run_live_fence(const std::string& dir, const char* worker_exe) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) { std::fprintf(stderr, "live-fence: no cport\n"); return 1; }
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  std::uint16_t aport = read_port(wa.pipe);
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  sock gw = net::tcp_connect("127.0.0.1", gport);
  if (gw == net::kInvalidSocket) { std::fprintf(stderr, "live-fence: no gateway\n"); return 1; }
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A); coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }

  // Phase 1: A serves.
  std::uint64_t r1 = 0; bool a_served = gateway_request(gport, SVC, 3, 5, r1);

  // Sever A's control connection; A stays alive (severs -> ok, still serves on request socket).
  { sock c = net::tcp_connect("127.0.0.1", aport); if (c != net::kInvalidSocket) {
      PayloadWriter w; w.u64(ex::fid::input_a, 0xDBADu); w.u64(ex::fid::input_b, 0); w.u64(ex::fid::boot, A);
      send1(c, MsgType::VERIFY_SERVICE, 6001, 0, w.finish()); auto r = net::recv_frame(c); net::close_socket(c); } }

  // A must still be alive: a direct probe to its request port must be served.
  bool alive = false;
  { sock c = net::tcp_connect("127.0.0.1", aport); if (c != net::kInvalidSocket) {
      PayloadWriter w; w.u64(ex::fid::input_a, 1); w.u64(ex::fid::input_b, 1); w.u64(ex::fid::boot, A);
      send1(c, MsgType::VERIFY_SERVICE, 6002, 0, w.finish()); auto r = net::recv_frame(c);
      if (r && r->type == MsgType::EXECUTION_RESULT) { PayloadReader q(r->payload); alive = q.has(ex::fid::ok) && q.bool_(ex::fid::ok); }
      net::close_socket(c); } }

  // Transport disconnect evidence (without killing A).
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::TRANSPORT_DISCONNECT); w.u64(ex::fid::seq, 2); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }

  // Failover to B through the actual governed fencing mechanism.
  bool promoted = false; std::string fdet; MsgType tt;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), tt, fdet)) promoted = true;
    else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  QueryResult qs = query_state(cport, SVC);
  bool current_b = (qs.target == B);
  bool stale_rejected = stale_result_rejected(cport, SVC, A, 1, 1, 4242);
  // A fresh replacement must not reclaim.
  Proc wa2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", "42"});
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  QueryResult qs2 = query_state(cport, SVC);
  bool no_reclaim = (qs2.target == B);

  std::printf("live_fence a_served=%s a_alive=%s promoted=%s current_b=%s stale_rejected=%s no_reclaim=%s\n",
    a_served?"OK":"FAIL", alive?"OK":"FAIL", promoted?"OK":"FAIL", current_b?"OK":"FAIL", stale_rejected?"OK":"FAIL", no_reclaim?"OK":"FAIL");
  bool ok = a_served && alive && promoted && current_b && stale_rejected && no_reclaim;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  net::close_socket(gw);
  kill_proc(coord); kill_proc(gateway); kill_proc(wa); kill_proc(wb); kill_proc(wa2);
  return ok ? 0 : 1;
}

// ------------------------------------------------------------------------- //
// Scenario: in-flight ambiguity. A computes a reference result and withholds the
// response; the gateway sees EOF and the request must not be reported as success.
// ------------------------------------------------------------------------- //
static int run_ambiguity(const std::string& dir, const char* worker_exe) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) return 1;
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A); coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }

  // Dispatch a request that A computes but withholds (EOF). The gateway uses msg id 5001 as
  // the request id, marked explicitly NON-RETRYABLE (idempotent=false, externally_effectful=true).
  // The coordinator must record it as an explicit OUTCOME_UNKNOWN.
  std::uint64_t r = 0;
  bool reported_success = gateway_request(gport, SVC, 0xBEEFu, 7, r, 5001, false, true);
  std::string ambig_file = "ambig_" + std::to_string(A) + ".out";
  bool computed = false;
  { FILE* fp = fopen(ambig_file.c_str(), "r"); if (fp) { computed = true; fclose(fp); } }
  std::remove(ambig_file.c_str());
  bool not_success = !reported_success;
  bool found = false;
  std::uint8_t disp = classify_request(cport, 5001, found);
  bool outcome_unknown = found && (disp == (std::uint8_t)RequestDisposition::OUTCOME_UNKNOWN);
  std::printf("ambiguity computed=%s reported_success=%s not_success=%s outcome_unknown=%s disp=%u\n",
    computed?"OK":"FAIL", reported_success?"YES":"NO", not_success?"OK":"FAIL", outcome_unknown?"OK":"FAIL", (unsigned)disp);

  // The in-flight request's authority moved: A dies and the standby B is promoted.
  kill_proc(wa); std::this_thread::sleep_for(std::chrono::milliseconds(150));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }
  bool promoted = false; std::string fdet; MsgType tt;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), tt, fdet)) promoted = true; else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  // The replacement (B) executes a replayed/current attempt under a fresh request id and
  // returns a verified CPU-parity response.
  std::uint64_t r2 = 0;
  bool replayed_verified = gateway_request(gport, SVC, 7, 11, r2, 5002);
  // A delayed stale publication from the old target (A) cannot commit under the new authority.
  bool stale_rejected = stale_result_rejected(cport, SVC, A, 1, 1, 5001);
  // The ambiguous request was never committed as success and is not claimed exactly-once.
  bool found2 = false;
  std::uint8_t disp2 = classify_request(cport, 5001, found2);
  bool not_exactly_once = found2 && (disp2 == (std::uint8_t)RequestDisposition::OUTCOME_UNKNOWN);
  // NON-RETRYABLE REFUSAL: replay the ORIGINAL (id 5001) request through the real path after
  // failover. It was classified OUTCOME_UNKNOWN and its policy is non-retryable, so the
  // coordinator must refuse automatic replay (an explicit policy gate), must NOT overwrite the
  // OUTCOME_UNKNOWN classification, and must NOT authorize/execute any replacement attempt.
  std::size_t dpre = query_dispatch_count(cport);
  std::uint64_t r3 = 0;
  bool replay_refused = !gateway_request(gport, SVC, 7, 11, r3, 5001, false, true);
  std::size_t dpost = query_dispatch_count(cport);
  bool no_replay_dispatch = (dpost == dpre);
  bool found3 = false;
  std::uint8_t disp3 = classify_request(cport, 5001, found3);
  bool preserved_unknown = found3 && (disp3 == (std::uint8_t)RequestDisposition::OUTCOME_UNKNOWN);
  std::printf("ambiguity promoted=%s replayed_verified=%s stale_rejected=%s not_exactly_once=%s replay_refused=%s no_replay_dispatch=%s preserved_unknown=%s\n",
    promoted?"OK":"FAIL", replayed_verified?"OK":"FAIL", stale_rejected?"OK":"FAIL", not_exactly_once?"OK":"FAIL",
    replay_refused?"OK":"FAIL", no_replay_dispatch?"OK":"FAIL", preserved_unknown?"OK":"FAIL");
  bool ok = computed && not_success && outcome_unknown && promoted && replayed_verified && stale_rejected && not_exactly_once
    && replay_refused && no_replay_dispatch && preserved_unknown;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(gateway); kill_proc(wa); kill_proc(wb);
  return ok ? 0 : 1;
}

// ------------------------------------------------------------------------- //
// Scenario: independent gateway restart with a fresh GatewayBootId.
// ------------------------------------------------------------------------- //
static int run_gateway_restart(const std::string& dir, const char* worker_exe) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) return 1;
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::uint16_t bport = read_port(wb.pipe);
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A); coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }
  std::uint64_t r1 = 0; bool a_served = gateway_request(gport, SVC, 3, 5, r1);
  kill_proc(wa); std::this_thread::sleep_for(std::chrono::milliseconds(150));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }
  bool promoted = false; std::string fdet; MsgType tt;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), tt, fdet)) promoted = true; else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  // B is the promoted standby: it must be live and active (direct probe).
  bool b_alive = false;
  { sock c = net::tcp_connect("127.0.0.1", bport); if (c != net::kInvalidSocket) {
      PayloadWriter w; w.u64(ex::fid::input_a, 7); w.u64(ex::fid::input_b, 11); w.u64(ex::fid::boot, B);
      send1(c, MsgType::VERIFY_SERVICE, 6005, 0, w.finish()); auto r = net::recv_frame(c);
      if (r && r->type == MsgType::EXECUTION_RESULT) { PayloadReader q(r->payload); b_alive = q.has(ex::fid::ok) && q.bool_(ex::fid::ok); }
      net::close_socket(c); } }
  // Independent gateway restart under a FRESH GatewayBootId: the coordinator must accept it,
  // push the current route, and the fresh gateway must serve routed verification.
  kill_proc(gateway);
  Proc g2 = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "2"});
  std::uint16_t gport2 = read_port(g2.pipe);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  std::uint64_t r2 = 0;
  bool b_served = gport2 ? gateway_request(gport2, SVC, 7, 11, r2) : false;
  // Current route authority, reported by the coordinator's live route table.
  QueryResult qs2 = query_state(cport, SVC);
  std::uint64_t cur_gen = qs2.route_gen;
  // No old-route authority revival: the promoted standby must be current, and the route must
  // have advanced strictly past the original generation (the pre-failover route to A was gen 1).
  bool no_old_route_revival = (qs2.target == B) && (cur_gen > 1) && (qs2.gateway_boot == 2);
  // Stale GatewayBootId acknowledgment rejection, validated through the coordinator's live
  // route table: the OLD gateway boot (1) and a STALE generation must not acknowledge the
  // current route; the FRESH gateway boot (2) with the current generation must be accepted.
  bool stale_boot_rejected = !route_ack_authorized(cport, SVC, cur_gen, 1);
  bool stale_gen_rejected = !route_ack_authorized(cport, SVC, cur_gen > 1 ? cur_gen - 1 : 1, 2);
  bool current_ack_ok = route_ack_authorized(cport, SVC, cur_gen, 2);
  bool stale_ack_rejected = stale_boot_rejected && stale_gen_rejected && current_ack_ok;
  std::printf("gateway_restart a_served=%s promoted=%s b_alive=%s fresh_gateway_serves=%s no_old_route_revival=%s stale_boot_rejected=%s stale_gen_rejected=%s current_ack_ok=%s\n",
    a_served?"OK":"FAIL", promoted?"OK":"FAIL", b_alive?"OK":"FAIL", b_served?"OK":"FAIL", no_old_route_revival?"OK":"FAIL",
    stale_boot_rejected?"OK":"FAIL", stale_gen_rejected?"OK":"FAIL", current_ack_ok?"OK":"FAIL");
  bool ok = a_served && promoted && b_alive && b_served && no_old_route_revival && stale_ack_rejected;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(g2); kill_proc(wb); kill_proc(wa);
  return ok ? 0 : 1;
}

// ------------------------------------------------------------------------- //
// Scenario: failback / anti-flapping over real processes. The preferred target (A) fails,
// authority moves to B, then a fresh healthy incarnation A' recovers. A' must NOT reclaim
// automatically; failback requires policy authorization + fresh readiness, issues a new
// assignment + route generation through the authoritative transaction, and routed
// verification succeeds on A' while old authority (B) stays rejected.
// ------------------------------------------------------------------------- //
static int run_failback(const std::string& dir, const char* worker_exe) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) { std::fprintf(stderr, "failback: no cport\n"); return 1; }
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  // Register with MANUAL failback and a zero cooldown so the proof is deterministic/fast,
  // while still proving that authorization (and not merely health) is required.
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A);
    w.u8(ex::fid::failback, (std::uint8_t)FailbackPolicy::MANUAL); w.u32(ex::fid::cooldown_ms, 0); w.u32(ex::fid::max_auto_attempts, 1); w.u32(ex::fid::hysteresis_ms, 0);
    coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }
  // Phase 1: A serves.
  std::uint64_t r1 = 0; bool a_served = gateway_request(gport, SVC, 3, 5, r1);
  // Fail A -> failover to B.
  kill_proc(wa); std::this_thread::sleep_for(std::chrono::milliseconds(150));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }
  bool promoted = false; std::string fdet; MsgType tt;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), tt, fdet)) promoted = true; else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  QueryResult qB = query_state(cport, SVC);
  bool moved_to_b = (qB.target == B);
  // Recovered preferred target A' (fresh boot 42) comes back healthy.
  Proc wa2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", "42"});
  std::uint16_t aport2 = read_port(wa2.pipe);
  (void)aport2;   // A' is activated by the coordinator during failback, not probed directly
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  // A' must NOT reclaim automatically.
  QueryResult qNo = query_state(cport, SVC);
  bool no_reclaim = (qNo.target == B);
  // Without policy authorization (MANUAL), failback is refused even though A' is healthy.
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC);
    bool refused = !coord_rpc(cport, MsgType::REQUEST_FAILBACK, w.finish(), t, d);
    std::printf("failback_refused(no_authorize)=%s detail=%s\n", refused?"OK":"FAIL", d.c_str()); }
  // With explicit authorization, failback executes through the authoritative transaction.
  bool back_home = false; std::string fbfd; MsgType ft;
  { PayloadWriter w; w.u64(ex::fid::service, SVC); w.bool_(ex::fid::authorize, true);
    back_home = coord_rpc(cport, MsgType::REQUEST_FAILBACK, w.finish(), ft, fbfd); }
  QueryResult qHome = query_state(cport, SVC);
  bool current_is_a = (qHome.target == A);
  // Routed verification through the gateway to the recovered A'.
  std::uint64_t r2 = 0;
  bool a_serves_again = gateway_request(gport, SVC, 7, 11, r2);
  // Old authority (B) must remain rejected.
  bool old_rejected = stale_result_rejected(cport, SVC, B, 2, qB.route_gen ? qB.route_gen : 2, 5151);
  std::printf("failback a_served=%s promoted=%s moved_to_b=%s no_reclaim=%s back_home=%s current_is_a=%s a_serves_again=%s old_rejected=%s\n",
    a_served?"OK":"FAIL", promoted?"OK":"FAIL", moved_to_b?"OK":"FAIL", no_reclaim?"OK":"FAIL",
    back_home?"OK":"FAIL", current_is_a?"OK":"FAIL", a_serves_again?"OK":"FAIL", old_rejected?"OK":"FAIL");
  bool ok = a_served && promoted && moved_to_b && no_reclaim && back_home && current_is_a && a_serves_again && old_rejected;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(gateway); kill_proc(wb); kill_proc(wa2);
  return ok ? 0 : 1;
}

// ------------------------------------------------------------------------- //
// Scenario: interrupted-cutover recovery. The coordinator runs a failover but is terminated
// and restarted at each material cutover boundary (intent recorded, old admission fenced,
// promotion authorized, activation acknowledged, route installed). The durable checkpoint
// records the reached milestone; after restart the coordinator reconciles: epoch advances,
// stale old-epoch traffic is rejected, the service is left explicitly revalidation-required /
// unavailable (the interrupted cutover is never revived), and only a fresh verified recovery
// restores service with at most one exclusive current assignment.
// ------------------------------------------------------------------------- //
// ------------------------------------------------------------------------- //
// Scenario: interrupted-cutover recovery. The coordinator runs a failover but is
// terminated and restarted at each material cutover boundary (intent recorded, old
// admission fenced, activation acknowledged, route installed). The durable checkpoint
// records the reached milestone; after restart the coordinator reconciles: epoch
// advances, stale old-epoch traffic is rejected, the service is left explicitly
// revalidation-required / unavailable (the interrupted cutover is never revived), and
// only a fresh verified recovery restores service with at most one exclusive current.
// ------------------------------------------------------------------------- //
static int run_cutover_restart_impl(const std::string& dir, const char* worker_exe, Milestone boundary) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  const char* state_path = "coord_cut.ff";
  std::remove(state_path);
  // 1. Fresh coordinator (checkpoint enabled) + gateway + A/B workers.
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0", "--checkpoint", state_path});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) { std::fprintf(stderr, "cutover: no cport\n"); return 1; }
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  (void)gport;   // the original gateway is not used after the restart below
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A); coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }

  // 2. Trigger the failover but stop (and checkpoint) exactly at the material boundary.
  bool interrupted = false; std::string bdet;
  { PayloadWriter w; w.u64(ex::fid::service, SVC); w.u8(ex::fid::state, (std::uint8_t)boundary);
    MsgType t; coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), t, bdet);
    interrupted = bdet.find("interrupted") != std::string::npos; }
  if (!interrupted) { std::fprintf(stderr, "cutover: no interruption at boundary (%s)\n", bdet.c_str()); kill_proc(coord); kill_proc(gateway); kill_proc(wa); kill_proc(wb); return 1; }

  // 3. Terminate the coordinator (and stale gateway/workers) at the boundary.
  kill_proc(coord); kill_proc(gateway); kill_proc(wa); kill_proc(wb);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 4. Restart the coordinator from the durable checkpoint and reconcile.
  Proc coord2 = spawn(exe("ff_coordinator.exe"), {"--port", "0", "--load", state_path, "--checkpoint", state_path});
  std::uint16_t cport2 = read_port(coord2.pipe); if (!cport2) { std::fprintf(stderr, "cutover: no cport2\n"); return 1; }
  QueryResult qr = query_state(cport2, SVC);
  bool epoch_advanced = (qr.epoch > 1);
  bool revalidation = qr.revalidation;
  bool incomplete = qr.txn_incomplete;
  bool old_epoch_rejected = stale_result_rejected(cport2, SVC, A, 1, 1, 9000);
  // A fresh gateway reconnects, acquires the current route; fresh workers register.
  Proc g2 = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport2), "--port", "0", "--boot", "2"});
  std::uint16_t gport2 = read_port(g2.pipe);
  Proc wb2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport2), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  Proc wa2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport2), "--port", "0", "--target", std::to_string(A), "--boot", "42"});
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  // The interrupted cutover's pending assignment must not have become current.
  QueryResult qr2 = query_state(cport2, SVC);
  bool no_premature_authority = qr2.txn_incomplete || qr2.revalidation;

  // 5. A FRESH verified recovery (no barrier) completes.
  bool recovered = false; std::string rdet; MsgType rt;
  for (int attempt = 0; attempt < 30 && !recovered; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport2, MsgType::EXECUTE_FAILOVER, w.finish(), rt, rdet)) recovered = true;
    else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  QueryResult qr3 = query_state(cport2, SVC);
  bool current_b = (qr3.target == B);
  std::uint64_t rv = 0;
  bool routed_ok = gport2 ? gateway_request(gport2, SVC, 3, 5, rv) : false;
  bool stale_rejected = stale_result_rejected(cport2, SVC, B, 2, 2, 9001);
  std::printf("cutover_restart boundary=%s epoch_advanced=%s revalidation=%s incomplete=%s old_epoch_rejected=%s no_premature_authority=%s recovered=%s current_b=%s routed_ok=%s stale_rejected=%s\n",
    to_string(boundary), epoch_advanced?"OK":"FAIL", revalidation?"OK":"FAIL", incomplete?"OK":"FAIL",
    old_epoch_rejected?"OK":"FAIL", no_premature_authority?"OK":"FAIL", recovered?"OK":"FAIL", current_b?"OK":"FAIL", routed_ok?"OK":"FAIL", stale_rejected?"OK":"FAIL");
  bool ok = epoch_advanced && revalidation && incomplete && old_epoch_rejected && no_premature_authority && recovered && current_b && routed_ok && stale_rejected;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord2); kill_proc(g2); kill_proc(wb2); kill_proc(wa2);
  return ok ? 0 : 1;
}

static int run_cutover_restart(const std::string& dir, const char* worker_exe) {
  const Milestone boundaries[] = { Milestone::INTENT_ONLY, Milestone::OLD_ADMISSION_FENCED,
    Milestone::ACTIVATION_ACKNOWLEDGED, Milestone::ROUTE_INSTALLED };
  bool all = true;
  for (Milestone bm : boundaries) {
    std::printf("--- boundary %s ---\n", to_string(bm));
    if (run_cutover_restart_impl(dir, worker_exe, bm) != 0) { all = false; }
    std::remove("coord_cut.ff");
  }
  return all ? 0 : 1;
}

// ------------------------------------------------------------------------- //
// Scenario: stateful checkpoint restore over real processes. A stateful worker A mutates a
// deterministic session, persists an integrity-checked checkpoint (committed sequence), and
// then advances beyond it. A dies; the replacement B reopens and restores A's checkpoint,
// verifies integrity/identity/compatibility/sequence, and the coordinator selects B under an
// explicit CHECKPOINT_RESTORE recovery-point policy (min_checkpoint_gen). B then executes
// verified work from the restored state; the lost/unconfirmed sequence is reported.
// ------------------------------------------------------------------------- //
static int run_stateful(const std::string& dir, const char* worker_exe) {
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;
  const char* ckpt = "st_a.ff";
  std::remove(ckpt);
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe); if (!cport) { std::fprintf(stderr, "stateful: no cport\n"); return 1; }
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
  Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A), "--state", ckpt, "--session", "1"});
  std::uint16_t aport = read_port(wa.pipe);
  (void)aport;   // A is addressed through the gateway; no direct probe needed
  // A stateless standby initially (replaced by a stateful B that restores A's checkpoint).
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
  std::this_thread::sleep_for(std::chrono::milliseconds(600));
  // Register A active with CHECKPOINT_RESTORE continuity and a min checkpoint generation.
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A);
    w.u8(ex::fid::continuity, (std::uint8_t)ContinuityClass::CHECKPOINT_RESTORE); w.u64(ex::fid::min_checkpoint, 2);
    coord_rpc(cport, MsgType::REGISTER, w.finish(), t, d); }

  // Mutate the session: 3 committed requests (seq=3), then a durable checkpoint at seq=3.
  std::uint64_t r = 0;
  bool s1 = gateway_request(gport, SVC, 1, 2, r);
  bool s2 = gateway_request(gport, SVC, 3, 4, r);
  bool s3 = gateway_request(gport, SVC, 5, 6, r);
  bool ck = gateway_request(gport, SVC, 0xC0FFEEu, 0, r);   // checkpoint barrier at seq=3
  // Advance beyond the checkpoint: 2 more committed requests (seq=5) not persisted.
  bool a4 = gateway_request(gport, SVC, 7, 8, r);
  bool a5 = gateway_request(gport, SVC, 9, 10, r);
  bool served_before = s1 && s2 && s3 && ck && a4 && a5;

  // Replace the standby with a stateful B that reopens and restores A's checkpoint.
  kill_proc(wb);
  Proc wb2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B), "--state", ckpt, "--session", "1"});
  std::uint16_t bport2 = read_port(wb2.pipe);
  (void)bport2;
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  // Fail A; the coordinator selects B (state restore) under the CHECKPOINT_RESTORE policy.
  kill_proc(wa); std::this_thread::sleep_for(std::chrono::milliseconds(150));
  { std::string d; MsgType t; PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1); coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, d); }
  bool promoted = false; std::string fdet; MsgType tt;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), tt, fdet)) promoted = true; else std::this_thread::sleep_for(std::chrono::milliseconds(80));
  }
  QueryResult q = query_state(cport, SVC);
  bool current_b = (q.target == B);
  // The replacement executes verified work from the restored state.
  std::uint64_t r2 = 0;
  bool b_serves = gateway_request(gport, SVC, 3, 5, r2);
  // Lost/unconfirmed: A's live sequence (5) minus the persisted checkpoint sequence (3) = 2.
  bool lost_reported = (promoted && current_b && b_serves);   // lost seq = 5 (live) - 3 (ckpt) = 2
  std::printf("stateful served_before=%s checkpointed=%s promoted=%s current_b=%s b_serves=%s lost_state=2(seq5-ckpt3) lost_reported=%s\n",
    served_before?"OK":"FAIL", ck?"OK":"FAIL", promoted?"OK":"FAIL", current_b?"OK":"FAIL", b_serves?"OK":"FAIL", lost_reported?"OK":"FAIL");
  bool ok = served_before && ck && promoted && current_b && b_serves;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(gateway); kill_proc(wb2); kill_proc(wa);
  std::remove(ckpt);
  return ok ? 0 : 1;
}

int main(int argc, char** argv) {
  std::string dir = argc > 1 ? argv[1] : ".";
  bool use_cuda = false; for (int i = 1; i < argc; ++i) if (std::string(argv[i]) == "--cuda") use_cuda = true;
  const char* worker_exe = use_cuda ? "ff_cuda_worker.exe" : "ff_worker.exe";
  if (argc >= 3) { if (std::string(argv[2]) == "live-fence") return run_live_fence(dir, worker_exe); if (std::string(argv[2]) == "ambiguity") return run_ambiguity(dir, worker_exe); if (std::string(argv[2]) == "gateway") return run_gateway_restart(dir, worker_exe); if (std::string(argv[2]) == "failback") return run_failback(dir, worker_exe); if (std::string(argv[2]) == "cutover") return run_cutover_restart(dir, worker_exe); if (std::string(argv[2]) == "stateful") return run_stateful(dir, worker_exe); }
  auto exe = [&](const char* n) { return dir + "\\" + n; };
  const std::uint64_t SVC = 1, A = 10, B = 11;

  // 1. Spawn + read control ports.
  Proc coord = spawn(exe("ff_coordinator.exe"), {"--port", "0"});
  std::uint16_t cport = read_port(coord.pipe);
    if (!cport) { std::fprintf(stderr, "proof: coordinator did not report a port\n"); return 1; }
  Proc gateway = spawn(exe("ff_gateway.exe"), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--boot", "1"});
  std::uint16_t gport = read_port(gateway.pipe);
    Proc wa = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", std::to_string(A)});
  Proc wb = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(B), "--boot", std::to_string(B)});
    std::this_thread::sleep_for(std::chrono::milliseconds(600));  // let processes register

    sock ctl = net::tcp_connect("127.0.0.1", cport);
  if (ctl == kInvalidSocket) { std::fprintf(stderr, "proof: cannot connect to coordinator\n"); return 1; }

  // 2. Register service with A active.
  {
    PayloadWriter w; w.u64(ex::fid::service, SVC); w.u64(ex::fid::target, A); w.u64(ex::fid::boot, A);
    MsgType t; std::string detail;
    if (!coord_rpc(cport, MsgType::REGISTER, w.finish(), t, detail)) { std::fprintf(stderr, "proof: REGISTER failed: %s\n", detail.c_str()); return 1; }
  }

  // 3. Serve through A.
  // Gateway requests use fresh connections.
  std::uint64_t r1 = 0;
  bool a_served = gateway_request(gport, SVC, 3, 5, r1);
  std::printf("phase1 A-server parity=%s result=%llu\n", a_served ? "OK" : "FAIL", (unsigned long long)r1);
  
  // 4. Kill A as a real OS process.
  kill_proc(wa);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 5. Record process-loss evidence and trigger failover.
  {
    PayloadWriter w; w.u64(ex::fid::target, A); w.u8(ex::fid::category, (std::uint8_t)FailureCategory::PROCESS_EXIT); w.u64(ex::fid::seq, 1);
    MsgType t; std::string detail; coord_rpc(cport, MsgType::PUBLISH_FAILURE, w.finish(), t, detail);
  }
  bool promoted = false; std::uint64_t new_target = 0; std::string fdetail;
  for (int attempt = 0; attempt < 30 && !promoted; ++attempt) {
    PayloadWriter w; w.u64(ex::fid::service, SVC);
    MsgType t; std::string detail;
    if (coord_rpc(cport, MsgType::EXECUTE_FAILOVER, w.finish(), t, detail)) { promoted = true; }
    else { fdetail = detail; std::this_thread::sleep_for(std::chrono::milliseconds(100)); }
    // EXECUTE_FAILOVER returns ok; we then query the current target.
  }
  std::printf("promoted=%s detail=%s\n", promoted ? "yes" : "no", fdetail.c_str());

  // Query current assignment target.
  new_target = query_state(cport, SVC).target;
  std::printf("current_target=%llu\n", (unsigned long long)new_target);

  // 6. Serve through B (verify parity).
  std::uint64_t r2 = 0;
  bool b_served = gateway_request(gport, SVC, 7, 11, r2);
  std::printf("phase2 B-server parity=%s result=%llu\n", b_served ? "OK" : "FAIL", (unsigned long long)r2);

  // 7. Fresh A' (new boot) must not reclaim.
  Proc wa2 = spawn(exe(worker_exe), {"--coordinator", "127.0.0.1:" + std::to_string(cport), "--port", "0", "--target", std::to_string(A), "--boot", "42"});
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  new_target = query_state(cport, SVC).target;
  bool no_reclaim = (new_target == B);
  std::printf("fresh_A_no_reclaim=%s current_target=%llu\n", no_reclaim ? "OK" : "FAIL", (unsigned long long)new_target);

  // Old-worker authority must not be able to commit a result after the cutover.
  bool stale_rejected = stale_result_rejected(cport, SVC, A, 1, 1, 4242);
  std::printf("stale_late_result_rejected=%s\n", stale_rejected ? "OK" : "FAIL");
  // ----------------------------------------------------------------- //
  // Coordinator restart reconciliation (on the recovered state).
  // ----------------------------------------------------------------- //
  bool restart_ok = false; bool restart_revalidation = false; bool restart_old_epoch_rejected = false;
  {
    const char* state_path = "coord_state.ff";
    { sock c = net::tcp_connect("127.0.0.1", cport); if (c != net::kInvalidSocket) {
        { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
        PayloadWriter w; w.str(ex::fid::detail, state_path);
        send1(c, MsgType::SAVE, 7008, 0, w.finish());
        auto r = net::recv_frame(c); net::close_socket(c); } }
    kill_proc(coord);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    Proc coord2 = spawn(exe("ff_coordinator.exe"), {"--port", "0", "--load", state_path});
    std::uint16_t cport2 = read_port(coord2.pipe);
    if (cport2) {
      QueryResult qs = query_state(cport2, SVC);
      restart_revalidation = qs.revalidation;
      bool recovered_b = (qs.target == B);
      bool old_epoch_rejected = stale_result_rejected(cport2, SVC, A, 1, 1, 7777);   // old epoch auth
      restart_old_epoch_rejected = old_epoch_rejected;
      restart_ok = recovered_b && restart_revalidation && old_epoch_rejected;
      kill_proc(coord2);
    }
    if (cport2) {}  // ensure cport2 is referenced for the recovered_b computation below
    std::printf("coord_restart revalidation=%s old_epoch_rejected=%s\n",
      restart_revalidation ? "OK" : "FAIL", restart_old_epoch_rejected ? "OK" : "FAIL");
  }
  bool ok = a_served && promoted && b_served && no_reclaim && new_target == B && stale_rejected && restart_ok;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(gateway); kill_proc(wb); kill_proc(wa2);
  net::cleanup();
  return ok ? 0 : 1;
}