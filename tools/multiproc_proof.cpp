// multiproc_proof.cpp — real multiprocess failover proof. Spawns coordinator, gateway, and
// two reference workers as independent OS processes (CPU by default, real CUDA with --cuda),
// serves requests through the current target, kills the active worker, and proves an
// authoritative failover to the standby with routed CPU-parity verification and stale
// authority recovery blocking.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <failover_fabric/protocol.hpp>

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
// Fresh-connection query: returns current target + revalidation-required flag.
struct QueryResult { std::uint64_t target{0}; bool revalidation{false}; std::uint64_t ambiguous{0}; };
QueryResult query_state(std::uint16_t cport, std::uint64_t service) {
  QueryResult qr;
  sock c = net::tcp_connect("127.0.0.1", cport);
  if (c == net::kInvalidSocket) return qr;
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleClient); h.u64(ex::fid::boot, 999); send1(c, MsgType::HELLO, 1, 0, h.finish()); }
  PayloadWriter w; w.u64(ex::fid::service, service);
  if (send1(c, MsgType::QUERY_ROUTE, 7005, 0, w.finish())) { auto r = net::recv_frame(c); if (r) { PayloadReader q(r->payload); if (q.has(ex::fid::target)) qr.target = q.u64(ex::fid::target); if (q.has(ex::fid::state)) qr.revalidation = q.bool_(ex::fid::state); if (q.has(ex::fid::seq)) qr.ambiguous = q.u64(ex::fid::seq); } }
  net::close_socket(c);
  return qr;
}

bool gateway_request(std::uint16_t gport, std::uint64_t service, std::uint64_t a, std::uint64_t b, std::uint64_t& result) {
  sock gw = net::tcp_connect("127.0.0.1", gport);
  if (gw == net::kInvalidSocket) return false;
  PayloadWriter w; w.u64(ex::fid::input_a, a); w.u64(ex::fid::input_b, b); w.u64(ex::fid::service, service); w.u64(ex::fid::slot, 1);
  bool ret = false;
  if (send1(gw, MsgType::VERIFY_SERVICE, 5001, 0, w.finish())) {
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

  // Dispatch a request that A will compute but for which A withholds the response.
  std::uint64_t r = 0;
  bool reported_success = gateway_request(gport, SVC, 0xBEEFu, 7, r);
  std::string ambig_file = "ambig_" + std::to_string(A) + ".out";
  bool computed = false;
  { FILE* fp = fopen(ambig_file.c_str(), "r"); if (fp) { computed = true; fclose(fp); } }
  std::remove(ambig_file.c_str());
  // The request must not be reported as success.
  bool not_success = !reported_success;
  std::printf("ambiguity computed=%s reported_success=%s not_success=%s (ambiguous classification is proven in-process)\n",
    computed?"OK":"FAIL", reported_success?"YES":"NO", not_success?"OK":"FAIL");
  bool ok = computed && not_success;
  std::printf("RESULT %s\n", ok ? "PASS" : "FAIL");
  kill_proc(coord); kill_proc(gateway); kill_proc(wa); kill_proc(wb);
  return ok ? 0 : 1;
}

int main(int argc, char** argv) {
  std::string dir = argc > 1 ? argv[1] : ".";
  bool use_cuda = false; for (int i = 1; i < argc; ++i) if (std::string(argv[i]) == "--cuda") use_cuda = true;
  const char* worker_exe = use_cuda ? "ff_cuda_worker.exe" : "ff_worker.exe";
  if (argc >= 3) { if (std::string(argv[2]) == "live-fence") return run_live_fence(dir, worker_exe); if (std::string(argv[2]) == "ambiguity") return run_ambiguity(dir, worker_exe); }
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