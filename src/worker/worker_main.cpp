// worker_main.cpp — the reference CPU worker. Registers, warms up, and serves the
// deterministic reference workload; can be killed as a real OS process during failover.
// With --state <path> --session <id> it becomes a stateful worker: it persists an
// integrity-checked checkpoint (committed sequence + session identity) and can restore
// one on startup, so stateful-restore recovery can be exercised over real processes.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }
bool file_exists(const std::string& p) { std::ifstream in(p, std::ios::binary); return in.good(); }

namespace {
std::uint32_t crc32(const std::uint8_t* p, std::size_t n) {
  std::uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < n; ++i) {
    crc ^= p[i];
    for (int k = 0; k < 8; ++k) crc = (crc >> 1) ^ (0xEDB88320u & (~((crc & 1u) - 1u)));
  }
  return ~crc;
}
void put32(std::vector<std::uint8_t>& b, std::uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back((std::uint8_t)((v >> (8 * i)) & 0xff)); }
void put64(std::vector<std::uint8_t>& b, std::uint64_t v) { for (int i = 0; i < 8; ++i) b.push_back((std::uint8_t)((v >> (8 * i)) & 0xff)); }
// Persist an integrity-checked checkpoint: magic, version, session, committed seq,
// model length + bytes, then a CRC of the whole body.
bool save_checkpoint(const std::string& path, std::uint64_t session, std::uint64_t seq, const std::string& model) {
  std::vector<std::uint8_t> body;
  body.push_back('F'); body.push_back('F'); body.push_back('C'); body.push_back('K');
  body.push_back(1);                          // version
  put64(body, session); put64(body, seq);
  put32(body, (std::uint32_t)model.size());
  body.insert(body.end(), model.begin(), model.end());
  put32(body, crc32(body.data(), body.size()));
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  out.write((const char*)body.data(), (std::streamsize)body.size());
  return out.good();
}
bool load_checkpoint(const std::string& path, std::uint64_t want_session, std::uint64_t& seq,
                     std::string& model, bool& correct_session, bool& integrity_ok) {
  correct_session = false; integrity_ok = false; seq = 0; model.clear();
  std::ifstream in(path, std::ios::binary | std::ios::ate);
  if (!in) return false;
  std::streamsize size = in.tellg();
  if (size < 20) return false;
  in.seekg(0, std::ios::beg);
  std::vector<std::uint8_t> blob((std::size_t)size);
  in.read((char*)blob.data(), size);
  if (!in) return false;
  // Magic + version.
  if (blob.size() < 5 || blob[0] != 'F' || blob[1] != 'F' || blob[2] != 'C' || blob[3] != 'K' || blob[4] != 1) return false;
  // body = [magic(4)+ver(1)][session(8)][seq(8)][model_len(4)][model][crc(4)]
  std::size_t off = 5;
  auto rd64 = [&](std::uint64_t& v) { v = 0; for (int i = 0; i < 8; ++i) v |= (std::uint64_t)blob[off + i] << (8 * i); off += 8; };
  auto rd32 = [&](std::uint32_t& v) { v = 0; for (int i = 0; i < 4; ++i) v |= (std::uint32_t)blob[off + i] << (8 * i); off += 4; };
  std::uint64_t session = 0; rd64(session);
  rd64(seq);
  std::uint32_t mlen = 0; rd32(mlen);
  if (off + mlen + 4 > blob.size()) return false;
  model.assign((const char*)blob.data() + off, mlen); off += mlen;
  std::uint32_t crc_stored = 0; rd32(crc_stored);
  std::uint32_t crc_calc = crc32(blob.data(), blob.size() - 4);
  integrity_ok = (crc_calc == crc_stored);
  correct_session = (session == want_session);
  // Return true if the checkpoint is well-formed enough to be read at all.
  return off == blob.size();
}
}

int main(int argc, char** argv) {
  std::string coord_host = "127.0.0.1"; std::uint16_t coord_port = 0, port = 0;
  std::uint64_t target = 1, boot = 1;
  std::string state_path; std::uint64_t session = 0;
  for (int i = 1; i < argc - 1; ++i) {
    std::string k = argv[i];
    if (k == "--coordinator") { auto p = std::string(argv[i+1]); auto c = p.find(':'); coord_host = p.substr(0, c); coord_port = (std::uint16_t)parse_u64(p.c_str()+c+1); }
    else if (k == "--port") port = (std::uint16_t)parse_u64(argv[i+1]);
    else if (k == "--target") target = parse_u64(argv[i+1]);
    else if (k == "--boot") boot = parse_u64(argv[i+1]);
    else if (k == "--state") state_path = argv[i+1];
    else if (k == "--session") session = parse_u64(argv[i+1]);
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
  std::string model_key = "ref-det-sequence-v1";

  // Restore a durable checkpoint on startup (stateful mode).
  std::atomic<std::uint64_t> committed_seq{0};
  bool stateful = !state_path.empty();
  bool restored = !stateful;   // stateless workers are trivially "ready"
  if (stateful) {
    std::uint64_t rseq = 0; std::string rmodel; bool cs = false, io = false;
    if (file_exists(state_path) && load_checkpoint(state_path, session, rseq, rmodel, cs, io)) {
      committed_seq.store(rseq); if (!rmodel.empty()) model_key = rmodel;
      restored = io && cs;
    }
  }

  // Publish readiness (and, when stateful, the candidate state for recovery-point selection).
  { PayloadWriter w; w.u64(ex::fid::target, target); w.u64(ex::fid::boot, boot); w.bool_(ex::fid::ready, true);
    w.str(ex::fid::model, model_key); w.str(ex::fid::abi, "ff-ref-v1"); w.f64(ex::fid::capacity, 2.0);
    if (stateful) {
      w.bool_(ex::fid::state_avail, restored);
      w.u64(ex::fid::checkpoint_gen, committed_seq.load() + 1);
      w.u64(ex::fid::committed_seq, committed_seq.load());
      w.bool_(ex::fid::session_ok, restored);
      w.bool_(ex::fid::tenant_ok, true);
      w.bool_(ex::fid::model_ok, true);
      w.bool_(ex::fid::format_ok, restored);
      w.bool_(ex::fid::integrity_ok, restored);
      w.bool_(ex::fid::replay_safe, true);
    }
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
      if (active.load() && req_boot == my_boot) {
        if (a == 0xDBADu) {
          net::close_socket(ctrl);
          w.bool_(ex::fid::ok, true);
        } else if (a == 0xBEEFu) {
          std::uint64_t res = ex::reference_result(a & 0xFFFFFFFFFFFFu, b);
          std::string of = "ambig_" + std::to_string(target) + ".out";
          FILE* fp = fopen(of.c_str(), "w"); if (fp) { fprintf(fp, "%llu\n", (unsigned long long)res); fclose(fp); }
          net::close_socket(c);
          return;
        } else if (a == 0xC0FFEEu) {
          // Explicit durable checkpoint barrier: persist the current committed sequence.
          if (stateful) save_checkpoint(state_path, session, committed_seq.load(), model_key);
          std::uint64_t res = ex::reference_result(a & 0xFFFFFFFFFFFFu, b);
          std::string rs = std::to_string(res);
          w.bool_(ex::fid::ok, true);
          std::vector<std::uint8_t> rb(rs.begin(), rs.end()); w.bytes(ex::fid::payload, rb);
        } else {
          std::uint64_t res = ex::reference_result(a, b);
          committed_seq.fetch_add(1);
          std::string rs = std::to_string(res);
          w.bool_(ex::fid::ok, true);
          std::vector<std::uint8_t> rb(rs.begin(), rs.end()); w.bytes(ex::fid::payload, rb);
        }
      } else {
        w.bool_(ex::fid::ok, false); w.str(ex::fid::detail, "not active or stale boot");
      }
      ex::send_msg(c, MsgType::EXECUTION_RESULT, f ? f->msg_id : 0, f ? f->epoch : 0, w.finish());
      net::close_socket(c);
    }).detach();
  }
  return 0;
}
