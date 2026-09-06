// common.hpp — shared protocol field ids and small helpers for the reference processes.
#pragma once
#include <failover_fabric/protocol.hpp>
#include <failover_fabric/route.hpp>
#include "socket_util.hpp"

#include <cstdlib>
#include <string>
#include <vector>

namespace failover_fabric::ex {

namespace fid {
constexpr std::uint8_t role = 1, boot = 2, target = 3, service = 4, slot = 5, seq = 6,
    ready = 7, model = 8, abi = 9, profile = 10, capacity = 11, category = 12, status = 13,
    source = 14, ok = 15, detail = 16, epoch = 17, route_gen = 18, assignment_gen = 19,
    request_id = 20, execution_id = 21, payload = 22, state = 23, transport = 24,
    operation = 25, incarnation = 26, host = 27, assignment_id = 28, gate_port = 29,
    req_port = 30, input_a = 31, input_b = 32, epoch_id = 33, epoch_boot = 34,
    failback = 35, cooldown_ms = 36, max_auto_attempts = 37, authorize = 38,
    checkpoint = 39, checkpoint_seq = 40, session = 41, withheld = 42, retry = 43,
    idempotent = 44, externally_effectful = 45, txn_incomplete = 46, milestone = 47,
    state_avail = 48, checkpoint_gen = 49, committed_seq = 50, session_ok = 51,
    tenant_ok = 52, model_ok = 53, format_ok = 54, integrity_ok = 55, replay_safe = 56,
    continuity = 57, min_checkpoint = 58;
namespace route {
constexpr std::uint8_t id = 40, gen = 41, target = 42, tgtgen = 43, inc = 44, boot = 45,
    assign = 46, assigngen = 47, routegen = 48, slot_service = 49, slot_id = 50,
    transport = 51, state = 52;
}
}  // namespace fid

constexpr std::uint8_t kRoleWorker = 1;
constexpr std::uint8_t kRoleGateway = 2;
constexpr std::uint8_t kRoleClient = 3;

inline std::string encode_workload(std::uint64_t a, std::uint64_t b) {
  return std::to_string(a) + ":" + std::to_string(b) + ":" + std::to_string((a ^ b) * 2654435761ull);
}
inline std::uint64_t reference_result(std::uint64_t a, std::uint64_t b) {
  volatile std::uint64_t acc = 0;
  for (std::uint64_t i = 0; i < 32; ++i) acc = (acc * 31u + (a * i) + (b * (i + 1))) & 0x3FFFFFFFULL;
  return acc;
}

// Parse a tcp://HOST:PORT transport into host and port. Uses the LAST colon (the port
// separator); the FIRST colon belongs to the "tcp://" scheme prefix.
inline bool parse_tcp_transport(const std::string& transport, std::string& host, std::uint16_t& port) {
  host = "127.0.0.1"; port = 0;
  auto p = transport.rfind(":");
  if (p != std::string::npos && transport.rfind("tcp://", 0) == 0) {
    host = transport.substr(6, p - 6);
    port = (std::uint16_t)std::strtoull(transport.c_str() + p + 1, nullptr, 10);
    return true;
  }
  return false;
}

struct Message { std::uint32_t msg_id{0}; std::uint64_t epoch{0}; std::vector<std::uint8_t> payload; };

inline bool send_msg(net::socket_t s, MsgType type, std::uint32_t msg_id, std::uint64_t epoch,
                     const std::vector<std::uint8_t>& payload = {}) {
  Frame f; f.type = type; f.msg_id = msg_id; f.epoch = epoch; f.payload = payload;
  return net::send_frame(s, f);
}

// Encode/decode a RouteEntry into payload fields.
inline void encode_route_entry(PayloadWriter& w, const RouteEntry& e) {
  w.u64(fid::route::id, e.route.value());
  w.u64(fid::route::gen, e.generation.value());
  w.u64(fid::route::target, e.target.value());
  w.u64(fid::route::tgtgen, e.target_generation.value());
  w.u64(fid::route::inc, e.incarnation.value());
  w.u64(fid::route::boot, e.permitted_boot.value());
  w.u64(fid::route::assign, e.assignment.value());
  w.u64(fid::route::assigngen, e.assignment_generation.value());
  w.u64(fid::route::routegen, e.generation.value());
  w.u64(fid::route::slot_service, e.slot.service.value());
  w.u64(fid::route::slot_id, e.slot.slot.value());
  w.str(fid::route::transport, e.transport);
  w.u8(fid::route::state, (std::uint8_t)e.state);
}
inline RouteEntry decode_route_entry(PayloadReader& r) {
  RouteEntry e;
  e.route = RouteId(r.u64(fid::route::id));
  e.generation = RouteGeneration(r.u64(fid::route::gen));
  e.target = TargetId(r.u64(fid::route::target));
  e.target_generation = TargetGeneration(r.u64(fid::route::tgtgen));
  e.incarnation = EngineIncarnationId(r.u64(fid::route::inc));
  e.permitted_boot = WorkerBootId(r.u64(fid::route::boot));
  e.assignment = AssignmentId(r.u64(fid::route::assign));
  e.assignment_generation = AssignmentGeneration(r.u64(fid::route::assigngen));
  e.generation = RouteGeneration(r.u64(fid::route::routegen));
  e.slot.service = ServiceId(r.u64(fid::route::slot_service));
  e.slot.slot = ServiceSlotId(r.u64(fid::route::slot_id));
  e.transport = r.str(fid::route::transport);
  e.state = (RouteState)r.u8(fid::route::state);
  return e;
}

}  // namespace failover_fabric::ex