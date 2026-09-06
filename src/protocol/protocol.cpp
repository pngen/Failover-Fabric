// protocol.cpp — frame + bounded TLV payload codec.
#include "failover_fabric/protocol.hpp"

#include <cstring>

namespace failover_fabric {

const char* to_string(MsgType m) noexcept {
  switch (m) {
    case MsgType::HELLO: return "HELLO";
    case MsgType::REGISTER: return "REGISTER";
    case MsgType::PUBLISH_TARGET: return "PUBLISH_TARGET";
    case MsgType::PUBLISH_READINESS: return "PUBLISH_READINESS";
    case MsgType::PUBLISH_FAILURE: return "PUBLISH_FAILURE";
    case MsgType::PUBLISH_CAPACITY: return "PUBLISH_CAPACITY";
    case MsgType::PLAN_FAILOVER: return "PLAN_FAILOVER";
    case MsgType::EXECUTE_FAILOVER: return "EXECUTE_FAILOVER";
    case MsgType::PREPARE_TARGET: return "PREPARE_TARGET";
    case MsgType::PREPARE_RESULT: return "PREPARE_RESULT";
    case MsgType::FENCE_ASSIGNMENT: return "FENCE_ASSIGNMENT";
    case MsgType::ACTIVATE_TARGET: return "ACTIVATE_TARGET";
    case MsgType::ACTIVATE_RESULT: return "ACTIVATE_RESULT";
    case MsgType::INSTALL_ROUTE: return "INSTALL_ROUTE";
    case MsgType::ROUTE_ACK: return "ROUTE_ACK";
    case MsgType::QUERY_ROUTE: return "QUERY_ROUTE";
    case MsgType::AUTHORIZE_REQUEST: return "AUTHORIZE_REQUEST";
    case MsgType::AUTHORIZE_RESULT: return "AUTHORIZE_RESULT";
    case MsgType::EXECUTION_RESULT: return "EXECUTION_RESULT";
    case MsgType::VERIFY_SERVICE: return "VERIFY_SERVICE";
    case MsgType::REQUEST_FAILBACK: return "REQUEST_FAILBACK";
    case MsgType::REVALIDATE: return "REVALIDATE";
    case MsgType::SAVE: return "SAVE";
    case MsgType::SHUTDOWN: return "SHUTDOWN";
    case MsgType::ERROR_MSG: return "ERROR";
  }
  return "UNKNOWN";
}

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
std::uint32_t get32(const std::uint8_t* p) { std::uint32_t v = 0; for (int i = 0; i < 4; ++i) v |= (std::uint32_t)p[i] << (8 * i); return v; }
std::uint64_t get64(const std::uint8_t* p) { std::uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= (std::uint64_t)p[i] << (8 * i); return v; }
constexpr std::uint8_t kMagic[4] = { 'F','F','P','1' };
constexpr std::uint8_t kVersion = 1;
}  // namespace

std::vector<std::uint8_t> encode_frame(const Frame& f) {
  std::vector<std::uint8_t> b;
  b.insert(b.end(), kMagic, kMagic + 4);
  b.push_back(kVersion);
  b.push_back((std::uint8_t)f.type);
  put32(b, f.msg_id);
  put64(b, f.epoch);
  if (f.payload.size() > kMaxFramePayload) throw ProtocolError("frame payload too large");
  put32(b, (std::uint32_t)f.payload.size());
  b.insert(b.end(), f.payload.begin(), f.payload.end());
  std::uint32_t crc = crc32(b.data(), b.size());
  put32(b, crc);
  return b;
}

Frame decode_frame(const std::uint8_t* data, std::size_t n) {
  if (n < 31) throw ProtocolError("frame too short");
  if (std::memcmp(data, kMagic, 4) != 0) throw ProtocolError("bad frame magic");
  std::size_t off = 4;
  if (data[off++] != kVersion) throw ProtocolError("unsupported frame version");
  Frame f;
  std::uint8_t t = data[off++];
  // Only accept valid message-type enums.
  if (t < (std::uint8_t)MsgType::HELLO || t > (std::uint8_t)MsgType::ERROR_MSG) throw ProtocolError("invalid message type");
  f.type = (MsgType)t;
  f.msg_id = get32(data + off); off += 4;
  f.epoch = get64(data + off); off += 8;
  std::uint32_t plen = get32(data + off); off += 4;
  if (plen > kMaxFramePayload) throw ProtocolError("frame payload too large");
  if (off + plen + 4 != n) throw ProtocolError("frame length mismatch");
  std::uint32_t crc_stored = get32(data + off + plen);
  if (crc32(data, off + plen) != crc_stored) throw ProtocolError("frame checksum mismatch");
  f.payload.assign(data + off, data + off + plen);
  return f;
}

// --------------------------------------------------------------------------- //
// PayloadWriter
// --------------------------------------------------------------------------- //
namespace {
void field_header(std::vector<std::uint8_t>& b, std::uint8_t id, std::uint8_t type, std::uint32_t size) {
  b.push_back(id); b.push_back(type); put32(b, size);
  if (b.size() > kMaxFramePayload) throw ProtocolError("payload too large");
}
}  // namespace

void PayloadWriter::u8(std::uint8_t field, std::uint8_t v) { field_header(buf_, field, 0, 1); buf_.push_back(v); }
void PayloadWriter::u32(std::uint8_t field, std::uint32_t v) { field_header(buf_, field, 1, 4); put32(buf_, v); }
void PayloadWriter::u64(std::uint8_t field, std::uint64_t v) { field_header(buf_, field, 2, 8); put64(buf_, v); }
void PayloadWriter::bool_(std::uint8_t field, bool v) { field_header(buf_, field, 3, 1); buf_.push_back(v ? 1 : 0); }
void PayloadWriter::i64(std::uint8_t field, std::int64_t v) { field_header(buf_, field, 4, 8); put64(buf_, (std::uint64_t)v); }
void PayloadWriter::f64(std::uint8_t field, double v) { field_header(buf_, field, 5, 8); std::uint64_t u; std::memcpy(&u, &v, 8); put64(buf_, u); }
void PayloadWriter::str(std::uint8_t field, const std::string& v) {
  if (v.size() > kMaxFramePayload) throw ProtocolError("string too large");
  field_header(buf_, field, 6, (std::uint32_t)v.size());
  buf_.insert(buf_.end(), v.begin(), v.end());
}
void PayloadWriter::bytes(std::uint8_t field, const std::vector<std::uint8_t>& v) {
  if (v.size() > kMaxFramePayload) throw ProtocolError("bytes too large");
  field_header(buf_, field, 7, (std::uint32_t)v.size());
  buf_.insert(buf_.end(), v.begin(), v.end());
}
std::vector<std::uint8_t> PayloadWriter::finish() { return std::move(buf_); }

// --------------------------------------------------------------------------- //
// PayloadReader
// --------------------------------------------------------------------------- //
namespace {
void ensure(const std::vector<std::uint8_t>::const_iterator&, std::size_t) {}
}
PayloadReader::PayloadReader(const std::vector<std::uint8_t>& data) {
  std::size_t i = 0;
  const std::size_t n = data.size();
  while (i < n) {
    if (i + 6 > n) throw ProtocolError("truncated TLV field");
    std::uint8_t id = data[i++];
    std::uint8_t type = data[i++];
    std::uint32_t size = (std::uint32_t)data[i] | ((std::uint32_t)data[i+1] << 8) | ((std::uint32_t)data[i+2] << 16) | ((std::uint32_t)data[i+3] << 24);
    i += 4;
    if (type > 7) throw ProtocolError("invalid TLV field type");
    if (size > kMaxFramePayload) throw ProtocolError("TLV field too large");
    if (i + size > n) throw ProtocolError("TLV field length exceeds payload");
    fields_.push_back(Field{id, type, std::vector<std::uint8_t>(data.begin() + i, data.begin() + i + size)});
    i += size;
  }
}
const PayloadReader::Field* PayloadReader::find_(std::uint8_t field) const {
  for (const Field& f : fields_) if (f.id == field) return &f;
  return nullptr;
}
bool PayloadReader::has(std::uint8_t field) const { return find_(field) != nullptr; }
std::uint8_t PayloadReader::u8(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 0 || f->value.size() != 1) throw ProtocolError("missing/incorrect u8 field");
  return f->value[0];
}
std::uint32_t PayloadReader::u32(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 1 || f->value.size() != 4) throw ProtocolError("missing/incorrect u32 field");
  return (std::uint32_t)f->value[0] | ((std::uint32_t)f->value[1] << 8) | ((std::uint32_t)f->value[2] << 16) | ((std::uint32_t)f->value[3] << 24);
}
std::uint64_t PayloadReader::u64(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 2 || f->value.size() != 8) throw ProtocolError("missing/incorrect u64 field");
  std::uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= (std::uint64_t)f->value[i] << (8 * i); return v;
}
bool PayloadReader::bool_(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 3 || f->value.size() != 1) throw ProtocolError("missing/incorrect bool field");
  if (f->value[0] > 1) throw ProtocolError("invalid bool value");
  return f->value[0] == 1;
}
std::int64_t PayloadReader::i64(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 4 || f->value.size() != 8) throw ProtocolError("missing/incorrect i64 field");
  std::uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= (std::uint64_t)f->value[i] << (8 * i); return (std::int64_t)v;
}
double PayloadReader::f64(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 5 || f->value.size() != 8) throw ProtocolError("missing/incorrect f64 field");
  std::uint64_t u = 0; for (int i = 0; i < 8; ++i) u |= (std::uint64_t)f->value[i] << (8 * i);
  double v; std::memcpy(&v, &u, 8); return v;
}
std::string PayloadReader::str(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 6) throw ProtocolError("missing/incorrect string field");
  return std::string((const char*)f->value.data(), f->value.size());
}
std::vector<std::uint8_t> PayloadReader::bytes(std::uint8_t field) {
  const Field* f = find_(field);
  if (!f || f->type != 7) throw ProtocolError("missing/incorrect bytes field");
  return f->value;
}

}  // namespace failover_fabric