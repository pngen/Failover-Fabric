// protocol.hpp — compact versioned framed control protocol.
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace failover_fabric {

enum class MsgType : std::uint8_t {
  HELLO = 1, REGISTER, PUBLISH_TARGET, PUBLISH_READINESS, PUBLISH_FAILURE, PUBLISH_CAPACITY,
  PLAN_FAILOVER, EXECUTE_FAILOVER, PREPARE_TARGET, PREPARE_RESULT, FENCE_ASSIGNMENT,
  ACTIVATE_TARGET, ACTIVATE_RESULT, INSTALL_ROUTE, ROUTE_ACK, QUERY_ROUTE, AUTHORIZE_REQUEST,
  AUTHORIZE_RESULT, EXECUTION_RESULT, VERIFY_SERVICE, REQUEST_FAILBACK, REVALIDATE, SAVE, SHUTDOWN, CHECK_ROUTE_ACK, CLASSIFY_REQUEST, ERROR_MSG
};
const char* to_string(MsgType m) noexcept;

struct ProtocolError : public std::runtime_error {
  using std::runtime_error::runtime_error;
};
constexpr std::uint32_t kMaxFramePayload = 16u << 20;

struct Frame {
  MsgType type{MsgType::ERROR_MSG};
  std::uint32_t msg_id{0};
  std::uint64_t epoch{0};
  std::vector<std::uint8_t> payload;
};

std::vector<std::uint8_t> encode_frame(const Frame& f);
Frame decode_frame(const std::uint8_t* data, std::size_t n);

class PayloadWriter {
 public:
  void u8(std::uint8_t field, std::uint8_t v);
  void u32(std::uint8_t field, std::uint32_t v);
  void u64(std::uint8_t field, std::uint64_t v);
  void bool_(std::uint8_t field, bool v);
  void i64(std::uint8_t field, std::int64_t v);
  void f64(std::uint8_t field, double v);
  void str(std::uint8_t field, const std::string& v);
  void bytes(std::uint8_t field, const std::vector<std::uint8_t>& v);
  std::vector<std::uint8_t> finish();
 private:
  std::vector<std::uint8_t> buf_;
};

class PayloadReader {
 public:
  // Parse all fields strictly (bounds-checked). Throws ProtocolError on malformed fields.
  explicit PayloadReader(const std::vector<std::uint8_t>& data);

  bool has(std::uint8_t field) const;
  std::uint8_t  u8(std::uint8_t field);
  std::uint32_t u32(std::uint8_t field);
  std::uint64_t u64(std::uint8_t field);
  bool          bool_(std::uint8_t field);
  std::int64_t  i64(std::uint8_t field);
  double        f64(std::uint8_t field);
  std::string   str(std::uint8_t field);
  std::vector<std::uint8_t> bytes(std::uint8_t field);
 private:
  struct Field { std::uint8_t id; std::uint8_t type; std::vector<std::uint8_t> value; };
  std::vector<Field> fields_;
  const Field* find_(std::uint8_t field) const;
};

}  // namespace failover_fabric