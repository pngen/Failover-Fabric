// socket_util.hpp — minimal blocking TCP helper for the reference processes.
// Administrative reference service is bound to loopback only.
#pragma once
#include "failover_fabric/protocol.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#ifdef _WIN32
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <winsock2.h>
  #include <ws2tcpip.h>
#else
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <arpa/inet.h>
  #include <unistd.h>
#endif

namespace failover_fabric::net {
#ifdef _WIN32
  using socket_t = SOCKET;
  static constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
  using socket_t = int;
  static constexpr socket_t kInvalidSocket = -1;
#endif

inline void init() {
#ifdef _WIN32
  static bool done = [] { WSADATA d; WSAStartup(MAKEWORD(2, 2), &d); return true; }();
  (void)done;
#endif
}
inline void cleanup() {
#ifdef _WIN32
  WSACleanup();
#endif
}
inline void close_socket(socket_t s) {
#ifdef _WIN32
  closesocket(s);
#else
  ::close(s);
#endif
}
inline bool send_all(socket_t s, const std::uint8_t* data, std::size_t n) {
  std::size_t off = 0;
  while (off < n) {
#ifdef _WIN32
    int w = ::send(s, (const char*)(data + off), (int)(n - off), 0);
#else
    ssize_t w = ::send(s, (const char*)(data + off), (n - off), 0);
#endif
    if (w <= 0) return false;
    off += (std::size_t)w;
  }
  return true;
}
inline bool recv_all(socket_t s, std::uint8_t* data, std::size_t n) {
  std::size_t off = 0;
  while (off < n) {
#ifdef _WIN32
    int r = ::recv(s, (char*)(data + off), (int)(n - off), 0);
#else
    ssize_t r = ::recv(s, (char*)(data + off), (n - off), 0);
#endif
    if (r <= 0) return false;
    off += (std::size_t)r;
  }
  return true;
}

inline socket_t tcp_listen(std::uint16_t port) {
  init();
  socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == kInvalidSocket) return kInvalidSocket;
  int one = 1;
#ifdef _WIN32
  ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (const char*)&one, sizeof(one));
#else
  ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#endif
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (::bind(s, (sockaddr*)&addr, sizeof(addr)) != 0) { close_socket(s); return kInvalidSocket; }
  if (::listen(s, 16) != 0) { close_socket(s); return kInvalidSocket; }
  return s;
}
inline std::uint16_t tcp_listen_port(socket_t s) {
  sockaddr_in addr{}; int len = sizeof(addr);
  ::getsockname(s, (sockaddr*)&addr, &len);
  return ntohs(addr.sin_port);
}
inline socket_t tcp_accept(socket_t s) {
  sockaddr_in cli{}; int len = sizeof(cli);
  return ::accept(s, (sockaddr*)&cli, &len);
}
inline socket_t tcp_connect(const std::string& host, std::uint16_t port) {
  init();
  socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (s == kInvalidSocket) return kInvalidSocket;
  sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
  ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
  if (::connect(s, (sockaddr*)&addr, sizeof(addr)) != 0) { close_socket(s); return kInvalidSocket; }
  return s;
}

inline bool send_frame(socket_t s, const Frame& f) {
  std::vector<std::uint8_t> b = encode_frame(f);
  std::uint32_t len = (std::uint32_t)b.size();
  std::uint8_t hdr[4] = { (std::uint8_t)(len & 0xff), (std::uint8_t)((len >> 8) & 0xff),
                          (std::uint8_t)((len >> 16) & 0xff), (std::uint8_t)((len >> 24) & 0xff) };
  if (!send_all(s, hdr, 4)) return false;
  return send_all(s, b.data(), b.size());
}
inline std::optional<Frame> recv_frame(socket_t s) {
  std::uint8_t hdr[4];
  if (!recv_all(s, hdr, 4)) return std::nullopt;
  std::uint32_t len = (std::uint32_t)hdr[0] | ((std::uint32_t)hdr[1] << 8) | ((std::uint32_t)hdr[2] << 16) | ((std::uint32_t)hdr[3] << 24);
  if (len > kMaxFramePayload + 64) return std::nullopt;
  std::vector<std::uint8_t> b(len);
  if (!recv_all(s, b.data(), len)) return std::nullopt;
  try { return decode_frame(b.data(), b.size()); }
  catch (...) { return std::nullopt; }
}

}  // namespace failover_fabric::net