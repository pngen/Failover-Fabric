// conn.hpp — a per-connection transport with a single inbound-decoding owner.
//
// One reader thread owns inbound frame decoding for a socket. A serialized writer guarantees
// complete frames are never interleaved. Requests are correlated to their responses by
// message id; unsolicited events are dispatched to a handler outside the state lock. A
// connection carries a generation, and a pending request completes as failed on disconnect.
// Pending requests are bounded and completed exactly once on disconnect. No state lock is
// held while waiting for a network reply.
#pragma once

#include "socket_util.hpp"
#include <failover_fabric/protocol.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <thread>

namespace failover_fabric::net {

class Conn {
 public:
  using sock = socket_t;
  using UnsolicitedFn = std::function<void(const Frame&)>;
  using DeadFn = std::function<void(std::uint64_t)>;

  static constexpr std::uint32_t kMaxPending = 64;

  Conn(sock s, std::uint64_t generation, UnsolicitedFn on_unsolicited = {}, DeadFn on_dead = {})
      : s_(s), gen_(generation), on_unsolicited_(std::move(on_unsolicited)), on_dead_(std::move(on_dead)) {}

  ~Conn() { shutdown(); }
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;

  void start() { reader_ = std::thread([this] { read_loop(); }); }

  std::uint64_t generation() const noexcept { return gen_; }
  bool alive() const noexcept { return !dead_.load(); }
  std::uint64_t frames_sent() const noexcept { return frames_sent_.load(); }
  std::uint64_t frames_recv() const noexcept { return frames_recv_.load(); }

  // Serialized complete-frame write.
  bool send_frame(const Frame& f) {
    std::lock_guard<std::mutex> lk(write_mtx_);
    if (s_ == kInvalidSocket || dead_.load()) return false;
    bool ok = net::send_frame(s_, f);
    if (ok) frames_sent_.fetch_add(1);
    return ok;
  }
  bool send(MsgType type, std::uint32_t msg_id, std::uint64_t epoch, const std::vector<std::uint8_t>& payload = {}) {
    Frame f; f.type = type; f.msg_id = msg_id; f.epoch = epoch; f.payload = payload;
    return send_frame(f);
  }

  // Send a request and block until the matching response arrives (or the connection dies).
  std::optional<Frame> request(MsgType type, std::uint32_t msg_id, std::uint64_t epoch,
                               const std::vector<std::uint8_t>& payload = {}) {
    if (dead_.load() || s_ == kInvalidSocket) return std::nullopt;
    std::unique_lock<std::mutex> lk(lock_);
    if (pending_.size() >= kMaxPending) return std::nullopt;
    auto w = std::make_shared<Pending>();
    pending_[msg_id] = w;
    lk.unlock();

    Frame f; f.type = type; f.msg_id = msg_id; f.epoch = epoch; f.payload = payload;
    if (!send_frame(f)) {
      std::lock_guard<std::mutex> l2(lock_);
      auto it = pending_.find(msg_id);
      if (it != pending_.end()) { it->second->done = true; it->second->cv.notify_all(); pending_.erase(it); }
      return std::nullopt;
    }

    lk.lock();
    if (!w->done) w->cv.wait(lk, [&] { return w->done; });
    std::optional<Frame> out = std::move(w->response);
    auto it = pending_.find(msg_id);
    if (it != pending_.end()) pending_.erase(it);
    return out;
  }

  void resolve_pending(std::optional<Frame> result) {
    std::lock_guard<std::mutex> lk(lock_);
    for (auto& kv : pending_) {
      kv.second->response = result;
      kv.second->done = true;
      kv.second->cv.notify_all();
    }
    pending_.clear();
  }

  // Stop inbound decoding, resolve pending requests, and join the reader thread. The socket
  // is closed exactly once (either by a reader EOF or here).
  void shutdown() {
    if (already_shutdown_.exchange(true)) return;
    stop_.store(true);
    if (s_ != kInvalidSocket) { net::close_socket(s_); s_ = kInvalidSocket; }
    resolve_pending(std::nullopt);
    if (reader_.joinable()) reader_.join();
  }

 private:
  struct Pending { std::mutex m; std::condition_variable cv; bool done{false}; std::optional<Frame> response; };

  void read_loop() {
    while (!stop_.load()) {
      auto f = net::recv_frame(s_);
      if (!f) break;
      frames_recv_.fetch_add(1);
      bool handled = false;
      {
        std::lock_guard<std::mutex> lk(lock_);
        auto it = pending_.find(f->msg_id);
        if (it != pending_.end()) {
          it->second->response = *f;
          it->second->done = true;
          it->second->cv.notify_all();
          handled = true;
        }
      }
      if (!handled) {
        // Unsolicited event: dispatch OUTSIDE the state lock.
        if (on_unsolicited_) on_unsolicited_(*f);
        else unsolicited_dropped_.fetch_add(1);
      }
    }
    dead_.store(true);
    resolve_pending(std::nullopt);
    if (on_dead_) on_dead_(gen_);
  }

  sock s_;
  std::uint64_t gen_;
  UnsolicitedFn on_unsolicited_;
  DeadFn on_dead_;
  std::atomic<bool> dead_{false};
  std::atomic<bool> stop_{false};
  std::atomic<bool> already_shutdown_{false};
  std::thread reader_;
  std::mutex write_mtx_;
  std::atomic<std::uint64_t> frames_sent_{0};
  std::atomic<std::uint64_t> frames_recv_{0};
  std::atomic<std::uint64_t> unsolicited_dropped_{0};
  std::mutex lock_;
  std::map<std::uint32_t, std::shared_ptr<Pending>> pending_;
};

}  // namespace failover_fabric::net