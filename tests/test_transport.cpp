// test_transport.cpp — regression tests for the actual socket/transport defects.
#include "framework.hpp"

#include "net/socket_util.hpp"
#include "net/common.hpp"
#include "net/conn.hpp"

#include <failover_fabric/protocol.hpp>

#include <atomic>
#include <thread>

using namespace failover_fabric;
using net::Conn;
using net::socket_t;

namespace {
// Cheap loopback socket pair (server listen + accepted, client connect).
struct Pair {
  socket_t listen{net::kInvalidSocket};
  socket_t server{net::kInvalidSocket};
  socket_t client{net::kInvalidSocket};
  ~Pair() { if (server != net::kInvalidSocket) net::close_socket(server);
            if (client != net::kInvalidSocket) net::close_socket(client);
            if (listen != net::kInvalidSocket) net::close_socket(listen); }
  static Pair make() {
    Pair p;
    p.listen = net::tcp_listen(0);
    std::thread([&]{ p.server = net::tcp_accept(p.listen); }).detach();
    p.client = net::tcp_connect("127.0.0.1", net::tcp_listen_port(p.listen));
    // Wait for accept.
    for (int i = 0; i < 1000 && p.server == net::kInvalidSocket; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    return p;
  }
};
}  // namespace

FF_TEST(transport_parse_tcp) {
  std::string host; std::uint16_t port = 0;
  FF_CHECK(ex::parse_tcp_transport("tcp://127.0.0.1:6250", host, port));
  FF_CHECK_EQ(host, "127.0.0.1");   // must be the host, not the whole string
  FF_CHECK_EQ((unsigned)port, 6250u);
  FF_CHECK(ex::parse_tcp_transport("tcp://10.0.0.1:8080", host, port));
  FF_CHECK_EQ(host, "10.0.0.1");
  FF_CHECK_EQ((unsigned)port, 8080u);
  // The old bug used find(':') which matched the scheme colon, yielding host=127.0.0.1:6250
  // and port=0. The fixed helper must not do that.
    FF_CHECK(!ex::parse_tcp_transport("pipes/11", host, port));
}

FF_TEST(conn_request_response_and_disconnect) {
  Pair p = Pair::make();
  FF_CHECK(p.server != net::kInvalidSocket && p.client != net::kInvalidSocket);
  int unsolicited = 0;
  auto server = std::make_shared<Conn>(p.server, 7, [&](const Frame&){ ++unsolicited; });
  server->start();
  // Client replies to a request with the same message id.
  std::thread client([&] {
    auto f = net::recv_frame(p.client);
    if (f) {
      PayloadWriter w; w.str(ex::fid::detail, "reply");
      Frame r; r.type = MsgType::REGISTER; r.msg_id = f->msg_id; r.epoch = f->epoch; r.payload = w.finish();
      net::send_frame(p.client, r);
    }
  });
  std::vector<std::uint8_t> req; PayloadWriter wq; wq.u64(ex::fid::service, 1); req = wq.finish();
  auto resp = server->request(MsgType::REGISTER, 4242, 1, req);
  FF_CHECK(resp.has_value());
  FF_CHECK(resp->type == MsgType::REGISTER);
  FF_CHECK_EQ((unsigned)resp->msg_id, 4242u);
  client.join();

  // Disconnect: closing the client socket from another thread must unblock a pending
  // request as failed rather than hang, and the Conn must report dead.
  std::thread closer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(40));
    net::close_socket(p.client);
    p.client = net::kInvalidSocket;
  });
  auto resp2 = server->request(MsgType::QUERY_ROUTE, 9999, 1, req);  // blocks until closer closes
  FF_CHECK(!resp2.has_value());        // pending request resolved as failed, not hung
  FF_CHECK(!server->alive());          // disconnect detected
  closer.join();
  server->shutdown();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
}

FF_TEST(conn_serialized_writes_no_interleave) {
  // A serialized writer must not interleave frames. Two threads writing full frames over one
  // Conn produce two intact frames on the reader.
  Pair p = Pair::make();
  FF_CHECK(p.server != net::kInvalidSocket && p.client != net::kInvalidSocket);
  auto server = std::make_shared<Conn>(p.server, 1);
  server->start();
  std::atomic<bool> go{false};
  auto writer = [&](std::uint32_t id) {
    while (!go.load()) std::this_thread::yield();
    PayloadWriter w; w.u64(ex::fid::request_id, id);
    server->send(MsgType::QUERY_ROUTE, id, 1, w.finish());
  };
  std::thread a(writer, 1001); std::thread b(writer, 1002);
  go.store(true);
  a.join(); b.join();
  // Reader must see exactly the two frames, each intact (correct message id and length).
  auto f1 = net::recv_frame(p.client);
  auto f2 = net::recv_frame(p.client);
  FF_CHECK(f1 && f2);
  bool id_ok = (f1->msg_id == 1001 && f2->msg_id == 1002) || (f1->msg_id == 1002 && f2->msg_id == 1001);
  FF_CHECK(id_ok);
  // Each frame's payload must be intact (corresponds to its own id, not the other thread's).
  bool payload_ok = false;
  { PayloadReader r1(f1->payload); PayloadReader r2(f2->payload);
    if (r1.has(ex::fid::request_id) && r2.has(ex::fid::request_id)) {
      payload_ok = (f1->msg_id == r1.u64(ex::fid::request_id)) && (f2->msg_id == r2.u64(ex::fid::request_id));
    } }
  FF_CHECK(payload_ok);
  server->shutdown();
}

int main() { return ff_test::run_all(); }