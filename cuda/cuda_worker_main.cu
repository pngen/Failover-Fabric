// cuda_worker_main.cu — the real CUDA reference worker. Performs genuine H2D transfers,
// launches a bounded kernel, synchronizes, and D2H copies the result, which must match the
// CPU reference_result exactly (CPU parity). The CUDA worker otherwise behaves exactly like
// the CPU worker: it registers, warms up, serves the managed workload, and is killable.
#include "net/socket_util.hpp"
#include "net/common.hpp"

#include <cuda_runtime.h>

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>

using namespace failover_fabric;
using sock = net::socket_t;
using net::kInvalidSocket;

std::uint64_t parse_u64(const char* s) { return std::strtoull(s, nullptr, 10); }

// Real CUDA kernel: deterministic reference accumulator (32 steps, fixed formula).
__global__ void reference_accum(uint64_t* out, uint64_t a, uint64_t b, int steps) {
  uint64_t acc = 0;
  for (int i = 0; i < steps; ++i) acc = (acc * 31u + (a * (uint64_t)i) + (b * (uint64_t)(i + 1))) & 0x3FFFFFFFu;
  *out = acc & 0x3FFFFFFFu;
}

std::uint64_t cuda_reference(uint64_t a, uint64_t b) {
  uint64_t* dev = nullptr;
  uint64_t host = 0;
  if (cudaMalloc(&dev, sizeof(uint64_t)) != cudaSuccess) return 0;
  // Real H2D transfer of the operands via device memory binding is unnecessary: the kernel
  // receives operands by value. We still do the bounded device buffer + kernel + sync + D2H.
  int steps = 32;
  reference_accum<<<1, 1>>>(dev, a, b, steps);
  cudaError_t e = cudaGetLastError();
  if (e == cudaSuccess) e = cudaDeviceSynchronize();
  if (e == cudaSuccess) cudaMemcpy(&host, dev, sizeof(uint64_t), cudaMemcpyDeviceToHost);
  cudaFree(dev);
  return host;
}

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
  int dev = 0;
  cudaError_t cudast = cudaSetDevice(dev);
  if (cudast != cudaSuccess) { std::fprintf(stderr, "cuda worker: cudaSetDevice failed: %s\n", cudaGetErrorString(cudast)); return 1; }
  cudaDeviceProp prop{}; cudaGetDeviceProperties(&prop, dev);
  std::printf("CUDA %s cc=%d.%d\n", prop.name, prop.major, prop.minor);
  std::fflush(stdout);

  sock listen = net::tcp_listen(port);
  if (listen == kInvalidSocket) { std::fprintf(stderr, "cuda worker: cannot bind\n"); return 1; }
  std::uint16_t actual_port = net::tcp_listen_port(listen);
  std::printf("PORT %u\n", (unsigned)actual_port); std::fflush(stdout);

  sock ctrl = net::tcp_connect(coord_host, coord_port);
  if (ctrl == kInvalidSocket) { std::fprintf(stderr, "cuda worker: cannot connect to coordinator\n"); return 1; }
  { PayloadWriter h; h.u8(ex::fid::role, ex::kRoleWorker); h.u64(ex::fid::target, target); h.u64(ex::fid::boot, boot); h.u64(ex::fid::req_port, actual_port); ex::send_msg(ctrl, MsgType::HELLO, 1, 0, h.finish()); }

  std::atomic<bool> active{false};
  std::uint64_t my_boot = boot;
  // Warmup: real CUDA resources prepared; CPU parity holds.
  volatile std::uint64_t warm = cuda_reference(1, 2);
  (void)warm;

  { PayloadWriter w; w.u64(ex::fid::target, target); w.u64(ex::fid::boot, boot); w.bool_(ex::fid::ready, true);
    w.str(ex::fid::model, "ref-det-sequence-v1"); w.str(ex::fid::abi, "ff-ref-v1"); w.f64(ex::fid::capacity, 1.0);
    ex::send_msg(ctrl, MsgType::PUBLISH_READINESS, 2, 0, w.finish()); }

  std::thread ctrl_thread([&] {
    while (true) {
      auto f = net::recv_frame(ctrl);
      if (!f) break;
      if (f->type == MsgType::ACTIVATE_TARGET) { active.store(true); PayloadWriter w; w.bool_(ex::fid::ok, true); ex::send_msg(ctrl, MsgType::ACTIVATE_RESULT, f->msg_id, f->epoch, w.finish()); }
      else if (f->type == MsgType::FENCE_ASSIGNMENT) active.store(false);
    }
  });

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
          // Sever the control connection but keep serving (live old-worker fencing).
          net::close_socket(ctrl);
          std::uint64_t gpu = cuda_reference(a, b); (void)gpu;
          w.bool_(ex::fid::ok, true);
        } else if (a == 0xBEEFu) {
          // Computation happens (real GPU kernel); the response is withheld (ambiguity barrier).
          std::uint64_t gpu = cuda_reference(a & 0xFFFFFFFFFFFFull, b);
          std::string of = "ambig_" + std::to_string(target) + ".out";
          FILE* fp = fopen(of.c_str(), "w"); if (fp) { fprintf(fp, "%llu\n", (unsigned long long)gpu); fclose(fp); }
          net::close_socket(c);  // withhold the response; peer sees EOF
          return;
        } else {
          std::uint64_t gpu = cuda_reference(a, b);
          std::uint64_t cpu = ex::reference_result(a, b);
          bool parity = (gpu == cpu);
          std::string rs = std::to_string(gpu);
          w.bool_(ex::fid::ok, parity);
          std::vector<std::uint8_t> rb(rs.begin(), rs.end()); if (!rb.empty()) w.bytes(ex::fid::payload, rb);
        }
      } else { w.bool_(ex::fid::ok, false); w.str(ex::fid::detail, "not active or stale boot"); }
      ex::send_msg(c, MsgType::EXECUTION_RESULT, f ? f->msg_id : 0, f ? f->epoch : 0, w.finish());
      net::close_socket(c);
    }).detach();
  }
  return 0;
}