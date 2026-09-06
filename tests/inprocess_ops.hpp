// inprocess_ops.hpp — an in-process CutoverOps adapter for tests. It models a gateway +
// worker pair that enforces generation-bound authority (fencing) at dispatch/result gates.
#pragma once
#include <failover_fabric/cutover.hpp>
#include <map>
#include <string>
#include <vector>

namespace ff_test {

struct InProcessOps : public failover_fabric::CutoverOps {
  bool force_fencing_unproven{false};
  bool force_activation_fail{false};
  bool force_route_fail{false};
  bool force_verify_fail{false};
  bool hold_verify{false};
  int calls_fence{0}, calls_activate{0}, calls_route{0}, calls_verify{0};
  std::string worker_transport;
  failover_fabric::GatewayBootId gateway_boot;

  InProcessOps(std::string transport) : worker_transport(std::move(transport)) {}

  failover_fabric::FenceOutcome fence_old(const failover_fabric::Assignment&,
                                          failover_fabric::WorkerBootId) override {
    ++calls_fence;
    failover_fabric::FenceOutcome fo;
    if (force_fencing_unproven) { fo.fencing_unproven = true; fo.applied = false; fo.state = failover_fabric::FenceState::UNPROVEN; }
    else { fo.applied = true; fo.state = failover_fabric::FenceState::ENFORCED; }
    fo.detail = "in-process fence";
    return fo;
  }
  failover_fabric::ActivateOutcome activate(const failover_fabric::Candidate&,
                                            const failover_fabric::WorkerAuthorization&,
                                            failover_fabric::ServiceAuthorityGeneration) override {
    ++calls_activate;
    failover_fabric::ActivateOutcome ao;
    if (force_activation_fail) ao.detail = "activation refused";
    else ao.acknowledged = true;
    return ao;
  }
  failover_fabric::RouteOutcome install_route(const failover_fabric::RouteEntry& e,
                                              failover_fabric::GatewayBootId) override {
    ++calls_route;
    failover_fabric::RouteOutcome ro;
    if (force_route_fail) ro.detail = "route refused";
    else { ro.acknowledged = true; ro.route = e.route; ro.generation = e.generation; }
    return ro;
  }
  failover_fabric::VerifyOutcome verify(const failover_fabric::RouteEntry&,
                                        const failover_fabric::WorkerAuthorization&) override {
    ++calls_verify;
    failover_fabric::VerifyOutcome vo;
    if (force_verify_fail) vo.detail = "verification refused";
    else { vo.verified = true; vo.result = "cpu-parity-ok"; }
    return vo;
  }
};

}  // namespace ff_test
