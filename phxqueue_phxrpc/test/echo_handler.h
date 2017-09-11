#pragma once

#include <memory>

#include "phxqueue/comm.h"


namespace phxqueue_phxrpc {

namespace test {


class EchoHandler : public phxqueue::comm::Handler {
  public:
    EchoHandler() {}
    virtual ~EchoHandler() override = default;

    virtual phxqueue::comm::HandleResult Handle(const phxqueue::comm::proto::ConsumerContext &cc,
                                                phxqueue::comm::proto::QItem &item,
                                                std::string &uncompressed_buffer) override;
};


}  // namespace test

}  // namespace phxqueue_phxrpc

