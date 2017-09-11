#pragma once

#include <memory>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace test {


class SimpleHandler : public comm::Handler {
  public:
    SimpleHandler() {}
    virtual ~SimpleHandler() override = default;

    virtual comm::HandleResult Handle(const comm::proto::ConsumerContext &cc,
                                      comm::proto::QItem &item,
                                      std::string &uncompressed_buffer) override;
};


}  // namespace test

}  // namespace phxqueue

