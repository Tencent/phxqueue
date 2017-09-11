#pragma once

#include <memory>

#include "phxqueue/producer.h"


namespace phxqueue {

namespace test {


class SimpleProducer : public producer::Producer {
  public:
    SimpleProducer(const producer::ProducerOption &opt) : producer::Producer(opt) {}
    virtual ~SimpleProducer() override = default;

    virtual void CompressBuffer(const std::string &buffer, std::string &compress_buffer,
                                int &buffer_type) override;

  protected:
    virtual comm::RetCode Add(const comm::proto::AddRequest &req,
                              comm::proto::AddResponse &resp) override;

};


}  // namespace test

}  // namespace phxqueue

