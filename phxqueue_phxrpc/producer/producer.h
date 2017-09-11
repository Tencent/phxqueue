#pragma once

#include <memory>

#include "phxqueue/producer.h"


namespace phxqueue_phxrpc {

namespace producer {


class Producer : public phxqueue::producer::Producer {
  public:
    Producer(const phxqueue::producer::ProducerOption &opt);
    virtual ~Producer();

    virtual void CompressBuffer(const std::string &buffer, std::string &compressed_buffer,
                                int &buffer_type);

  protected:
    virtual phxqueue::comm::RetCode Add(const phxqueue::comm::proto::AddRequest &req,
                                        phxqueue::comm::proto::AddResponse &resp);
};


}  // namespace producer

}  // namespace phxqueue_phxrpc

