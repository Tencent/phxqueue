#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/consumerconfig.pb.h"


namespace phxqueue {

namespace config {


class ConsumerConfig : public BaseConfig<proto::ConsumerConfig> {
  public:
    ConsumerConfig();

    virtual ~ConsumerConfig();

    comm::RetCode GetAllConsumer(std::vector<std::shared_ptr<const proto::Consumer>> &consumers) const;

    comm::RetCode GetConsumerByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Consumer> &consumer) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::ConsumerConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class ConsumerConfigImpl;
    std::unique_ptr<ConsumerConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

