#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class ConsumerConfig : public BaseConfig<phxqueue::config::ConsumerConfig> {
  public:
    ConsumerConfig() : BaseConfig<phxqueue::config::ConsumerConfig>() {}
    virtual ~ConsumerConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

