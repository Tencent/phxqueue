#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class TopicConfig : public BaseConfig<phxqueue::config::TopicConfig> {
  public:
    TopicConfig() : BaseConfig<phxqueue::config::TopicConfig>() {}
    virtual ~TopicConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

