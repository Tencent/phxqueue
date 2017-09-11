#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class SchedulerConfig : public BaseConfig<phxqueue::config::SchedulerConfig> {
  public:
    SchedulerConfig() : BaseConfig<phxqueue::config::SchedulerConfig>() {}
    virtual ~SchedulerConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

