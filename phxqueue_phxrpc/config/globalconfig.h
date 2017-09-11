#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class GlobalConfig : public BaseConfig<phxqueue::config::GlobalConfig> {
  public:
    GlobalConfig() : BaseConfig<phxqueue::config::GlobalConfig>() {}
    virtual ~GlobalConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

