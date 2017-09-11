#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class LockConfig : public BaseConfig<phxqueue::config::LockConfig> {
  public:
    LockConfig() : BaseConfig<phxqueue::config::LockConfig>() {}
    virtual ~LockConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

