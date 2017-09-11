#pragma once

#include "phxqueue_phxrpc/config/baseconfig.h"
#include "phxqueue/config.h"


namespace phxqueue_phxrpc {

namespace config {


class StoreConfig : public BaseConfig<phxqueue::config::StoreConfig> {
  public:
    StoreConfig() : BaseConfig<phxqueue::config::StoreConfig>() {}
    virtual ~StoreConfig() {}
};


}  // namespace config

}  // namespace phxqueue_phxrpc

