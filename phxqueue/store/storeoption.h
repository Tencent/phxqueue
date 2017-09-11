#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace store {


class StoreOption {
  public:
    StoreOption() {}
    virtual ~StoreOption() {}

    std::string topic;

    std::string data_dir_path;

    std::string ip;
    int port{0};
    int paxos_port{0};

    int ngroup{100};
    int nsub{64};
    int nqueue{2000};

    int npaxos_iothread{3};

    comm::LogFunc log_func{nullptr};
    plugin::ConfigFactoryCreateFunc config_factory_create_func{nullptr};
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func{nullptr};
};


}  // namespace store

}  // namespace phxqueue


