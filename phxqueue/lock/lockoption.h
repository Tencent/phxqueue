#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace lock {


class LockOption {
  public:
    LockOption() {}
    virtual ~LockOption() {}

    std::string topic;
    std::string data_dir_path;

    std::string ip;
    int port{0};
    int paxos_port{0};
    int nr_group{100};

    int clean_interval_s{1};
    int idle_write_checkpoint_interval_s{60};
    int max_clean_lock_num{100};
    int checkpoint_interval{100};
    bool no_leveldb{false};
    bool no_clean_thread{false};

    comm::LogFunc log_func{nullptr};
    plugin::ConfigFactoryCreateFunc config_factory_create_func{nullptr};
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func{nullptr};
};


}  // namespace lock

}  // namespace queue

