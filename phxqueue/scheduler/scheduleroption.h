#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace scheduler {


class SchedulerOption {
  public:
    SchedulerOption() {}
    virtual ~SchedulerOption() {}

    std::string topic;

    std::string ip;
    int port{0};

    comm::LogFunc log_func{nullptr};
    plugin::ConfigFactoryCreateFunc config_factory_create_func{nullptr};
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func{nullptr};
};


}  // namespace scheduler

}  // namespace phxqueue

