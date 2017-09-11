#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"

namespace phxqueue {

namespace producer {


class ProducerOption {
  public:
    ProducerOption() {}
    virtual ~ProducerOption() {}

    comm::LogFunc log_func = nullptr;
    plugin::ConfigFactoryCreateFunc config_factory_create_func = nullptr;
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func = nullptr;

    int ndaemon_batch_thread = 0;
    int nprocess_routine = 50;
    int process_routine_share_stack_size_kb = 128;
};


}  // namespace producer

}  // namespace phxqueue

