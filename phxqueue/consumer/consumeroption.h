#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace consumer {


class ConsumerOption {
  public:
    ConsumerOption() {}
    virtual ~ConsumerOption() {}

    std::string topic = "";

    std::string ip = "";
    int port = 0;
    int paxos_port = 0;

    int nprocs = 1;
    int nhandler = 100;
    int nbatch_handler = 1;
    //int nshare_stack = 100;
    int share_stack_size_kb = 128;

    int shm_key_base = 14335;
    std::string proc_pid_path = "";
    std::string lock_path_base = "";

    int use_store_master_client_on_get = 0;
    int use_store_master_client_on_add = 0;

    comm::LogFunc log_func = nullptr;
    plugin::ConfigFactoryCreateFunc config_factory_create_func = nullptr;
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func = nullptr;
};


}  // namespace consumer

}  // namesapce phxqueue

