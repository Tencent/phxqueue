/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace consumer {


class ConsumerOption {
  public:
    ConsumerOption() = default;
    virtual ~ConsumerOption() = default;

    std::string topic;

    std::string ip;
    int port{0};
    int paxos_port{0};

    int nprocs{1};
    int nhandler{100};
    int nbatch_handler{1};
    //int nshare_stack{100};
    int share_stack_size_kb{128};

    int shm_key_base{14335};
    std::string proc_pid_path;
    std::string lock_path_base;

    int use_store_master_client_on_get{0};
    int use_store_master_client_on_add{0};

    comm::LogFunc log_func{nullptr};
    plugin::ConfigFactoryCreateFunc config_factory_create_func{nullptr};
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func{nullptr};
};


}  // namespace consumer

}  // namesapce phxqueue

