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

namespace store {


class StoreOption {
  public:
    StoreOption() = default;
    virtual ~StoreOption() = default;

    std::string topic;

    std::string data_dir_path;

    std::string ip;
    int port{0};
    int paxos_port{0};

    int ngroup{100};
    int nconsumer_group{64};
    int nqueue{2000};

    int npaxos_iothread{3};

    comm::LogFunc log_func{nullptr};
    plugin::ConfigFactoryCreateFunc config_factory_create_func{nullptr};
    plugin::BreakPointFactoryCreateFunc break_point_factory_create_func{nullptr};
};


}  // namespace store

}  // namespace phxqueue


