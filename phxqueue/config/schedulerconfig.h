/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/schedulerconfig.pb.h"


namespace phxqueue {

namespace config {


struct SchedulerConfigImpl_t;

class SchedulerConfig : public BaseConfig<proto::SchedulerConfig> {
  public:
    SchedulerConfig();

    virtual ~SchedulerConfig();

    comm::RetCode GetScheduler(std::shared_ptr<const proto::Scheduler> &scheduler) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::SchedulerConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class SchedulerConfigImpl;
    std::unique_ptr<SchedulerConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

