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
#include "phxqueue/config/proto/routeconfig.pb.h"


namespace phxqueue {

namespace config {


class RouteConfig : public BaseConfig<proto::RouteConfig> {
  public:
    RouteConfig();

    virtual ~RouteConfig();

    comm::RetCode GetAllRoute(std::vector<std::shared_ptr<const proto::Route>> &routes) const;

    comm::RetCode GetAddrByConsistentHash(const uint64_t key, comm::proto::Addr &addr) const;
	int GetConnTimeoutMs();

  protected:
    virtual comm::RetCode ReadConfig(proto::RouteConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class RouteConfigImpl;
    std::unique_ptr<RouteConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

