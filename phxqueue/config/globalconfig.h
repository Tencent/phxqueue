/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cassert>
#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/consumerconfig.h"
#include "phxqueue/config/lockconfig.h"
#include "phxqueue/config/proto/globalconfig.pb.h"
#include "phxqueue/config/schedulerconfig.h"
#include "phxqueue/config/storeconfig.h"
#include "phxqueue/config/topicconfig.h"


namespace phxqueue {

namespace config {

using namespace std;


class GlobalConfig : public BaseConfig<proto::GlobalConfig>{
  public:
    GlobalConfig();

    virtual ~GlobalConfig();

    static GlobalConfig *GetThreadInstance();

    comm::RetCode GetTopicIDByTopicName(const std::string &topic_name, int &topic_id) const;
    comm::RetCode GetTopicNameByTopicID(const int topic_id, std::string &topic_name) const;

    comm::RetCode GetTopicConfigByTopicID(const int topic_id, std::shared_ptr<const TopicConfig> &topic_config);

    comm::RetCode GetAllTopicConfig(std::vector<std::shared_ptr<const TopicConfig> > &topic_confs) const;

    comm::RetCode GetAllTopicID(std::set<int> &topic_ids) const;

    comm::RetCode GetTopicIDByHandleID(const int handle_id, int &topic_id) const;

    comm::RetCode GetConsumerConfig(const int topic_id, std::shared_ptr<const ConsumerConfig> &consumer_config);

    comm::RetCode GetStoreConfig(const int topic_id, std::shared_ptr<const StoreConfig> &store_config);

    comm::RetCode GetSchedulerConfig(const int topic_id, std::shared_ptr<const SchedulerConfig> &scheduler_config);

    comm::RetCode GetLockConfig(const int topic_id, std::shared_ptr<const LockConfig> &lock_config);

    uint64_t GetLastModTime(const int topic_id);

  protected:
    virtual comm::RetCode ReadConfig(proto::GlobalConfig &proto);

    comm::RetCode Rebuild() override;

  protected:
    struct GlobalConfigImpl_t;
    std::unique_ptr<struct GlobalConfigImpl_t> impl_;
};


}  // namespace config

}  // namespace phxqueue

