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
#include "phxqueue/config/proto/topicconfig.pb.h"


namespace phxqueue {

namespace config {


class TopicConfig : public BaseConfig<proto::TopicConfig> {
  public:
    TopicConfig();

    virtual ~TopicConfig() override;

    // pub
    comm::RetCode GetAllPub(std::vector<std::shared_ptr<const proto::Pub>> &pubs) const;

    comm::RetCode GetAllPubID(std::set<int> &pub_ids) const;

    bool IsValidPubID(const int pub_id) const;

    comm::RetCode GetPubByPubID(const int pub_id, std::shared_ptr<const proto::Pub> &pub) const;

    comm::RetCode GetConsumerGroupIDsByPubID(const int pub_id, std::set<int> &consumer_group_ids) const;

    // consumer_group
    comm::RetCode GetAllConsumerGroup(std::vector<std::shared_ptr<const proto::ConsumerGroup>> &consumer_groups) const;

    comm::RetCode GetAllConsumerGroupID(std::set<int> &consumer_group_ids) const;

    bool IsValidConsumerGroupID(const int consumer_group_id) const;

    comm::RetCode GetConsumerGroupByConsumerGroupID(const int consumer_group_id, std::shared_ptr<const proto::ConsumerGroup> &consumer_group) const;

    // queue info

    comm::RetCode GetQueueInfoIDRankByPub(const int queue_info_id, const proto::Pub *pub, uint64_t &rank) const;

    comm::RetCode GetQueueInfoIDRankByPubID(const int queue_info_id, const int pub_id, uint64_t &rank) const;

    bool IsValidQueue(const int queue, const int pub_id = -1, const int consumer_group_id = -1) const;

    comm::RetCode GetQueuesByQueueInfoID(const int queue_info_id, std::set<int> &queues) const;

    comm::RetCode GetQueueDelay(const int queue, int &delay) const;

    comm::RetCode GetNQueue(const int queue_info_id, int &nqueue) const;

    comm::RetCode GetQueueByRank(const int queue_info_id, const uint64_t rank, int &queue) const;

    comm::RetCode GetQueueByLoopRank(const int queue_info_id, const uint64_t rank, int &queue) const;

    comm::RetCode GetQueueInfoByQueue(const int queue, std::shared_ptr<const proto::QueueInfo> &queue_info) const;

    comm::RetCode GetQueueInfoByQueueInfoID(const int queue_info_id, std::shared_ptr<const proto::QueueInfo> &queue_info) const;

    bool ShouldSkip(const comm::proto::QItem &item, const int consumer_group_id, const int queue_info_id) const;

    comm::RetCode GetQueueInfoIDByCount(const int pub_id, const int cnt, int &queue_info_id) const;

    comm::RetCode GetHandleIDRank(const int handle_id, int &rank) const;

    // freq info
    comm::RetCode GetAllFreqInfo(std::vector<std::shared_ptr<proto::FreqInfo> > &freq_infos) const;

    // replay info
    comm::RetCode GetAllReplayInfo(std::vector<std::unique_ptr<proto::ReplayInfo> > &replay_infos) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::TopicConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class TopicConfigImpl;
    std::unique_ptr<TopicConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

