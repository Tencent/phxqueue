/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/consumer/consumer.h"


namespace phxqueue {

namespace consumer {

using namespace std;

class HeartBeatLock {
  public:
    HeartBeatLock();
    virtual ~HeartBeatLock();

    comm::RetCode Init(Consumer *consumer, const int shmkey, const std::string &lockpath, const int nproc);
    void RunSync();
    comm::RetCode Lock(const int vpid, int &consumer_group_id, int &store_id, int &queue_id);
    comm::RetCode GetQueuesDistribute(std::vector<Queue_t> &queues);

  protected:
    comm::RetCode Sync();
    void ClearInvalidConsumerGroupIDs(const std::set<int> &valid_consumer_group_ids);
    void DistubePendingQueues(const std::map<int, std::vector<Queue_t>> &consumer_group_id2pending_queues);
    comm::RetCode GetAddrScale(ConsumerGroupID2AddrScales &consumer_group_id2addr_scales);
    comm::RetCode GetAllQueues(const int consumer_group_id, std::vector<Queue_t> &all_queues);
    void GetQueuesWithFilter(const int pub_id, const int store_id, const int consumer_group_id, std::vector<Queue_t> &all_queues, const ::google::protobuf::RepeatedField< ::google::protobuf::int32 >* queue_info_ids, std::shared_ptr<const config::TopicConfig> topic_config, set<pair<int, int> > &filter);
    comm::RetCode GetPendingQueues(const std::vector<Queue_t> &all_queues, const AddrScales &addr_scales, std::vector<Queue_t> &pending_queues);
    comm::RetCode DoLock(const int vpid, Queue_t *const info);
    void UpdateProcUsed();

  private:
    class HeartBeatLockImpl;
    std::unique_ptr<HeartBeatLockImpl> impl_;
};


}  // namespace consumer

}  // namespace phxqueue

