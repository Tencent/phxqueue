/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstdint>
#include <memory>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace producer {


class QueueSelector {
  public:
    QueueSelector() {}
    virtual ~QueueSelector() {}
    virtual comm::RetCode GetQueueID(const comm::proto::QueueType queue_type, int &queue_id) = 0;
};

class QueueSelectorDefault : public QueueSelector {
  public:
    QueueSelectorDefault(const int topic_id, const int pub_id, const uint64_t uin, const int count = 0, const bool retry_switch_queue = false);
    virtual ~QueueSelectorDefault();

    virtual comm::RetCode GetQueueID(const comm::proto::QueueType queue_type, int &queue_id);

  private:
    class QueueSelectorDefaultImpl;
    std::unique_ptr<QueueSelectorDefaultImpl> impl_;
};

class StoreSelector {
  public:
    StoreSelector() {}
    virtual ~StoreSelector() {}
    virtual comm::RetCode GetStoreID(int &store_id) = 0;
};

class StoreSelectorDefault : public StoreSelector {
  public:
    StoreSelectorDefault(const int topic_id, const int pub_id, const uint64_t uin,
                         const bool retry_switch_store = false);
    virtual ~StoreSelectorDefault();

    virtual comm::RetCode GetStoreID(int &store_id);

  protected:
    virtual bool IsStoreBlocked(const int store_id) {return false;}

    int GetTopicID();
    int GetPubID();
    uint64_t GetUin();
    bool IsRetrySwitchStore();

  private:
    class StoreSelectorDefaultImpl;
    std::unique_ptr<StoreSelectorDefaultImpl> impl_;
};


}  // namespace producer

}  // namespace phxqueue

