/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class StoreMetaQueue;

class BaseMgr {
  public:
    BaseMgr(Store *store);
    virtual ~BaseMgr();

    comm::RetCode Init();
    comm::RetCode Add(const uint64_t cursor_id, const comm::proto::AddRequest &req);
    bool NeedSkipAdd(const uint64_t cursor_id, const comm::proto::AddRequest &req);
    comm::RetCode Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp);
    comm::RetCode GetItemsByCursorID(const int queue_id, const uint64_t cursor_id,
                                     std::vector<comm::proto::QItem> &items);
    StoreMetaQueue *GetMetaQueue(const int queue_id);

  private:
    class BaseMgrImpl;
    std::unique_ptr<BaseMgrImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

