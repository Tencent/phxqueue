/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>

#include "phxqueue/comm.h"

#include "phxqueue/store/proto/store.pb.h"
#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class SyncCtrl {
  public:
    SyncCtrl(Store *const store);
    virtual ~SyncCtrl();

    comm::RetCode Init();
    comm::RetCode AdjustNextCursorID(const int consumer_group_id, const int queue_id,
                                     uint64_t &prev_cursor_id, uint64_t &next_cursor_id);
    comm::RetCode UpdateCursorID(const int consumer_group_id, const int queue_id,
                                 const uint64_t cursor_id, const bool is_prev = true);
    comm::RetCode GetCursorID(const int consumer_group_id, const int queue_id,
                              uint64_t &cursor_id, const bool is_prev = true) const;
    void ClearSyncCtrl();
    comm::RetCode GetBackLogByCursorID(const int queue_id, const uint64_t cursor_id, int &backlog);
    comm::RetCode SyncCursorID(const proto::SyncCtrlInfo &sync_ctrl_info);

  private:
    comm::RetCode ClearCursorID(const int consumer_group_id, const int queue_id);
    comm::RetCode Flush(const int consumer_group_id, const int queue_id);

    class SyncCtrlImpl;
    std::unique_ptr<SyncCtrlImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

