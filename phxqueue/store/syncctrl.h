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
    comm::RetCode AdjustNextCursorID(const int sub_id, const int queue_id,
                                     uint64_t &prev_cursor_id, uint64_t &next_cursor_id);
    comm::RetCode UpdateCursorID(const int sub_id, const int queue_id,
                                 const uint64_t cursor_id, const bool is_prev = true);
    comm::RetCode GetCursorID(const int sub_id, const int queue_id,
                              uint64_t &cursor_id, const bool is_prev = true) const;
    void ClearSyncCtrl();
    comm::RetCode GetBackLogByCursorID(const int queue_id, const uint64_t cursor_id, int &backlog);
    comm::RetCode SyncCursorID(const proto::SyncCtrlInfo &sync_ctrl_info);

  private:
    comm::RetCode ClearCursorID(const int sub_id, const int queue_id);
    comm::RetCode Flush(const int sub_id, const int queue_id);

    class SyncCtrlImpl;
    std::unique_ptr<SyncCtrlImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

