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

