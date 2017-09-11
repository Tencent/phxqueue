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
    virtual comm::RetCode GetQueueID(int &queue_id) = 0;
};

class QueueSelectorDefault : public QueueSelector {
  public:
    QueueSelectorDefault(const int topic_id, const int pub_id, const uint64_t uin, const int count = 0, const bool retry_switch_queue = false);
    virtual ~QueueSelectorDefault();

    virtual comm::RetCode GetQueueID(int &queue_id);

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

  private:
    class StoreSelectorDefaultImpl;
    std::unique_ptr<StoreSelectorDefaultImpl> impl_;
};


}  // namespace producer

}  // namespace phxqueue

