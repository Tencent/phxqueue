#pragma once

#include <memory>
#include <unistd.h>

#include "phxqueue/consumer/consumer.h"


namespace phxqueue {

namespace consumer {


class FreqMan {
  public:
    FreqMan();
    virtual ~FreqMan();
    comm::RetCode Init(const int topic_id, Consumer *consumer);
    void Run();
    void UpdateConsumeStat(const int vpid, const comm::proto::ConsumerContext &cc,
                           const std::vector<std::shared_ptr<comm::proto::QItem>> &items);
    void Judge(const int vpid, bool &need_block, bool &need_freqlimit,
               int &nhandle_per_get_recommand, int &sleep_ms_per_get_recommand);

  private:
    void ClearAllConsumeStat();
    comm::RetCode UpdateLimitInfo();

    class FreqManImpl;
    std::unique_ptr<FreqManImpl> impl_;
};


}  // namespace consumer

}  // namespace phxqueue

