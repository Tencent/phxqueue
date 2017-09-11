#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"
#include "phxqueue/producer/producer.h"

namespace phxqueue {

namespace producer {

class ProcessCtx_t;

class BatchHelper {
  public:
    BatchHelper(producer::Producer *producer);
    ~BatchHelper();

    void Init();
    void Run();
    void Stop();

    comm::RetCode BatchRawAdd(const comm::proto::AddRequest &req);

    void DispatchBatchTask(int vtid);
    void Process(ProcessCtx_t *ctx);

  protected:
    void DaemonThreadRun(int vtid);

  private:
    class BatchHelperImpl;
    std::unique_ptr<BatchHelperImpl> impl_;
};

}  // namespace producer

}  // namespace phxqueue

