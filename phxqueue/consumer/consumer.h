/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <queue>
#include <set>
#include <vector>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/store.h"

#include "phxqueue/consumer/consumeroption.h"


namespace phxqueue {

namespace consumer {


#pragma pack(1)

struct Queue_t {
    uint32_t magic;

    // 共享内存中缓存队列信息, 重启后不丢失
    uint32_t consumer_group_id;
    uint32_t pub_id;
    uint32_t store_id;
    uint32_t queue_id;
};

struct QueueBuf_t {
    uint32_t magic;
    Queue_t queues[0];
};

#pragma pack()


using AddrScales = std::vector<comm::proto::AddrScale>;
using ConsumerGroupID2AddrScales = std::map<int, AddrScales>;


class Consumer : public comm::MultiProc {
  public:
    Consumer(const ConsumerOption &opt);
    virtual ~Consumer() override;

    // ------------------------ Interfaces generally used in main ------------------------
    // Usage please refer to phxqueue_phxrpc/test/consumer_main.cpp

    // Specify a HandlerFactory for a particular handle id.
    // Implement of Handler please refer to phxqueue/test/simplehandler.h
    void AddHandlerFactory(const int handle_id, comm::HandlerFactory * const hf);

    // Consumer start.
    // Make sure this is the last operation in main().
    void Run();


    // ------------------------ Interfaces MUST be overrided ------------------------
    // Implement please refer to phxqueue_phxrpc/consumer/consumer.cpp

    // Get items from Store, as dequeue operation.
    // Need to implement an RPC that corresponds to Store::Get().
    virtual comm::RetCode Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp) = 0;

    // Add items to Store.
    // Handling failed items requires retry.
    // Need to implement an RPC that corresponds to Store::Add().
    virtual comm::RetCode Add(comm::proto::AddRequest &req, comm::proto::AddResponse &resp) = 0;

    // Consumer reports its own machine load to the Scheduler, which, after statistics, returns the dynamic weight to the Consumer.
    // Depending on the dynamic weight, each Consumer uses a same algorithm to calculate which queues should be handled by themselves.
    // Need to implement an RPC that corresponds to Scheduler::GetAddrScale().
    virtual comm::RetCode GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                                       comm::proto::GetAddrScaleResponse &resp) = 0;

    // According to specific lock key, get lock infomation from Lock.
    // Such as version, lease time, who is holding the lock. Refer to LockInfo in phxqueue/comm/proto/comm.proto
    // Need to implement an RPC that corresponds to Lock::GetLockInfo().
    virtual comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                      comm::proto::GetLockInfoResponse &resp) = 0;

    // According to the lock information returned by GetLockInfo, after a series of distributed lock logic, decide whether to lease or renew this lock by AcquireLock.
    // Need to implement an RPC that corresponds to Lock::AcquireLock().
    virtual comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                                      comm::proto::AcquireLockResponse &resp) = 0;

    // ------------------------ Interfaces CAN be overrided ------------------------

  protected:
    // Consumer will fork out multi child process as workers, to pull items from Store and process. The worker number designated by ConsumerOption::nprocs.

    // Callback before worker fork.
    virtual void BeforeFork() {}

    // Callback after worker fork.
    virtual void OnChildRun(const int vpid) {}

    // Consumer will fork one sync process, to do things belows:
    // 1. Report the local CPU load to Scheduler.
    // 2. Get the dynamic weight of all the Consumer from Scheduler.
    // 3. Establish consistent hash according to the dynamic weight.
    // 4. Calculate which queue is handled by itself according to the consistent hash.

    // Callback after the sync process fork.
    virtual void OnRunSync() {}

    // Each worker process has two threads,
    // one is call pull thread, for pulling items from the store,
    // another is call consume thread, for consumption items mentioned above.

    // Callback after the consume thread start.
    virtual void OnConsumeThreadRun(const int vpid) {}

    // The processing flow of pull thread is as follows:
    // 1. Identify which queue to be handled by self.
    // 2. Lease/renew corresponding lock of the queue from Lock.
    // 3. Pull item froms Store within the lease time.
    // 4. Deliver items to consume thread.
    // 5. Repeate step 3-4, and do step 1-2 periodically.

    // Callback before the pull thread start lock process.
    virtual void BeforeLock(const comm::proto::ConsumerContext &cc) {}

    // Callback after the pull thread finished lock process.
    virtual void AfterLock(const comm::proto::ConsumerContext &cc) {}

    // Custom pre_cursor_id/next_cursor_id/limit in comm::GetRequest before Get.
    virtual void CustomGetRequest(const comm::proto::ConsumerContext &cc,
                                  const comm::proto::GetRequest &req,
                                  uint64_t &pre_cursor_id, uint64_t &next_cursor_id, int &limit);

    // Callback before the pull thread start get items from Store.
    virtual void BeforeGet(const comm::proto::GetRequest &req) {}

    // Callback after the pull thread finished get items from Store.
    virtual void AfterGet(const comm::proto::GetRequest &req,
                          const comm::proto::GetResponse &resp) {}

    // Callback before the pull thread start add items to Store.
    virtual void BeforeAdd(const comm::proto::AddRequest &req) {}

    // Callback after the pull thread finished add items to Store.
    virtual void AfterAdd(const comm::proto::AddRequest &req,
                          const comm::proto::AddResponse &resp) {}

    // Callback after the consume thread finished consume.
    // e.g. For add items to retry queue.
    virtual void AfterConsume(const comm::proto::ConsumerContext &cc,
                              const std::vector<std::shared_ptr<comm::proto::QItem> > &items,
                              const std::vector<comm::HandleResult> &handle_results);

    // In a consume process, there are multiple consumer_group processes concurrently, each corresponding to a worker routine:
    // 1. numbers of hanlde process, specified by ConsumerOption::nhandler, each handle items with same uin.
    // 2. numbers of batch handle process, specified by ConsumerOption::nbatch_handler, each handle all items from last pull.

    // Callback before the worker routine start handle.
    virtual void BeforeHandle(const comm::proto::ConsumerContext &cc,
                              const comm::proto::QItem &item) {}

    // Callback after the worker routine finished handle.
    virtual void AfterHandle(const comm::proto::ConsumerContext &cc,
                             const comm::proto::QItem &item,
                             const comm::HandleResult &handle_result) {}

    // Callback before the worker routine start batch handle.
    virtual void BeforeBatchHandle(const comm::proto::ConsumerContext &cc,
                                   const std::vector<std::shared_ptr<comm::proto::QItem>> &items,
                                   const int idx) {}


    // Batch handle implement.
    // arg idx within [0, ConsumerOption::nbatch_handler).
    virtual comm::RetCode BatchHandle(const comm::proto::ConsumerContext &cc,
                                const std::vector<std::shared_ptr<comm::proto::QItem>> &items,
                                const int idx) {
        return comm::RetCode::RET_OK;
    }

    // Callback after the worker routine finish batch handle.
    virtual void AfterBatchHandle(const comm::proto::ConsumerContext &cc,
                                  const std::vector<std::shared_ptr<comm::proto::QItem>> &items,
                                  const int idx) {}


    // Implement to uncompress buffer in item.
    // refer to QItem::buffer in phxqueue/comm/proto/comm.proto.
    virtual comm::RetCode UncompressBuffer(const std::string &buffer, const int buffer_type,
                                           std::string &uncompressed_buffer) = 0;

    // Implement to compress buffer to item, same with Producer::CompressBuffer.
    // refer to QItem::buffer in phxqueue/comm/proto/comm.proto.
    virtual void CompressBuffer(const std::string &buffer, std::string &compressed_buffer,
                                const int buffer_type) = 0;

    // Implement to restore user cookies.
    virtual void RestoreUserCookies(const comm::proto::Cookies &user_cookies) = 0;


    // Pull thread establishs consistent hash according to the dynamic weight, and calculate which queue is handled by itself according to the consistent hash.
    virtual comm::RetCode GetQueueByAddrScale(const std::vector<Queue_t> &queues,
                                              const AddrScales &addr_scales,
                                              std::set<size_t> &queue_idxs);

    // Deliver items from pull thread to consume thread.
    virtual comm::RetCode Consume(const comm::proto::ConsumerContext &cc,
                                  const std::vector<std::shared_ptr<comm::proto::QItem>> &items,
                                  std::vector<comm::HandleResult> &handle_results);

    // Implement to skip handle some items.
    virtual bool SkipHandle(const comm::proto::ConsumerContext &cc, const comm::proto::QItem &item);

    // ------------------------ Interfaces used internally ------------------------

  public:
    // Dispatch items to each worker routine, and resume processes of handle and batch handle.
    void TaskDispatch();

    bool HasHandleTasks(const int cid);

    bool HasBatchHandleTasks(const int cid);

    void ProcessAllHandleTasks(const int cid);

    void ProcessAllBatchHandleTasks(const int cid);

    // Entrance of handle process.
    void HandleRoutineFunc(const int idx);

    // Entrance of batch handle process.
    void BatchHandleRoutineFunc(const int idx);

    // Called while all processes of handle done.
    void HandleTaskFinish();

    // Called while all processes of batch handle done.
    void BatchHandleTaskFinish();

  protected:
    // Return ConsumerOption.
    const ConsumerOption * GetConsumerOption() const;

    // Return topic id.
    int GetTopicID();

    // Return the handle relationship between queues and worker processes.
    comm::RetCode GetQueuesDistribute(std::vector<Queue_t> &queues);

    // Judge if all processes of handle and batch handle done.
    bool IsAllTaskFinish();

    // Return handler by specific handle_id from comm::HandlerFactory.
    std::unique_ptr<comm::Handler> GetHandler(const int handle_id);

    // Implement of handle process.
    comm::RetCode Handle(const comm::proto::ConsumerContext &cc,
                         comm::proto::QItem &item, comm::HandleResult &handle_result);

    // Entrance of worker process.
    void ChildRun(const int vpid);

    // Make request of Store::Get.
    void MakeGetRequest(const comm::proto::ConsumerContext &cc,
                        const config::proto::QueueInfo &queue_info, const int limit,
                        comm::proto::GetRequest &req);

    // Update consumer context by response of Store::Get, including the cursor of next Store::Get.
    void UpdateConsumerContextByGetResponse(const comm::proto::GetResponse &resp,
                                            comm::proto::ConsumerContext &cc);

    // Step 2-4 memtioned above in the process of pull thread.
    comm::RetCode Process(comm::proto::ConsumerContext &cc);

    // Worker process suicide when process count more than consumer_max_loop_per_proc, refer to phxqueue/config/proto/topicconfig.proto.
    void CheckMaxLoop(const int vpid);

    // Worker process suicide when rss used more than consumer_max_mem_size_mb_per_proc, refer to phxqueue/config/proto/topicconfig.proto.
    void CheckMem(const int vpid);

    // Items with same uin must categorized into a bucket for a worker routine.
    comm::RetCode MakeHandleBuckets();

    // Start consume thread.
    void ConsumeThreadRun(const int vpid);

  private:
    class ConsumerImpl;
    std::unique_ptr<ConsumerImpl> impl_;

    friend class FreqMan;
    friend class HeartBeatLock;
};


}  // namespace consumer

}  // namespace phxqueue

