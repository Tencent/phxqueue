/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/producer/batchhelper.h"
#include "phxqueue/producer/producer.h"

#include <cstdio>
#include <cinttypes>
#include <functional>
#include <zlib.h>
#include <stack>
#include <queue>
#include <map>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/store.h"

#include "co_routine.h"


namespace phxqueue {

namespace producer {


using namespace std;

class Task {
public:
    Task(const comm::proto::AddRequest *req) : req_(req) {}
    ~Task() {}

    void PrepareWait();
    comm::RetCode Wait();
    void Notify(comm::RetCode retcode);

    const comm::proto::AddRequest *GetReq();

private:
    const comm::proto::AddRequest *req_ = nullptr;
    unique_ptr<comm::Notifier> notifier_ = nullptr;
};

void Task::PrepareWait() {
    if (nullptr != notifier_) return;
    notifier_ = move(comm::NotifierPool::GetInstance()->Get());
}

comm::RetCode Task::Wait() {
    if (nullptr == notifier_) return comm::RetCode::RET_ERR_NOTIFIER_MISS;

    comm::RetCode retcode = comm::RetCode::RET_OK;
    notifier_->Wait(retcode);

    comm::NotifierPool::GetInstance()->Put(notifier_);

    return retcode;
}

void Task::Notify(comm::RetCode retcode) {
    if (nullptr == notifier_) return;
    notifier_->Notify(retcode);
}

const comm::proto::AddRequest *Task::GetReq() {
    return req_;
}


////////////////////////////////////////////////////////////////////////////////////

class BatchTask {
public:
    BatchTask(int topic_id, Producer *producer) : topic_id_(topic_id), producer_(producer) {}
    ~BatchTask() {}

    void Init();
    void AddTask(shared_ptr<Task> task);
    bool Ready(uint64_t *ready_timestamp_ms = nullptr);
    comm::RetCode Process(bool is_timeout = false);

private:
    int topic_id_;
    Producer *producer_ = nullptr;
    vector<shared_ptr<Task>> tasks_;

    uint32_t nbyte_ = 0;
    uint32_t nitems_ = 0;
    uint64_t start_timestamp_ms_ = 0;

    uint32_t items_byte_size_limit_ = 0;
    uint32_t batch_limit_ = 0;
    uint32_t producer_batch_delay_time_ms_ = 0;
};


void BatchTask::Init() {
    comm::RetCode ret;
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id_, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return;
    }

    items_byte_size_limit_ = topic_config->GetProto().topic().items_byte_size_limit();
    batch_limit_ = topic_config->GetProto().topic().batch_limit();
    producer_batch_delay_time_ms_ = topic_config->GetProto().topic().producer_batch_delay_time_ms();
}


void BatchTask::AddTask(shared_ptr<Task> task) {
    auto req = task->GetReq();
    if (nullptr == req) return;
    if (0 == start_timestamp_ms_) start_timestamp_ms_ = comm::utils::Time::GetSteadyClockMS();

    nbyte_ += req->ByteSize();
    nitems_ += req->items_size();

    tasks_.push_back(task);
}

bool BatchTask::Ready(uint64_t *ready_timestamp_ms) {
    if (ready_timestamp_ms) *ready_timestamp_ms = 0;

    if (0 == nitems_ || 0 == start_timestamp_ms_) return false;

    auto now_timestamp_ms = comm::utils::Time::GetSteadyClockMS();

    if (nbyte_ * 2 >= items_byte_size_limit_ ||
        nitems_ * 2 >= batch_limit_ ||
        start_timestamp_ms_ + producer_batch_delay_time_ms_ <= now_timestamp_ms) {
        return true;
    }
    if (ready_timestamp_ms) *ready_timestamp_ms = start_timestamp_ms_ + producer_batch_delay_time_ms_;

    return false;
}

comm::RetCode BatchTask::Process(bool is_timeout) {
    auto now_timestamp_ms = comm::utils::Time::GetSteadyClockMS();
    uint64_t time_wait_ms = now_timestamp_ms - start_timestamp_ms_;

    comm::proto::AddRequest batch_req;
    comm::proto::AddResponse batch_resp;

    for (auto &task : tasks_) {
        auto req = task->GetReq();
        if (nullptr == req) continue;
        if (0 == batch_req.items_size()) {
            batch_req.CopyFrom(*req);
        } else {
            for (int i{0}; i < req->items_size(); ++i) {
                batch_req.add_items()->CopyFrom(req->items(i));
            }
        }
    }

    auto retcode = producer_->RawAdd(batch_req, batch_resp);

    comm::ProducerBP::GetThreadInstance()->OnBatchStat(batch_req, retcode, time_wait_ms, is_timeout);
    //printf("batch %d time_wait_ms %" PRIu64 " is_timeout %d\n", batch_req.items_size(), time_wait_ms, is_timeout);

    for (auto &task : tasks_) {
        task->Notify(retcode);
    }
    return retcode;
}


////////////////////////////////////////////////////////////////////////////////////

struct ProcessCtx_t {
    stCoRoutine_t *co = nullptr;
    BatchHelper *batch_helper = nullptr;
    int vtid = -1;
    int pid = -1;
    unique_ptr<BatchTask> ready_batch_task = nullptr;
};

struct DispatchCtx_t {
    BatchHelper *batch_helper = nullptr;
    int vtid = -1;
};

using BatchInfo = pair<mutex, unique_ptr<BatchTask>>;

class BatchHelper::BatchHelperImpl {
public:
    BatchHelperImpl() {}
    ~BatchHelperImpl() {}

    Producer *producer = nullptr;

    int ndaemon_thread = 0;
    int nprocess_routine = 0;
    int process_routine_share_stack_size_kb = 0;

    unique_ptr<pthread_rwlock_t[]> vtid2rwlock;
    unique_ptr<map<tuple<int, int, int>, BatchInfo>[]> vtid2batch_infos;
    unique_ptr<stack<ProcessCtx_t*>[]> vtid2idle_process_ctxs;
    vector<unique_ptr<thread>> daemon_threads;
    bool stop = true;
};

BatchHelper::BatchHelper(Producer *producer) : impl_(new BatchHelperImpl) {
    impl_->producer = producer;
}

BatchHelper::~BatchHelper() {
    Stop();
}

void BatchHelper::Init() {
    auto opt = impl_->producer->GetProducerOption();

    impl_->ndaemon_thread = opt->ndaemon_batch_thread;
    impl_->nprocess_routine = opt->nprocess_routine;
    impl_->process_routine_share_stack_size_kb = opt->process_routine_share_stack_size_kb;

    impl_->vtid2rwlock.reset(new pthread_rwlock_t[impl_->ndaemon_thread]);
    for (int i{0}; i < impl_->ndaemon_thread; ++i) {
        pthread_rwlock_init(&impl_->vtid2rwlock[i], nullptr);
    }

    impl_->vtid2batch_infos.reset(new map<tuple<int, int, int>, BatchInfo>[impl_->ndaemon_thread]);
    impl_->vtid2idle_process_ctxs.reset(new stack<ProcessCtx_t*>[impl_->ndaemon_thread]);
}

void BatchHelper::Run() {
    impl_->stop = false;
    for (int i{0}; i < impl_->ndaemon_thread; ++i) {
        impl_->daemon_threads.push_back(move(unique_ptr<thread>(new thread(&BatchHelper::DaemonThreadRun, this, i))));
    }
}

void BatchHelper::Stop() {
    impl_->stop = true;
    for (auto &&t : impl_->daemon_threads) {
        t->join();
        t.release();
    }
}

static int DispatchTickRun(void *arg) {
    DispatchCtx_t *ctx = static_cast<DispatchCtx_t*>(arg);
    ctx->batch_helper->DispatchBatchTask(ctx->vtid);
    return 0;
}

void BatchHelper::DispatchBatchTask(int vtid) {
    auto &batch_infos = impl_->vtid2batch_infos[vtid];
    auto rwlock = &impl_->vtid2rwlock[vtid];
    auto &idle_process_ctxs = impl_->vtid2idle_process_ctxs[vtid];

    static __thread uint64_t next_dispatch_timestampe_ms = 0;
    uint64_t ready_timestamp_ms;

    if (next_dispatch_timestampe_ms && next_dispatch_timestampe_ms > comm::utils::Time::GetSteadyClockMS()) return;
    next_dispatch_timestampe_ms = 0;

    comm::utils::RWLock rwlock_read(rwlock, comm::utils::RWLock::LockMode::READ);

    for (auto &&kv : batch_infos) {
        auto batch_info = &kv.second;

        lock_guard<mutex> lock_guard(batch_info->first);
        auto &batch_task = batch_info->second;

        if (nullptr != batch_task) {
            if (batch_task->Ready(&ready_timestamp_ms)) {
                if (!idle_process_ctxs.empty()) {
                    auto ctx = idle_process_ctxs.top();
                    idle_process_ctxs.pop();
                    ctx->ready_batch_task = move(batch_task);
                    co_resume(ctx->co);
                }
            } else {
                if (ready_timestamp_ms > 0 && (0 == next_dispatch_timestampe_ms || ready_timestamp_ms < next_dispatch_timestampe_ms)) {
                    next_dispatch_timestampe_ms = ready_timestamp_ms;
                }
            }
        }
    }
}

static void *ProcessRoutineRun(void *arg) {
    co_enable_hook_sys();

    ProcessCtx_t *ctx = static_cast<ProcessCtx_t*>(arg);
    ctx->batch_helper->Process(ctx);
    return nullptr;
}


void BatchHelper::Process(ProcessCtx_t *ctx) {
    auto &idle_process_ctxs = impl_->vtid2idle_process_ctxs[ctx->vtid];

    while (1) {
        if (nullptr == ctx->ready_batch_task) {
            idle_process_ctxs.push(ctx);
            co_yield_ct();
            continue;
        }
        ctx->ready_batch_task->Process(true);
        ctx->ready_batch_task.release();
    }
}


void BatchHelper::DaemonThreadRun(int vtid) {
    stCoRoutineAttr_t attr;
    attr.stack_size = 1024 * impl_->process_routine_share_stack_size_kb;

    auto process_ctxs = unique_ptr<struct ProcessCtx_t[]>(new ProcessCtx_t[impl_->nprocess_routine]);
    for (int i{0}; i < impl_->nprocess_routine; ++i) {
        auto &&ctx = process_ctxs[i];
        ctx.vtid = vtid;
        ctx.pid = i;
        ctx.batch_helper = this;

        co_create(&(ctx.co), &attr, ProcessRoutineRun, &ctx);
        co_resume(ctx.co);
    }

    DispatchCtx_t ctx;
    ctx.batch_helper = this;
    ctx.vtid = vtid;
    co_eventloop(co_get_epoll_ct(), DispatchTickRun, &ctx);

    QLErr("DaemonThreadRun end");
    exit(0);
}

comm::RetCode BatchHelper::BatchRawAdd(const comm::proto::AddRequest &req) {
    auto batch_key = tuple<int, int, int>(req.topic_id(), req.store_id(), req.queue_id());

    BatchInfo *batch_info = nullptr;

    comm::ProducerBP::GetThreadInstance()->OnBatchRawAdd(req);

    auto vtid = req.queue_id() % impl_->ndaemon_thread;

    auto &batch_infos = impl_->vtid2batch_infos[vtid];
    auto rwlock = &impl_->vtid2rwlock[vtid];

    {
        comm::utils::RWLock rwlock_read(rwlock, comm::utils::RWLock::LockMode::READ);

        auto &&it = batch_infos.find(batch_key);
        if (batch_infos.end() != it) {
            batch_info = &it->second;
        }
    }


    if (nullptr == batch_info) {
        comm::utils::RWLock rwlock_read(rwlock, comm::utils::RWLock::LockMode::WRITE);

        auto &&it = batch_infos.find(batch_key);
        if (batch_infos.end() != it) {
            batch_info = &it->second;
        } else {
            batch_info = &batch_infos[batch_key];
        }
    }

    assert(nullptr != batch_info);

    shared_ptr<Task> task = make_shared<Task>(&req);
    unique_ptr<BatchTask> ready_batch_task = nullptr;
    {
        lock_guard<mutex> lock_guard(batch_info->first);

        auto &batch_task = batch_info->second;
        if (nullptr == batch_task) {
            batch_task.reset(new BatchTask(req.topic_id(), impl_->producer));
            batch_task->Init();
        }

        batch_task->AddTask(task);

        if (batch_task->Ready()) {
            ready_batch_task = move(batch_task);
        } else {
            task->PrepareWait();
        }
    }

    comm::RetCode retcode = comm::RetCode::RET_OK;
    if (nullptr != ready_batch_task) {
        retcode = ready_batch_task->Process();
    } else {
        retcode = task->Wait();
    }

    if (comm::RetCode::RET_OK == retcode) {
        comm::ProducerBP::GetThreadInstance()->OnBatchRawAddSucc(req);
    } else if (0 > as_integer(retcode)) {
        comm::ProducerBP::GetThreadInstance()->OnBatchRawAddFail(req);
        QLErr("ret %d", as_integer(retcode));
    }


    return retcode;
}





}  // namespace producer

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

