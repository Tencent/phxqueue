/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/consumer/consumer.h"

#include <cinttypes>
#include <condition_variable>
#include <errno.h>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <zlib.h>

#include "co_routine.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/store.h"
#include "phxqueue/producer.h"

#include "phxqueue/consumer/freqman.h"
#include "phxqueue/consumer/hblock.h"


namespace phxqueue {

namespace consumer {


using namespace std;


struct DispatchCtx_t {
    stCoRoutine_t *co{nullptr};
    Consumer *consumer{nullptr};
};

struct ConsumeCtx_t {
    stCoRoutine_t *co{nullptr};
    int cid{-1};
    Consumer *consumer{nullptr};
};

class Consumer::ConsumerImpl {
  public:
    ConsumerImpl() {}
    virtual ~ConsumerImpl() {}

    string topic;
    int topic_id{-1};
    comm::FactoryList<comm::HandlerFactory> fs;
    HeartBeatLock lock;
    FreqMan freq;
    ConsumerOption opt;

    int vpid{-1};
    comm::proto::ConsumerContext cc;

    int consume_fds[2];

    vector<shared_ptr<comm::proto::QItem>> items;
    vector<comm::HandleResult> handle_results;

    DispatchCtx_t dispatch_ctx;
    unique_ptr<ConsumeCtx_t[]> handle_ctxs;
    unique_ptr<ConsumeCtx_t[]> batch_handle_ctxs;
    unique_ptr<queue<int>[]> handle_buckets;
    unique_ptr<bool[]> batch_handle_finish;

    stCoCond_t *cond{nullptr};
    int nhandle_task_finished{0};
    int nbatch_handle_task_finished{0};

};

Consumer::Consumer(const ConsumerOption &opt) : impl_(new ConsumerImpl()) {
    assert(impl_);
    impl_->opt = opt;
}

Consumer::~Consumer() {}

const ConsumerOption *Consumer::GetConsumerOption() const {
    return &impl_->opt;
}

void Consumer::AddHandlerFactory(const int handle_id, comm::HandlerFactory *const hf) {
    impl_->fs.AddFactory(handle_id, hf);
}

unique_ptr<comm::Handler> Consumer::GetHandler(const int handle_id) {
    auto &&f(impl_->fs.GetFactory(handle_id));
    if (f) return move(unique_ptr<comm::Handler>(f->New()));
    return nullptr;
}

void Consumer::Run() {
    comm::RetCode ret;

    if (impl_->opt.log_func) {
        comm::Logger::GetInstance()->SetLogFunc(impl_->opt.log_func);
    }

    if (impl_->opt.config_factory_create_func) {
        plugin::ConfigFactory::SetConfigFactoryCreateFunc(impl_->opt.config_factory_create_func);
    }

    if (impl_->opt.break_point_factory_create_func) {
        plugin::BreakPointFactory::SetBreakPointFactoryCreateFunc(
                impl_->opt.break_point_factory_create_func);
    }

    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicIDByTopicName(impl_->opt.topic, impl_->topic_id))) {
        QLErr("GetTopicIDByTopicName ret %d topic %s",
              comm::as_integer(ret), impl_->opt.topic.c_str());
        return;
    }

    const int shm_key{impl_->opt.shm_key_base + impl_->topic_id};
    const string lock_path{impl_->opt.lock_path_base + to_string(impl_->topic_id)};

    if (comm::RetCode::RET_OK != (ret = impl_->lock.Init(
            this, shm_key, lock_path, impl_->opt.nprocs))) {
        QLErr("HeartBeatLock::Init ret %d", comm::as_integer(ret));
        return;
    }

    if (comm::RetCode::RET_OK != (ret = impl_->freq.Init(impl_->topic_id, this))) {
        QLErr("FreqMan::Init ret %d", comm::as_integer(ret));
        return;
    }

    BeforeFork();


    QLInfo("INFO: run sync");
    impl_->lock.RunSync();


    QLInfo("INFO: run freqman");
    impl_->freq.Run();

    QLInfo("INFO: procs %d", impl_->opt.nprocs);
    ForkAndRun(impl_->opt.nprocs);

    QLErr("consumer stop");
}

int Consumer::GetTopicID() {
    return impl_->topic_id;
}

comm::RetCode Consumer::GetQueuesDistribute(vector<Queue_t> &queues) {
    queues.clear();
    return impl_->lock.GetQueuesDistribute(queues);
}

comm::RetCode Consumer::Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp) {
    return comm::RetCode::RET_OK;
}

comm::RetCode Consumer::MakeHandleBuckets() {
    comm::ConsumerConsumeBP::GetThreadInstance()->OnMakeHandleBucket(impl_->cc);

    for (int i{0}; i < impl_->opt.nhandler; ++i) {
        while(!impl_->handle_buckets[i].empty())impl_->handle_buckets[i].pop();
    }

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d",
              comm::as_integer(ret), impl_->topic_id);
        return ret;
    }

    shared_ptr<const config::proto::QueueInfo> queue_info;
    if (comm::RetCode::RET_OK != (ret = topic_config->
                                  GetQueueInfoByQueue(impl_->cc.queue_id(), queue_info))) {
        QLErr("GetQueueInfoByQueue ret %d queue_id %d",
              comm::as_integer(ret), impl_->cc.queue_id());
        return ret;
    }

    int nbucket_used{0};
    map<uint64_t, int> uin2bucket_idx;
    int max_sz{-1};
    int max_handle_id{-1};
    uint64_t max_key(-1);
    for (int i{0}; i < impl_->items.size(); ++i) {
        auto key(impl_->items[i]->meta().uin());
        auto handle_id(impl_->items[i]->meta().handle_id());

        int bucket_idx{-1};
        bool update_uin2bucket_idx{false};
        if (key && !queue_info->handle_by_random_uin()) {
            auto &&it(uin2bucket_idx.find(key));
            if (uin2bucket_idx.end() != it) {
                bucket_idx = it->second;
            } else {
                update_uin2bucket_idx = true;
            }
        }
        if (-1 == bucket_idx) {
            if (nbucket_used >= impl_->opt.nhandler) {
                bucket_idx = comm::utils::OtherUtils::FastRand() % impl_->opt.nhandler;
            } else {
                bucket_idx = nbucket_used++;
            }
        }
        if (update_uin2bucket_idx) uin2bucket_idx[key] = bucket_idx;

        impl_->handle_buckets[bucket_idx].push(i);
        auto sz(impl_->handle_buckets[bucket_idx].size());
        if (-1 == max_sz || sz > max_sz) {
            max_sz = sz;
            max_handle_id = handle_id;
            max_key = key;
        }
    }

    QLInfo("nbucket_used %zu max_sz %d max_handle_id %d max_key %" PRIu64,
           nbucket_used, max_sz, max_handle_id, max_key);

    return comm::RetCode::RET_OK;
}


void Consumer::TaskDispatch() {

    // set nonblock for CoRead/CoWrite
    {
         auto flags(fcntl(impl_->consume_fds[1], F_GETFL, 0));
         fcntl(impl_->consume_fds[1], F_SETFL, flags | O_NONBLOCK);
    }

    comm::RetCode ret;

    char ch;
    while (true) {
        comm::ConsumerConsumeBP::GetThreadInstance()->OnTaskDispatch(impl_->cc);

        impl_->nhandle_task_finished = 0;
        impl_->nbatch_handle_task_finished = 0;

        while (!comm::utils::CoRead(impl_->consume_fds[1], &ch, sizeof(char))) {
            QLErr("CoRead fail");
            poll(nullptr, 0, 100);
        }

        if (comm::RetCode::RET_OK != (ret = MakeHandleBuckets())) {
            QLErr("MakeHandleBuckets ret %d", comm::as_integer(ret));
        }

        for (int i{0}; i < impl_->opt.nbatch_handler; ++i) {
            impl_->batch_handle_finish[i] = false;
            co_resume(impl_->batch_handle_ctxs[i].co);
        }

        for (int i{0}; i < impl_->opt.nhandler; ++i) {
            if (!impl_->handle_buckets[i].empty()) {
                co_resume(impl_->handle_ctxs[i].co);
            }
        }

        while (true) {
            if (IsAllTaskFinish()) break;
        }

        comm::ConsumerConsumeBP::GetThreadInstance()->OnTaskDispatchFinish(impl_->cc);


        while (!comm::utils::CoWrite(impl_->consume_fds[1], "a", sizeof(char))) {
            QLErr("CoWrite fail");
            poll(nullptr, 0, 100);
        }
    }
}

void Consumer::HandleTaskFinish() {
    ++impl_->nhandle_task_finished;
    if (impl_->nhandle_task_finished == impl_->items.size() &&
        impl_->nbatch_handle_task_finished == impl_->opt.nbatch_handler)
        co_cond_signal(impl_->cond);
}

void Consumer::BatchHandleTaskFinish() {
    ++impl_->nbatch_handle_task_finished;
    if (impl_->nhandle_task_finished == impl_->items.size() &&
        impl_->nbatch_handle_task_finished == impl_->opt.nbatch_handler)
        co_cond_signal(impl_->cond);
}

bool Consumer::IsAllTaskFinish() {
    if (impl_->nhandle_task_finished == impl_->items.size() &&
        impl_->nbatch_handle_task_finished == impl_->opt.nbatch_handler)
        return true;
    co_cond_timedwait(impl_->cond, 1000);
    return false;
}


void Consumer::HandleRoutineFunc(const int idx) {
    auto &&item(impl_->items[idx]);
    auto &&handle_result(impl_->handle_results[idx]);

    comm::ConsumerConsumeBP::GetThreadInstance()->OnHandleRoutineFunc(impl_->cc, idx);

    if (!item) {
        QLErr("item null. idx %d", idx);
        handle_result = comm::HandleResult::RES_ERROR;
        return;
    }

    if (SkipHandle(impl_->cc, *item)) {
        comm::ConsumerConsumeBP::GetThreadInstance()->OnSkipHandle(impl_->cc, *item);
        QLInfo("Handle skip handle_id %d ori_pub_id %d pub_id %d consumer_group_ids %" PRIu64
               " hash %" PRIu64 " uin %" PRIu64, item->meta().handle_id(),
               item->meta().pub_id(), item->pub_id(), (uint64_t)item->consumer_group_ids(),
               (uint64_t)item->meta().hash(), (uint64_t)item->meta().uin());
        handle_result = comm::HandleResult::RES_OK;
        return;
    }

    BeforeHandle(impl_->cc, *item);

    comm::RetCode ret;
    if (comm::RetCode::RET_OK != (ret = Handle(impl_->cc, *item, handle_result))) {
        QLErr("Handle ret %d", comm::as_integer(ret));
        return;
    }

    AfterHandle(impl_->cc, *item, handle_result);

    return;
}

void Consumer::BatchHandleRoutineFunc(const int idx) {
    comm::ConsumerConsumeBP::GetThreadInstance()->OnBatchHandleRoutineFunc(impl_->cc, idx);

    BeforeBatchHandle(impl_->cc, impl_->items, idx);

    comm::RetCode ret;
    if (comm::RetCode::RET_OK != (ret = BatchHandle(impl_->cc, impl_->items, idx))) {
        QLErr("Handle ret %d", comm::as_integer(ret));
        return;
    }

    AfterBatchHandle(impl_->cc, impl_->items, idx);

    return;
}

static void *DispatchRoutineRun(void *arg) {
    co_enable_hook_sys();

    DispatchCtx_t *ctx = static_cast<DispatchCtx_t*>(arg);
    ctx->consumer->TaskDispatch();
    return nullptr;
}

static void *HandleRoutineRun(void *arg) {
    co_enable_hook_sys();

    ConsumeCtx_t *ctx{static_cast<ConsumeCtx_t *>(arg)};

    while (true) {
        if (ctx->consumer->HasHandleTasks(ctx->cid)) {
            co_yield_ct();
            continue;
        }
        ctx->consumer->ProcessAllHandleTasks(ctx->cid);
    }
    return nullptr;
}

static void *BatchHandleRoutineRun(void *arg) {
    co_enable_hook_sys();

    ConsumeCtx_t *ctx{static_cast<ConsumeCtx_t *>(arg)};

    while (true) {
        if (ctx->consumer->HasBatchHandleTasks(ctx->cid)) {
            co_yield_ct();
            continue;
        }
        ctx->consumer->ProcessAllBatchHandleTasks(ctx->cid);
    }
    return nullptr;
}

bool Consumer::HasHandleTasks(const int cid) {
    return impl_->handle_buckets[cid].empty();
}

bool Consumer::HasBatchHandleTasks(const int cid) {
    return impl_->batch_handle_finish[cid];
}

void Consumer::ProcessAllHandleTasks(const int cid) {
    while (!impl_->handle_buckets[cid].empty()) {
        int idx = impl_->handle_buckets[cid].front();
        impl_->handle_buckets[cid].pop();

        HandleRoutineFunc(idx);
        HandleTaskFinish();
    }
}

void Consumer::ProcessAllBatchHandleTasks(const int cid) {
    if (!impl_->batch_handle_finish[cid]) {
        BatchHandleRoutineFunc(cid);
        BatchHandleTaskFinish();
        impl_->batch_handle_finish[cid] = true;
    }
}

void Consumer::ConsumeThreadRun(const int vpid) {
    OnConsumeThreadRun(vpid);
    comm::ConsumerConsumeBP::GetThreadInstance()->OnConsumeThreadRun(impl_->cc);

    //stShareStack_t *share_stack = co_alloc_sharestack(impl_->opt.nshare_stack,
    //                                                  1024 * impl_->opt.share_stack_size_kb);
    stCoRoutineAttr_t attr;
    //attr.stack_size = 0;
    attr.stack_size = 1024 * impl_->opt.share_stack_size_kb;
    //attr.share_stack = share_stack;

    impl_->cond = co_cond_alloc();

    if (0 != socketpair(PF_LOCAL, SOCK_STREAM, 0, impl_->consume_fds)) {
        QLErr("socketpair fail");
        return;
    }

    impl_->handle_buckets = unique_ptr<queue<int>[]>(new queue<int>[impl_->opt.nhandler]);

    impl_->handle_ctxs = unique_ptr<struct ConsumeCtx_t[]>(new ConsumeCtx_t[impl_->opt.nhandler]);
    for (int i{0}; i < impl_->opt.nhandler; ++i) {
        auto &&ctx(impl_->handle_ctxs[i]);
        ctx.co = nullptr;
        ctx.cid = i;
        ctx.consumer = this;

        co_create(&(ctx.co), &attr, HandleRoutineRun, &ctx);
        co_resume(ctx.co);
    }

    impl_->batch_handle_finish = unique_ptr<bool[]>(new bool[impl_->opt.nbatch_handler]);

    impl_->batch_handle_ctxs = unique_ptr<struct ConsumeCtx_t[]>(
            new ConsumeCtx_t[impl_->opt.nbatch_handler]);
    for (int i{0}; i < impl_->opt.nbatch_handler; ++i) {
        impl_->batch_handle_finish[i] = true;

        auto &&ctx(impl_->batch_handle_ctxs[i]);
        ctx.co = nullptr;
        ctx.cid = i;
        ctx.consumer = this;

        co_create(&(ctx.co), &attr, BatchHandleRoutineRun, &ctx);
        co_resume(ctx.co);
    }

    // task dispatch
    {
        auto &&ctx(impl_->dispatch_ctx);
        ctx.co = nullptr;
        ctx.consumer = this;
        co_create(&(ctx.co), &attr, DispatchRoutineRun, &ctx);
        co_resume(ctx.co);
    }


    co_eventloop(co_get_epoll_ct(), nullptr, this);

    comm::ConsumerConsumeBP::GetThreadInstance()->OnConsumeThreadRunEnd(impl_->cc);

    QLErr("ConsumeThreadRun end");
    exit(0);
}


void Consumer::ChildRun(const int vpid) {
    QLInfo("INFO: start vpid %u", vpid);

    impl_->vpid = vpid;

    comm::RetCode ret;

    OnChildRun(vpid);

    comm::proto::ConsumerContext &cc = impl_->cc;
    cc.set_topic_id(GetTopicID());

    comm::ConsumerBP::GetThreadInstance()->OnChildRun(cc.topic_id());

    thread t(bind(&Consumer::ConsumeThreadRun, this, vpid));

    while (true) {

        BeforeLock(cc);

        comm::ConsumerBP::GetThreadInstance()->OnLock(cc.topic_id());

        int consumer_group_id, store_id, queue_id;
        if (comm::RetCode::RET_OK !=
            (ret = impl_->lock.Lock(vpid, consumer_group_id, store_id, queue_id))) {
            if (comm::as_integer(ret) < 0)
                QLErr("DoLock ret %d vpid %u", comm::as_integer(ret), vpid);
            sleep(2);
            continue;
        }
        if (cc.consumer_group_id() != consumer_group_id ||
            cc.store_id() != store_id ||
            cc.queue_id() != queue_id) {
            cc.set_consumer_group_id(consumer_group_id);
            cc.set_store_id(store_id);
            cc.set_queue_id(queue_id);
            cc.set_prev_cursor_id(-1);
            cc.set_next_cursor_id(-1);
        }

        comm::ConsumerBP::GetThreadInstance()->OnLockSucc(cc);

        QLInfo("QUEUEINFO: vpid %u consumer_group_id %d store_id %d queue_id %d",
               vpid, cc.consumer_group_id(), cc.store_id(), cc.queue_id());

        AfterLock(cc);

        CheckMaxLoop(vpid);
        CheckMem(vpid);

        comm::ConsumerBP::GetThreadInstance()->OnProcess(cc);

        if (comm::RetCode::RET_OK != (ret = Process(cc))) {
            comm::ConsumerBP::GetThreadInstance()->OnProcessFail(cc);
            QLErr("ERR: Process ret %d", comm::as_integer(ret));
            usleep(100000);
        } else {
            comm::ConsumerBP::GetThreadInstance()->OnProcessSucc(cc);
        }
    }
}


void Consumer::MakeGetRequest(const comm::proto::ConsumerContext &cc,
                              const config::proto::QueueInfo &queue_info,
                              const int limit, comm::proto::GetRequest &req) {
    req.set_topic_id(impl_->topic_id);
    req.set_store_id(cc.store_id());
    req.set_queue_id(cc.queue_id());
    req.set_limit(limit);
    if (queue_info.delay() > 0) {
        auto now = comm::utils::Time::GetTimestampMS();
        req.set_atime(now / 1000 - queue_info.delay());
        req.set_atime_ms(now % 1000);
    }
    req.set_consumer_group_id(cc.consumer_group_id());

    req.set_prev_cursor_id(cc.prev_cursor_id());
    req.set_next_cursor_id(cc.next_cursor_id());

}

void Consumer::UpdateConsumerContextByGetResponse(const comm::proto::GetResponse &resp,
                                                  comm::proto::ConsumerContext &cc) {
    QLVerb("cc: consumer_group_id %d store_id %d queue_id %d prev_cursor_id (%" PRIu64 "->%" PRIu64
           ") next_cursor_id (%" PRIu64 "->%" PRIu64,
           cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
           (uint64_t)cc.prev_cursor_id(), (uint64_t)resp.prev_cursor_id(),
           (uint64_t)cc.next_cursor_id(), (uint64_t)resp.next_cursor_id());
    cc.set_prev_cursor_id(resp.prev_cursor_id());
    cc.set_next_cursor_id(resp.next_cursor_id());
}

comm::RetCode Consumer::Process(comm::proto::ConsumerContext &cc) {

    QLInfo("cc.consumer_group_id %d cc.store_id %d cc.queue_id %d",
           cc.consumer_group_id(), cc.store_id(), cc.queue_id());

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), impl_->topic_id);
        return ret;
    }

    if (!topic_config->IsValidQueue(cc.queue_id(), -1, cc.consumer_group_id())) {
        QLErr("IsValidQueue return false queue_id %d", cc.queue_id());
        return comm::RetCode::RET_ERR_RANGE_QUEUE;
    }

    shared_ptr<const config::proto::QueueInfo> queue_info;
    if (comm::RetCode::RET_OK != (ret = topic_config->
                                  GetQueueInfoByQueue(cc.queue_id(), queue_info))) {
        QLErr("GetQueueInfoByQueue ret %d queue_id %d", comm::as_integer(ret), cc.queue_id());
        return ret;
    }

    const int topic_id{topic_config->GetProto().topic().topic_id()};

    uint32_t maxlimit{topic_config->GetProto().topic().batch_limit()};
    uint32_t limit{topic_config->GetProto().topic().batch_limit()};

    const int sleep_us_per_get = queue_info->sleep_us_per_get();
    const int sleep_us_on_get_fail = queue_info->sleep_us_on_get_fail();
    const int sleep_us_on_get_no_item = queue_info->sleep_us_on_get_no_item();
    const int sleep_us_on_get_size_too_small = queue_info->sleep_us_on_get_size_too_small();


    const int queue_delay{queue_info->delay()};
    if (queue_delay < 0) {
        comm::ConsumerBP::GetThreadInstance()->OnGetQueueDelayFail(cc);
        QLErr("GetQueueDelay ret %d", queue_delay);
        return comm::RetCode::RET_ERR_RANGE;
    }

    auto start_loop_time = comm::utils::Time::GetSteadyClockMS();


    static double freq_quota = 0;
    static uint64_t last_fill_time = 0;

    static uint64_t last_topic_config_mod_time = 0;
    {
        auto new_topic_config_mod_time = topic_config->GetLastModTime();
        if (last_topic_config_mod_time != new_topic_config_mod_time) {
            last_topic_config_mod_time = new_topic_config_mod_time;
            if (freq_quota > 0) freq_quota = 0;
        }
    }

    while (true) {
        comm::ConsumerBP::GetThreadInstance()->OnLoop(cc);

        QLVerb("loop begin");

        auto now(comm::utils::Time::GetSteadyClockMS());
        if (start_loop_time < now - topic_config->GetProto().topic().consumer_lock_interval_s() * 1000) {
            comm::ConsumerBP::GetThreadInstance()->OnLoopBreak(cc);
            QLVerb("loop break");
            return comm::RetCode::RET_OK;
        }

        comm::proto::GetRequest req;
        comm::proto::GetResponse resp;
        MakeGetRequest(cc, *queue_info, limit, req);


        bool need_block{false}, need_freqlimit{false};
        int nrefill{0}, refill_interval_ms{0};
        impl_->freq.Judge(impl_->vpid, need_block, need_freqlimit, nrefill, refill_interval_ms);
        if (need_block) {
            static uint64_t last_block_log_time{0uLL};
            if (now > last_block_log_time + 100) {
                QLInfo("vpid %d need_block. topic %d consumer_group_id %d store_id %d queue_id %d",
                       impl_->vpid, impl_->topic_id, cc.consumer_group_id(), cc.store_id(), cc.queue_id());
                last_block_log_time = now;
            }
            if (freq_quota > 0) freq_quota = nrefill;
            last_fill_time = now;
            usleep(5000);
            continue;
        }


        if (need_freqlimit) {
            if (0 == last_fill_time) {
                freq_quota = nrefill;
                last_fill_time = now;
            } else if (now - last_fill_time >= refill_interval_ms) {
                freq_quota += (double)(now - last_fill_time) / refill_interval_ms * nrefill;
                last_fill_time = now;
            }

            if (freq_quota > 1e7) {
                freq_quota = 1e7;
            }

            if (req.limit() > freq_quota) {
                req.set_limit(freq_quota >= 0 ? (int)freq_quota : 0);
            }

            QLInfo("vpid %d need_freqlimit. topic %d consumer_group_id %d store_id %d queue_id %d "
                   "nrefill %d refill_interval_ms %d freq_quota %.2lf get_limit %u",
                   impl_->vpid, impl_->topic_id, cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
                   nrefill, refill_interval_ms, freq_quota, req.limit());
        } else {
            freq_quota = 0;
            last_fill_time = 0;
        }

        {
            uint64_t custom_prev_cursor_id{req.prev_cursor_id()};
            uint64_t custom_next_cursor_id{req.next_cursor_id()};

            int custom_limit = req.limit();
            CustomGetRequest(cc, req, custom_prev_cursor_id, custom_next_cursor_id, custom_limit);
            req.set_prev_cursor_id(custom_prev_cursor_id);
            req.set_next_cursor_id(custom_next_cursor_id);
            if (custom_limit >= 0 && custom_limit <= req.limit()) {
                req.set_limit(custom_limit);
            }
        }

        BeforeGet(req);

        comm::ConsumerBP::GetThreadInstance()->OnGet(cc);

        if (impl_->opt.use_store_master_client_on_get) {
            store::StoreMasterClient<comm::proto::GetRequest, comm::proto::GetResponse> store_master_client;
            ret = store_master_client.ClientCall(req, resp, bind(&Consumer::Get, this,
                                                                 placeholders::_1, placeholders::_2));
        } else {
            ret = Get(req, resp);
        }

        QLInfo("Dequeue ret %d topic %d consumer_group_id %d store_id %d queue_id %d size %u "
               "prev_cursor_id %" PRIu64 " next_cursor_id %" PRIu64,
               comm::as_integer(ret), cc.topic_id(), cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
               resp.items_size(), (uint64_t)resp.prev_cursor_id(), (uint64_t)resp.next_cursor_id());

        freq_quota -= resp.items_size();


        if (comm::RetCode::RET_OK != ret) {
            comm::ConsumerBP::GetThreadInstance()->OnGetFail(cc);

            QLErr("Get ret %d", comm::as_integer(ret));
            usleep(sleep_us_on_get_fail + (sleep_us_on_get_fail ? comm::utils::OtherUtils::FastRand() %
                                           sleep_us_on_get_fail : 0));

            limit /= 2;
            if (limit < 1) { limit = 1; }

            continue;
        } else {
            comm::ConsumerBP::GetThreadInstance()->OnGetSucc(cc);

            limit += (maxlimit >= 10 ? maxlimit / 10 : 1);
            if (limit > maxlimit) { limit = maxlimit; }
        }

        AfterGet(req, resp);

        UpdateConsumerContextByGetResponse(resp, cc);

        if (0 == resp.items_size()) {
            comm::ConsumerBP::GetThreadInstance()->OnGetNoItem(cc);
            usleep(sleep_us_on_get_no_item + (sleep_us_on_get_no_item ? comm::utils::OtherUtils::FastRand() %
                                              sleep_us_on_get_no_item : 0));
        }

        if (sleep_us_per_get) {
            comm::ConsumerBP::GetThreadInstance()->OnSleepAfterGet(cc);
            usleep(sleep_us_per_get + (sleep_us_per_get ? comm::utils::OtherUtils::FastRand() % sleep_us_per_get : 0));
        }

        vector<shared_ptr<comm::proto::QItem>> &items = impl_->items;
        items.clear();
        for (size_t i{0}; i < resp.items_size(); ++i) {
            auto item = resp.mutable_items(i);

            QLVerb("get item hash %" PRIu64 " count %d", (uint64_t)item->meta().hash(), item->count());

            if (!topic_config->ShouldSkip(*item, cc.consumer_group_id(), queue_info->queue_info_id())) {
                items.emplace_back(make_shared<comm::proto::QItem>(), item);
            }
        }

        comm::ConsumerBP::GetThreadInstance()->OnGetWithItem(cc, items);

        if (queue_info->drop_all()) {
            comm::ConsumerBP::GetThreadInstance()->OnDropAll(cc, items);
        } else {
            vector<comm::HandleResult> &handle_results = impl_->handle_results;
            handle_results.clear();

            if (items.size()) {
                if (comm::RetCode::RET_OK != (ret = Consume(cc, items, handle_results))) {
                    QLErr("BatchHandle ret %d", comm::as_integer(ret));
                }
            }

            AfterConsume(cc, items, handle_results);
        }

        if (items.size() && items.size() < queue_info->get_size_too_small_threshold()) {
            comm::ConsumerBP::GetThreadInstance()->OnGetSizeTooSmall(cc, items);
            if (!need_freqlimit)
                usleep(sleep_us_on_get_size_too_small + (sleep_us_on_get_size_too_small ?
                        comm::utils::OtherUtils::FastRand() % sleep_us_on_get_size_too_small : 0));
        }
    }

    return comm::RetCode::RET_OK;
}



comm::RetCode
Consumer::GetQueueByAddrScale(const vector<consumer::Queue_t> &queues,
                              const consumer::AddrScales &addr_scales,
                              set<size_t> &queue_idxs) {
    queue_idxs.clear();

    map<uint32_t, uint64_t> hash2encoded_addr;

    comm::proto::Addr local_addr;
    local_addr.set_ip(impl_->opt.ip);
    local_addr.set_port(impl_->opt.port);

    uint64_t local_encoded_addr = comm::utils::EncodeAddr(local_addr);

    for (int i{0}; i < addr_scales.size(); ++i) {
        auto &addr = addr_scales[i].addr();
        auto scale = addr_scales[i].scale();

        uint64_t encoded_addr = comm::utils::EncodeAddr(addr);
        for (int j{0}; j < scale; ++j) {
            size_t h = 0;
            h = comm::utils::MurmurHash64(&encoded_addr, sizeof(uint64_t), h);
            h = comm::utils::MurmurHash64(&j, sizeof(int), h);


            hash2encoded_addr.emplace(h, encoded_addr);
            //QLInfo("insert h %u encoded_addr %" PRIu64 " j %d", h, encoded_addr, j);
        }
    }

    if (hash2encoded_addr.empty()) {
        QLWarn("hash2encoded_addr empty");
        return comm::RetCode::RET_OK;
    }

    for (int i{0}; i < queues.size(); ++i) {
        auto &&queue(queues[i]);
        size_t h{0u};
        h = comm::utils::MurmurHash64(&queue.consumer_group_id, sizeof(uint32_t), h);
        h = comm::utils::MurmurHash64(&queue.store_id, sizeof(uint32_t), h);
        h = comm::utils::MurmurHash64(&queue.queue_id, sizeof(uint32_t), h);


        auto &&it = hash2encoded_addr.lower_bound(h);
        if (hash2encoded_addr.end() == it) it = hash2encoded_addr.begin();

        //QLInfo("find h %u %" PRIu64, h, it->second);

        if (it->second == local_encoded_addr) {
            queue_idxs.insert(i);
        }
    }
    QLInfo("queues.size %zu addrscales.size %zu queue_idxs.size %zu local_encoded_addr %" PRIu64,
           queues.size(), addr_scales.size(), queue_idxs.size(), local_encoded_addr);

    return comm::RetCode::RET_OK;
}

comm::RetCode Consumer::Consume(const comm::proto::ConsumerContext &cc,
                                const vector<shared_ptr<comm::proto::QItem>> &items,
                                vector<comm::HandleResult> &handle_results) {
    comm::ConsumerBP::GetThreadInstance()->OnConsume(cc, items);

    handle_results.clear();
    for (int i{0}; i < items.size(); ++i) {
        handle_results.push_back(comm::HandleResult::RES_ERROR);
    }

    {
        int iret = 0;

        while (sizeof(char) != (iret = write(impl_->consume_fds[0], "a", sizeof(char)))) {
            QLErr("write ret %d err %s", iret, strerror(errno));
            poll(nullptr, 0, 100);
        }


        char ch;
        while (sizeof(char) != (iret = read(impl_->consume_fds[0], &ch, sizeof(char)))) {
            QLErr("read ret %d err %s", iret, strerror(errno));
            poll(nullptr, 0, 100);
        }
    }

    comm::ConsumerBP::GetThreadInstance()->OnConsumeSucc(cc, items, handle_results);

    impl_->freq.UpdateConsumeStat(impl_->vpid, cc, items);

    return comm::RetCode::RET_OK;
}

bool Consumer::SkipHandle(const comm::proto::ConsumerContext &cc, const comm::proto::QItem &item) {
    if (!(item.consumer_group_ids() & (1uLL << (cc.consumer_group_id() - 1)))) return true;
    return false;
}

comm::RetCode Consumer::Handle(const comm::proto::ConsumerContext &cc,
                               comm::proto::QItem &item, comm::HandleResult &handle_result) {

    comm::ConsumerConsumeBP::GetThreadInstance()->OnHandle(impl_->cc, item);

    comm::RetCode ret;

    RestoreUserCookies(item.meta().user_cookies());

    string uncompressed_buffer;
    if (comm::RetCode::RET_OK !=
        (ret = UncompressBuffer(item.buffer(), item.buffer_type(), uncompressed_buffer))) {
        QLErr("DecodeBuffer ret %d", comm::as_integer(ret));
        handle_result = comm::HandleResult::RES_ERROR;
        return ret;
    }

    auto &&handler(GetHandler(item.meta().handle_id()));
    if (!handler) {
        QLErr("GetHandler return null handle_id %d", item.meta().handle_id());
        handle_result = comm::HandleResult::RES_ERROR;
        return comm::RetCode::RET_ERR_RANGE_HANDLE;
    }

    handle_result = handler->Handle(cc, item, uncompressed_buffer);
    QLVerb("handle_result %d handle->IsBufferUpdated() %d",
           static_cast<int>(handle_result), handler->IsBufferUpdated());

    if (handler->IsBufferUpdated()) {
        CompressBuffer(uncompressed_buffer, *item.mutable_buffer(), item.buffer_type());
    }

    comm::ConsumerConsumeBP::GetThreadInstance()->OnHandleEnd(impl_->cc, item, handle_result);

    auto &&client_id(item.meta().client_id());
    if (client_id.empty()) {
        QLVerb("Handle handle_id %d ori_pub_id %d pub_id %d consumer_group_ids %" PRIu64
               " hash %" PRIu64 " uin %" PRIu64 " handle_result %d",
               item.meta().handle_id(), item.meta().pub_id(), item.pub_id(),
               (uint64_t)item.consumer_group_ids(), (uint64_t)item.meta().hash(),
               (uint64_t)item.meta().uin(), handle_result);
    } else {
        QLInfo("Handle handle_id %d ori_pub_id %d pub_id %d consumer_group_ids %" PRIu64
               " hash %" PRIu64 " uin %" PRIu64 " handle_result %d client_id %s",
               item.meta().handle_id(), item.meta().pub_id(), item.pub_id(),
               (uint64_t)item.consumer_group_ids(), (uint64_t)item.meta().hash(),
               (uint64_t)item.meta().uin(), handle_result, client_id.c_str());
    }

    return comm::RetCode::RET_OK;
}

void Consumer::CheckMaxLoop(const int vpid) {
    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d",
              comm::as_integer(ret), impl_->topic_id);
        return;
    }

    static uint32_t nloop{0u};
    ++nloop;

    if (0 == nloop % 100) {
        QLInfo("nloop %u", nloop);
    }

    auto consumer_max_loop_per_proc(topic_config->GetProto().topic().consumer_max_loop_per_proc());
    uint32_t fixed_limit(consumer_max_loop_per_proc +
                         consumer_max_loop_per_proc / 100.0 * (vpid % 20));
    if (consumer_max_loop_per_proc && nloop > fixed_limit) {
        comm::ConsumerBP::GetThreadInstance()->OnMaxLoopCheckUnpass(impl_->topic_id);
        QLInfo("nloop(%u) > fixed_limit(%u), kill it, consumer_max_loop_per_proc %u",
               nloop, fixed_limit, consumer_max_loop_per_proc);
        nloop = 0;
        exit(-1);
    }
    comm::ConsumerBP::GetThreadInstance()->OnMaxLoopCheckPass(impl_->topic_id);
}

void Consumer::CheckMem(const int vpid) {
    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), impl_->topic_id);
        return;
    }

    auto mem_size_limit(topic_config->GetProto().topic().consumer_max_mem_size_mb_per_proc());
    if (!mem_size_limit) return;

    static time_t last_check_time(0);
    auto now(time(nullptr));
    if (last_check_time + 60 < now) {
        last_check_time = now;

        comm::ConsumerBP::GetThreadInstance()->OnMemCheck(impl_->topic_id);

        comm::utils::MemStat mem_stat;
        if (!mem_stat.Stat()) return;

        if (mem_stat.resident > mem_stat.share) {
            auto mem_size = (mem_stat.resident - mem_stat.share) / 256;
            QLInfo("mem_size %d mem_size_limit %d", mem_size, mem_size_limit);
            uint32_t fixed_limit = mem_size_limit + mem_size_limit / 100.0 * (vpid % 20);
            if (mem_size > fixed_limit) {
                comm::ConsumerBP::GetThreadInstance()->OnMemCheckUnpass(impl_->topic_id);

                QLErr("ERR: memory size %u > %u MB, kill it, res %lu, "
                      "share %lu, size %lu, mem_size_limit %u",
                      mem_size, fixed_limit, mem_stat.resident, mem_stat.share,
                      mem_stat.size, mem_size_limit);
                exit(-1);
            }
        }

        comm::ConsumerBP::GetThreadInstance()->OnMemCheckPass(impl_->topic_id);
    }
}

void Consumer::CustomGetRequest(const comm::proto::ConsumerContext &cc,
                                const comm::proto::GetRequest &req,
                                uint64_t &pre_cursor_id, uint64_t &next_cursor_id, int &limit) {
    pre_cursor_id = req.next_cursor_id();
}

void Consumer::AfterConsume(const comm::proto::ConsumerContext &cc,
                            const vector<shared_ptr<comm::proto::QItem>> &items,
                            const vector<comm::HandleResult> &handle_results) {

    comm::RetCode ret;

    if (handle_results.size() < items.size()) {
        QLErr("handle_results.size() %d < items.size() %d",
              (int)handle_results.size(), (int)items.size());
        return;
    }

    vector<shared_ptr<comm::proto::QItem>> retry_items;
    for (int i{0}; i < items.size(); ++i) {
        auto &&item = items[i];
        auto &&handle_result = handle_results[i];
        if (comm::HandleResult::RES_ERROR == handle_result) {
            retry_items.push_back(item);
        }
    }

    if (0 == retry_items.size()) return;

    vector<unique_ptr<comm::proto::AddRequest>> reqs;

    uint64_t retry_consumer_group_ids(1uLL << (cc.consumer_group_id() - 1));
    {
        ret = phxqueue::producer::Producer::MakeAddRequests(cc.topic_id(), retry_items, reqs,
                [&cc, retry_consumer_group_ids](comm::proto::QItem &item)->void {
            item.set_count(item.count() + 1);
            item.set_consumer_group_ids(retry_consumer_group_ids);

            auto now(comm::utils::Time::GetTimestampMS());
            item.set_atime(now / 1000);
            item.set_atime_ms(now % 1000);
        });
        if (comm::RetCode::RET_OK != ret) {
            QLErr("MakeAddRequests ret %d", as_integer(ret));
            return;
        }
    }

    for (auto &&req : reqs) {
        if (!req || 0 == req->items_size()) continue;

        int retry_pub_id{req->items(0).pub_id()};

        BeforeAdd(*req);

        comm::proto::AddResponse resp;
        while (true) {  // retry forever
            if (impl_->opt.use_store_master_client_on_add) {
                store::StoreMasterClient<comm::proto::AddRequest, comm::proto::AddResponse>
                        store_master_client;
                ret = store_master_client.ClientCall(*req, resp,
                        bind(&Consumer::Add, this, placeholders::_1, placeholders::_2));
            } else {
                ret = Add(*req, resp);
            }
            if (comm::RetCode::RET_OK != ret) {
                QLErr("Retry ret %d topic_id %d retry_pub_id %d consumer_group_id %d store_id %d "
                      "queue_id %d item_size %zu retry_item_size %d",
                      ret, cc.topic_id(), retry_pub_id, cc.consumer_group_id(), cc.store_id(),
                      cc.queue_id(), items.size(), req->items_size());
            } else {
                QLInfo("INFO: Retry succ topic_id %d retry_pub_id %d consumer_group_id %d store_id %d "
                       "queue_id %d item_size %zu retry_item_size %d",
                      cc.topic_id(), retry_pub_id, cc.consumer_group_id(), cc.store_id(),
                      cc.queue_id(), items.size(), req->items_size());
                break;
            }

            poll(nullptr, 0, 1000);
        }

        AfterAdd(*req, resp);

    }
}


}  // namespace consumer

}  // namespace phxqueue

