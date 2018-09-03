/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "publish_mgr.h"

#include <functional>

#include "phxqueue/comm.h"
#include "phxqueue/store.h"
#include "phxqueue_phxrpc/app/logic/mqtt.h"
#include "phxqueue_phxrpc/app/store/store_client.h"

#include "../mqtt/mqtt_msg.h"
#include "../mqtt/mqtt_packet_id.h"
#include "../mqtt/mqtt_session.h"
#include "../mqttbroker_server_config.h"
#include "../server_mgr.h"
#include "publish_memory.h"


namespace {


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace phxqueue_phxrpc::mqttbroker;
using namespace std;


struct DispatchCtx {
    stCoRoutine_t *co{nullptr};
    uint64_t session_id{0uL};
    const MqttBrokerServerConfig *config{nullptr};
};

int Publish(const HttpPublishPb &req, const DispatchCtx *const ctx) {
    // 1. check publish session
    const auto sub_mqtt_session(MqttSessionMgr::GetInstance()->
                                GetByClientId(req.sub_client_id()));
    if (!sub_mqtt_session) {
        NLErr("GetByClientId err sub_client_id \"%s\"", req.sub_client_id().c_str());

        return -1;
    }

    if (sub_mqtt_session->session_id != ctx->session_id) {
        return 0;
    }

    // 2. publish to event_loop_server
    MqttPublishPb mqtt_publish_pb;
    mqtt_publish_pb.CopyFrom(req.mqtt_publish());
    // mqtt-3.3.1-3: reset dup
    mqtt_publish_pb.set_dup(false);

    if (1 == mqtt_publish_pb.qos()) {
        // alloc packet_id
        uint16_t sub_packet_id{0u};
        if (!MqttPacketIdMgr::GetInstance()->AllocPacketId(req.cursor_id(), req.pub_client_id(),
                req.mqtt_publish().packet_identifier(), req.sub_client_id(), &sub_packet_id)) {
            NLErr("sub_session_id %" PRIx64 " AllocPacketId err sub_client_id \"%s\"",
                  ctx->session_id, req.sub_client_id().c_str());

            return -1;
        }

        // send
        mqtt_publish_pb.set_packet_identifier(sub_packet_id);
        auto *mqtt_publish(new MqttPublish);
        mqtt_publish->FromPb(mqtt_publish_pb);
        int ret{-1};
        while (true) {
            ret = phxqueue_phxrpc::mqttbroker::ServerMgr::GetInstance()->Send(ctx->session_id, mqtt_publish);
            //ret = server_mgr->Send(ctx->session_id, mqtt_publish);
            if (0 != ret) {
                NLErr("sub_session_id %" PRIx64 " server_mgr.Send err %d sub_client_id \"%s\" qos %u",
                      ctx->session_id, ret, req.sub_client_id().c_str(),
                      req.mqtt_publish().qos());
                poll(nullptr, 0, ctx->config->publish_sleep_time_ms());

                continue;
            }

            break;  // finish send
        }
    } else {
        auto *mqtt_publish(new MqttPublish);
        mqtt_publish->FromPb(mqtt_publish_pb);
        int ret{-1};
        while (true) {
            ret = phxqueue_phxrpc::mqttbroker::ServerMgr::GetInstance()->
                    Send(ctx->session_id, mqtt_publish);
            //ret = server_mgr->Send(ctx->session_id, mqtt_publish);
            if (0 != ret) {
                NLErr("sub_session_id %" PRIx64 " server_mgr.Send err %d sub_client_id \"%s\" qos %u",
                      ctx->session_id, ret, req.sub_client_id().c_str(),
                      req.mqtt_publish().qos());
                poll(nullptr, 0, ctx->config->publish_sleep_time_ms());

                continue;
            }
            MqttPacketIdMgr::GetInstance()->SetPrevCursorId(req.sub_client_id(), req.cursor_id());

            break;  // finish send
        }

        // set remote prev_cursor_id
        TableMgr table_mgr(1000);
        //TableMgr table_mgr(ctx->config->topic_id());
        uint64_t version{0uLL};
        SessionPb session_pb;
        table_mgr.GetSessionByClientIdRemote(req.sub_client_id(), &version, &session_pb);
        session_pb.set_prev_cursor_id(req.cursor_id());
        table_mgr.SetSessionByClientIdRemote(req.sub_client_id(), version, session_pb);

        NLInfo("sub_session_id %" PRIx64 " server_mgr.Send sub_client_id \"%s\" qos %u",
               ctx->session_id, req.sub_client_id().c_str(),
               req.mqtt_publish().qos());
    }

    return 0;
}

phxqueue::comm::RetCode
Get(const phxqueue::comm::proto::GetRequest &req,
    phxqueue::comm::proto::GetResponse &resp) {
    static thread_local StoreClient store_client;
    auto ret(store_client.ProtoGet(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        NLErr("ProtoGet ret %d", phxqueue::comm::as_integer(ret));
    }

    return ret;
}

void *PublishRoutineRun(void *arg) {
    co_enable_hook_sys();
    printf("%s:%d co %p begin\n", __func__, __LINE__, co_self());

    DispatchCtx *ctx{static_cast<DispatchCtx *>(arg)};
    auto it(PublishQueue::GetInstance()->cbegin());
    uint64_t last_cursor_id(-1);
    uint32_t nr_succ{0u};

    while (true) {
        // 1. pick from publish queue
        if (PublishQueue::GetInstance()->cend() == it) {
            // already consumed all items, wait
            poll(nullptr, 0, ctx->config->publish_sleep_time_ms());

            continue;
        } else if (!it.Valid()) {
            // fall behind, start from begin
            it = PublishQueue::GetInstance()->cbegin();
            if (!it.Valid()) {
                // publish queue empty
                poll(nullptr, 0, ctx->config->publish_sleep_time_ms());

                continue;
            }
        }

        auto &&kv(*it);
        // 2. if lost msg
        if (-1 != last_cursor_id && last_cursor_id + 1 < kv.first) {
            for (uint64_t cursor_id{last_cursor_id + 1}; kv.first > cursor_id; ++cursor_id) {
                // 2.1. get from lru cache
                HttpPublishPb http_publish_pb;
                int ret{PublishLruCache::GetInstance()->Get(cursor_id, http_publish_pb)};
                if (0 != ret) {
                    // 2.2. if no cache, rpc get from store
                    phxqueue::comm::proto::GetRequest req;
                    phxqueue::comm::proto::GetResponse resp;
                    req.set_topic_id(ctx->config->topic_id());
                    req.set_store_id(kv.second.store_id());
                    req.set_queue_id(kv.second.queue_id());
                    req.set_prev_cursor_id(-1);
                    req.set_next_cursor_id(kv.first);
                    req.set_limit(10);
                    req.set_random(true);
                    phxqueue::store::StoreMasterClient<phxqueue::comm::proto::GetRequest,
                            phxqueue::comm::proto::GetResponse> store_master_client;
                    phxqueue::comm::RetCode ret2{store_master_client.ClientCall(
                            req, resp, bind(Get, placeholders::_1, placeholders::_2))};
                    if (phxqueue::comm::RetCode::RET_OK != ret2) {
                        NLErr("Get ret %d", phxqueue::comm::as_integer(ret2));
                    } else {
                        for (const auto &item : resp.items()) {
                            if (kv.first == item.cursor_id()) {
                                NLErr("skip %" PRIu64, item.cursor_id());

                                continue;
                            }

                            // 2.3. set to lru cache
                            phxqueue_phxrpc::logic::mqtt::HttpPublishPb message;
                            if (!message.ParseFromString(item.buffer())) {
                                NLErr("ParseFromString err");

                                continue;
                            }
                            PublishLruCache::GetInstance()->Put(cursor_id, message);
                            // 2.4. add to eventloop server out queue
                            ret = Publish(message, ctx);
                            if (ret) {
                                NLErr("Publish err %d", ret);

                                continue;
                            }
                        }
                    }

                    break;
                }
            }
        }
        last_cursor_id = kv.first;

        // 3. add to el out queue
        int ret{Publish(kv.second, ctx)};
        if (ret) {
            NLErr("Publish err %d", ret);
        }
        // 3.1. if session die
        //return nullptr;
        ++nr_succ;

        // 4. rpc set prev to lock
        if (0 == (nr_succ % 10)) {
            // yield other co
            poll(nullptr, 0, 0);
        }

        ++it;
    }

    delete ctx;
    ctx = nullptr;
}

int CoTickFunc(void *args) {
    PublishThread *publish_thread((PublishThread *)args);
    uint64_t session_id(-1);
    SessionOpType op{SessionOpType::NOP};
    //printf("%s:%d config %p\n", __func__, __LINE__, publish_thread->config());
    while (0 == publish_thread->PopSessionOp(&session_id, &op)) {
        if (SessionOpType::CREATE == op) {
            stCoRoutineAttr_t attr;
            attr.stack_size = 1024 * publish_thread->config()->share_stack_size_kb();

            DispatchCtx *ctx{new DispatchCtx};
            ctx->co = nullptr;
            ctx->session_id = session_id;
            ctx->config = publish_thread->config();
            co_create(&(ctx->co), &attr, PublishRoutineRun, ctx);
            co_resume(ctx->co);
        } else if (SessionOpType::DESTROY == op) {
        }
    }

    return 0;
}


}  // namespace


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


int SessionOpQueue::Push(const uint64_t session_id, const SessionOpType op) {
    lock_guard<mutex> lock(mutex_);

    queue_.emplace(session_id, op);

    return 0;
}

int SessionOpQueue::Pop(uint64_t *const session_id, SessionOpType *const op) {
    if (!session_id || !op) {
        return -1;
    }

    lock_guard<mutex> lock(mutex_);

    if (queue_.empty()) {
        return -1;
    }

    auto session_op(queue_.front());
    *session_id = session_op.session_id;
    *op = session_op.op;
    queue_.pop();

    return 0;
}


PublishThread::PublishThread(const MqttBrokerServerConfig *const config)
        : config_(config), thread_(&PublishThread::RunFunc, this) {
}

PublishThread::PublishThread(PublishThread &&publish_thread) {
    config_ = publish_thread.config_;
    publish_thread.config_ = nullptr;
    thread_ = move(publish_thread.thread_);
}

PublishThread::~PublishThread() {
    thread_.join();
}

void PublishThread::RunFunc() {
    co_eventloop(co_get_epoll_ct(), CoTickFunc, this);
}

int PublishThread::CreateSession(const uint64_t session_id) {
    return session_op_queue_.Push(session_id, SessionOpType::CREATE);
}

int PublishThread::DestroySession(const uint64_t session_id) {
    return session_op_queue_.Push(session_id, SessionOpType::DESTROY);
}

int PublishThread::PopSessionOp(uint64_t *const session_id, SessionOpType *const op) {
    return session_op_queue_.Pop(session_id, op);
}


unique_ptr<PublishMgr> PublishMgr::s_instance;

PublishMgr *PublishMgr::GetInstance() {
    return s_instance.get();
}

void PublishMgr::SetInstance(PublishMgr *const instance) {
    s_instance.reset(instance);
}


PublishMgr::PublishMgr(const MqttBrokerServerConfig *const config)
        : config_(config), nr_publish_threads_(config->nr_publish_thread()) {
    for (int i{0}; nr_publish_threads_ > i; ++i) {
        publish_threads_.emplace_back(move(unique_ptr<PublishThread>(new PublishThread(config))));
    }
}

PublishMgr::~PublishMgr() {
}

int PublishMgr::CreateSession(const uint64_t session_id) {
    return publish_threads_[hash<uint64_t>{}(session_id) % nr_publish_threads_]->
            CreateSession(session_id);
}

int PublishMgr::DestroySession(const uint64_t session_id) {
    return publish_threads_[hash<uint64_t>{}(session_id) % nr_publish_threads_]->
            DestroySession(session_id);
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

