/*
Tencent is pleased to support the open source community by making
PhxRPC available.
Copyright (C) 2016 THL A29 Limited, a Tencent company.
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may
not use this file except in compliance with the License. You may
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

See the AUTHORS file for names of contributors.
*/

#include "event_loop_server.h"

#include <cassert>

#include "mqtt/mqtt_msg_handler.h"


using namespace std;


int Session::GetServerUnitIdx(const uint64_t session_id) {
    return ((session_id >> 48) & 0xFFFF);
}

Session::Session() {
}

Session::~Session() {
    while (!resps.empty()) {
        const auto &resp(resps.front());
        resps.pop();
        delete resp;
    }
}


SessionMgr::SessionMgr(const int idx, phxrpc::UThreadEpollScheduler *const scheduler,
                       const EventLoopServerConfig *config, phxrpc::HshaServerStat *server_stat)
        : idx_(idx), scheduler_(scheduler), config_(config), server_stat_(server_stat) {
}

SessionMgr::~SessionMgr() {
}

shared_ptr<Session> SessionMgr::CreateSession(const int fd) {
    const auto &session(make_shared<Session>());
    uint64_t idx_part{static_cast<uint64_t>(idx_) & 0xFFFF};
    uint64_t time_part{phxrpc::Timer::GetTimestampMS() & 0xFFFF};
    uint64_t seq_part{static_cast<uint64_t>(++s_seq)};
    session->session_id = (idx_part << 48) | (time_part << 32) | seq_part;
    session->in_socket = scheduler_->CreateSocket(fd);
    session->out_socket = scheduler_->CreateSocket(dup(fd));
    UThreadSetSocketTimeout(*(session->in_socket), config_->GetSocketTimeoutMS());
    UThreadSetSocketTimeout(*(session->out_socket), config_->GetSocketTimeoutMS());
    session->in_stream.reset(new phxrpc::UThreadTcpStream);
    session->out_stream.reset(new phxrpc::UThreadTcpStream);
    session->in_stream->Attach(session->in_socket);
    session->out_stream->Attach(session->out_socket);
    session->in_stream->exceptions(ios::failbit | ios::badbit);
    session->out_stream->exceptions(ios::failbit | ios::badbit);

    auto &&kv(session_id2session_map_.emplace(session->session_id, session));

    if (!kv.second) {
        phxrpc::log(LOG_ERR, "%s session_id2session_map.emplace err", __func__);

        return nullptr;
    }

    server_stat_->hold_fds_++;

    return kv.first->second;
}

shared_ptr<Session> SessionMgr::GetSession(const uint64_t session_id) {
    auto &&it(session_id2session_map_.find(session_id));
    if (session_id2session_map_.end() != it) {
        return it->second;
    }

    return nullptr;
}

void SessionMgr::DestroySession(const uint64_t session_id) {
    auto &&it(session_id2session_map_.find(session_id));
    if (session_id2session_map_.end() != it) {
        server_stat_->hold_fds_--;
        session_id2session_map_.erase(it);
    }
}

atomic_uint32_t SessionMgr::s_seq{0};


EventLoopServerIO::EventLoopServerIO(const int idx, phxrpc::UThreadEpollScheduler *const scheduler,
                       const EventLoopServerConfig *config, phxrpc::DataFlow *data_flow,
                       phxrpc::HshaServerStat *server_stat, phxrpc::HshaServerQos *server_qos,
                       phxrpc::WorkerPool *worker_pool,
                       phxrpc::BaseMessageHandlerFactory *const factory)
        : idx_(idx), scheduler_(scheduler), config_(config), data_flow_(data_flow),
          server_stat_(server_stat), server_qos_(server_qos), worker_pool_(worker_pool),
          factory_(factory), session_mgr_(idx, scheduler, config, server_stat) {
}

EventLoopServerIO::~EventLoopServerIO() {
}

bool EventLoopServerIO::AddAcceptedFd(const int accepted_fd) {
    lock_guard<mutex> lock(queue_mutex_);
    if (accepted_fd_list_.size() > MAX_ACCEPT_QUEUE_LENGTH) {
        return false;
    }
    accepted_fd_list_.push(accepted_fd);
    if (static_cast<int>(server_stat_->io_read_request_qps_) < 5000 &&
        static_cast<int>(server_stat_->accept_qps_) < 5000) {
        scheduler_->NotifyEpoll();
    }
    return true;
}

void EventLoopServerIO::HandlerAcceptedFd() {
    lock_guard<mutex> lock(queue_mutex_);
    while (!accepted_fd_list_.empty()) {
        int accepted_fd = accepted_fd_list_.front();
        accepted_fd_list_.pop();

        const auto &session{session_mgr_.CreateSession(accepted_fd)};
        if (!session) {
            phxrpc::log(LOG_ERR, "%s CreateSession err fd %d", __func__, accepted_fd);

            return;
        }

        scheduler_->AddTask(bind(&EventLoopServerIO::UThreadIFunc, this, session->session_id), nullptr);
        scheduler_->AddTask(bind(&EventLoopServerIO::UThreadOFunc, this, session->session_id), nullptr);
    }
}

phxrpc::UThreadSocket_t *EventLoopServerIO::ActiveSocketFunc() {
    while (data_flow_->CanPluckResponse()) {
        void *args{nullptr};
        phxrpc::BaseResponse *resp{nullptr};
        int queue_wait_time_ms{data_flow_->PluckResponse(args, resp)};
        if (!resp) {
            // break out
            return nullptr;
        }
        server_stat_->outqueue_wait_time_costs_ += queue_wait_time_ms;
        server_stat_->outqueue_wait_time_costs_count_++;

        if (!args) {
            delete resp;
            phxrpc::log(LOG_ERR, "%s data_flow_args nullptr", __func__);

            continue;
        }

        phxrpc::DataFlowArgs *data_flow_args{(phxrpc::DataFlowArgs *)args};
        if (!data_flow_args) {
            delete resp;
            phxrpc::log(LOG_ERR, "%s data_flow_args nullptr", __func__);

            continue;
        }

        const auto &session(session_mgr_.GetSession(data_flow_args->session_id));
        if (!session || !session->active) {
            delete resp;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " GetSession err",
                        __func__, data_flow_args->session_id);

            continue;
        }

        session->resps.push(resp);

        return session->out_socket;
    }

    return nullptr;
}

void EventLoopServerIO::UThreadIFunc(const uint64_t session_id) {
    const auto &session(session_mgr_.GetSession(session_id));
    if (!session || !session->active) {
        phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " GetSession err",
                    __func__, session_id);

        return;
    }

    phxrpc::BaseMessageHandler *msg_handler(factory_->Create(*(session->in_stream)));
    if (!msg_handler) {
        phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " msg_handler_factory.Create err, "
                    "client closed or no msg handler accept", __func__, session_id);

        // client closed or no msg handler accept

        // a client disconnection is signalled by a EOF condition on the file descriptor.
        // the system considers EOF to be a state in which the file descriptor is 'readable'.
        // read returns 0 bytes read.
        // should close fd to prevent from being epolled again.

        session_mgr_.DestroySession(session_id);

        return;
    }

    while (session->active) {
        server_stat_->io_read_requests_++;

        // will be deleted by worker
        phxrpc::BaseRequest *req{nullptr};
        phxrpc::ReturnCode ret{msg_handler->ServerRecv(*(session->in_stream), req)};
        if (phxrpc::ReturnCode::ERROR_STREAM_NOT_GOOD == ret) {
            // client closed
            if (req) {
                delete req;
            }
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " ServerRecv err client maybe closed idx %d",
                        __func__, session_id, idx_);

            break;
        }

        phxrpc::log(LOG_DEBUG, "%s session_id %" PRIx64 " ServerRecv ret %d idx %d",
                    __func__, session_id, static_cast<int>(ret), idx_);
        if (phxrpc::ReturnCode::OK != ret) {
            delete req;
            server_stat_->io_read_fails_++;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " read request err",
                        __func__, session_id);

            break;
        }

        server_stat_->io_read_bytes_ += req->size();

        if (!data_flow_->CanPushRequest(config_->GetMaxQueueLength())) {
            delete req;
            server_stat_->queue_full_rejected_after_accepted_fds_++;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " overflow can't enqueue",
                        __func__, session_id);

            break;
        }

        if (!server_qos_->CanEnqueue()) {
            // fast reject don't cal rpc_time_cost;
            delete req;
            server_stat_->enqueue_fast_rejects_++;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " fast reject can't enqueue",
                        __func__, session_id);

            break;
        }

        server_stat_->inqueue_push_requests_++;
        phxrpc::DataFlowArgs *data_flow_args{new phxrpc::DataFlowArgs};
        if (!data_flow_args) {
            delete req;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " data_flow_args nullptr",
                        __func__, session_id);

            break;
        }

        ret = msg_handler->GenResponse(data_flow_args->resp);
        if (phxrpc::ReturnCode::OK != ret) {
            delete req;
            phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " GenResponse err %d",
                        __func__, session_id, static_cast<int>(ret));

            break;
        }

        data_flow_args->session_id = session->session_id;
        data_flow_args->session_mgr = &session_mgr_;

        // if have enqueue, request will be deleted after pop.
        data_flow_->PushRequest(data_flow_args, req);
        // if is uthread worker mode, need notify.
        // req deleted by worker after this line
        worker_pool_->NotifyEpoll();
    }

    session->active = false;
    session_mgr_.DestroySession(session_id);
}

void EventLoopServerIO::UThreadOFunc(const uint64_t session_id) {
    const auto &session(session_mgr_.GetSession(session_id));
    if (!session || !session->active) {
        phxrpc::log(LOG_ERR, "%s session_id %" PRIx64 " GetSession err", __func__, session_id);

        return;
    }

    while (session->active) {
        while (!session->resps.empty()) {
            if (!session->active) break;

            unique_ptr<phxrpc::BaseResponse> resp(session->resps.front());
            session->resps.pop();

            if (!resp->fake()) {
                server_stat_->io_write_responses_++;

                phxrpc::ReturnCode ret{resp->Send(*(session->out_stream))};
                if (phxrpc::ReturnCode::OK != ret) {
                    server_stat_->io_write_fails_++;
                    phxrpc::log(LOG_ERR, "%s Send err %d session_id %" PRIx64, __func__,
                                static_cast<int>(ret), session_id);
                } else {
                    phxrpc::log(LOG_DEBUG, "%s session_id %" PRIx64 " Send ret %d idx %d",
                                __func__, session_id, static_cast<int>(ret), idx_);
                }
                server_stat_->io_write_bytes_ += resp->size();
            }
        }
        UThreadWait(*(session->out_socket), config_->GetSocketTimeoutMS());
    }

    session->active = false;
    session_mgr_.DestroySession(session_id);
}

void EventLoopServerIO::RunForever() {
    scheduler_->SetHandlerAcceptedFdFunc(bind(&EventLoopServerIO::HandlerAcceptedFd, this));
    scheduler_->SetActiveSocketFunc(bind(&EventLoopServerIO::ActiveSocketFunc, this));
    scheduler_->RunForever();
}


EventLoopServerUnit::EventLoopServerUnit(const int idx,
        EventLoopServer *const event_loop_server,
        const int worker_thread_count,
        const int worker_uthread_count_per_thread,
        const int worker_uthread_stack_size,
        phxrpc::Dispatch_t dispatch, void *const args,
        phxrpc::BaseMessageHandlerFactory *const factory)
        : server_(event_loop_server),
#ifndef __APPLE__
          scheduler_(8 * 1024, 1000000, false),
#else
          scheduler_(32 * 1024, 1000000, false),
#endif
          worker_pool_(idx, &scheduler_, server_->config_,
                       worker_thread_count, worker_uthread_count_per_thread,
                       worker_uthread_stack_size, this,
                       &data_flow_, &server_->server_stat_, dispatch, args),
          server_io_(idx, &scheduler_, server_->config_, &data_flow_,
                     &server_->server_stat_, &server_->server_qos_,
                     &worker_pool_, factory),
          thread_(&EventLoopServerUnit::RunFunc, this) {
}

EventLoopServerUnit::~EventLoopServerUnit() {
    thread_.join();
}

void EventLoopServerUnit::RunFunc() {
    server_io_.RunForever();
}

bool EventLoopServerUnit::AddAcceptedFd(const int accepted_fd) {
    return server_io_.AddAcceptedFd(accepted_fd);
}

void EventLoopServerUnit::SendResponse(void *const args, phxrpc::BaseResponse *const resp) {
    data_flow_.PushResponse(args, resp);
    //server_io_.server_stat_.outqueue_push_responses_++;
}


EventLoopServerAcceptor::EventLoopServerAcceptor(EventLoopServer *event_loop_server)
        : server_(event_loop_server) {
}

EventLoopServerAcceptor::~EventLoopServerAcceptor() {
}

void EventLoopServerAcceptor::LoopAccept(const char *bind_ip, const int port) {
    int listen_fd{-1};
    if (!phxrpc::BlockTcpUtils::Listen(&listen_fd, bind_ip, port)) {
        printf("listen %s:%d err\n", bind_ip, port);
        exit(-1);
    }

    printf("listen %s:%d ok\n", bind_ip, port);

#ifndef __APPLE__
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    pid_t thread_id = 0;
    int ret{sched_setaffinity(thread_id, sizeof(mask), &mask)};
    if (ret != 0) {
        printf("sched_setaffinity err\n");
    }
#endif

    while (true) {
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        int accepted_fd{accept(listen_fd, (struct sockaddr *)&addr, &socklen)};
        if (accepted_fd >= 0) {
            if (!server_->server_qos_.CanAccept()) {
                server_->server_stat_.rejected_fds_++;
                phxrpc::log(LOG_ERR, "%s too many connection, reject accept, fd %d", __func__, accepted_fd);
                close(accepted_fd);
                continue;
            }

            idx_ %= server_->server_unit_list_.size();
            if (!server_->server_unit_list_[idx_++]->AddAcceptedFd(accepted_fd)) {
                server_->server_stat_.rejected_fds_++;
                phxrpc::log(LOG_ERR, "%s accept queue full, reject accept, fd %d", __func__, accepted_fd);
                close(accepted_fd);
                continue;
            }

            server_->server_stat_.accepted_fds_++;
        } else {
            server_->server_stat_.accept_fail_++;
        }
    }

    close(listen_fd);
}


EventLoopServer::EventLoopServer(const EventLoopServerConfig &config,
                                 const phxrpc::Dispatch_t &dispatch, void *args,
                                 phxrpc::BaseMessageHandlerFactory *const factory)
        : config_(&config),
          server_monitor_(phxrpc::MonitorFactory::GetFactory()->
                          CreateServerMonitor(config.GetPackageName())),
          server_stat_(&config, server_monitor_),
          server_qos_(&config, &server_stat_),
          server_acceptor_(this) {
    size_t io_count = (size_t)config.GetIOThreadCount();
    size_t worker_thread_count = (size_t)config.GetMaxThreads();
    assert(worker_thread_count > 0);
    if (worker_thread_count < io_count) {
        io_count = worker_thread_count;
    }

    int worker_uthread_stack_size = config.GetWorkerUThreadStackSize();
    size_t worker_thread_count_per_io = worker_thread_count / io_count;
    for (size_t i{0}; i < io_count; ++i) {
        if (i == io_count - 1) {
            worker_thread_count_per_io = worker_thread_count - (worker_thread_count_per_io * (io_count - 1));
        }
        auto server_unit =
            new EventLoopServerUnit(i, this, (int)worker_thread_count_per_io,
                    config.GetWorkerUThreadCount(), worker_uthread_stack_size,
                    dispatch, args, factory);
        assert(server_unit != nullptr);
        server_unit_list_.push_back(server_unit);
    }
    printf("server already started, %zu io threads %zu workers\n", io_count, worker_thread_count);
    if (config.GetWorkerUThreadCount() > 0) {
        printf("server in uthread mode, %d uthread per worker\n", config.GetWorkerUThreadCount());
    }
}

EventLoopServer::~EventLoopServer() {
    for (auto &server_unit : server_unit_list_) {
        delete server_unit;
    }
}

void EventLoopServer::RunForever() {
    server_acceptor_.LoopAccept(config_->GetBindIP(), config_->GetPort());
}

void EventLoopServer::SendResponse(const uint64_t session_id, phxrpc::BaseResponse *resp) {
    // push to server unit outqueue
    int server_unit_idx{Session::GetServerUnitIdx(session_id)};
    phxrpc::DataFlowArgs *data_flow_args{new phxrpc::DataFlowArgs};
    data_flow_args->session_id = session_id;
    // forward req and do not delete here
    server_unit_list_[server_unit_idx]->SendResponse(data_flow_args, (phxrpc::BaseResponse *)resp);
}

