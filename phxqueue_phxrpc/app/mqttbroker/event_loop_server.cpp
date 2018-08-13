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

#include <arpa/inet.h>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __APPLE__
#include "plugin_darwin/network/epoll-darwin.h"
#else
#include <sys/epoll.h>
#endif

#include "mqtt/mqtt_msg_handler.h"


using namespace std;


namespace {


const int EPOLL_TIMEOUT{-1};


}  // namespace


bool SessionAttribute::IsExpired() {
    return expire_time_ms_ <= phxrpc::Timer::GetSteadyClockMS();
}

void SessionAttribute::set_expire_time_ms(const uint64_t expire_time_ms) {
    expire_time_ms_ = expire_time_ms;
}


SessionManager::SessionManager(const int idx) : idx_(idx) {
}

SessionManager::~SessionManager() {
}

void Session::Heartbeat() {
    if (0 >= session_attribute.keep_alive) {
        expire_time_ms_ = -1;
    } else {
        expire_time_ms_ = session_attribute.keep_alive * 1000 + phxrpc::Timer::GetSteadyClockMS();
    }
}

bool Session::IsExpired() {
    return expire_time_ms_ <= phxrpc::Timer::GetSteadyClockMS();
}


Session *SessionManager::Create(const int fd) {
    Session session;
    uint64_t idx_part{static_cast<uint64_t>(idx_) & 0xFFFF};
    uint64_t time_part{phxrpc::Timer::GetTimestampMS() & 0xFFFF};
    uint64_t inc_part{static_cast<uint64_t>(++s_session_num)};
    session.session_id = (idx_part << 48) | (time_part << 32) | inc_part;
    session.fd = fd;
    session.stream.reset(new phxrpc::BlockTcpStream);
    session.stream->Attach(session.fd);
    session.stream->exceptions(ios::failbit | ios::badbit);

    sessions_.emplace_back(move(session));

    return &(sessions_.back());
}

Session *SessionManager::GetByClientId(const string &client_id) {
    for (auto &&session : sessions_) {
        if (session.session_attribute.client_id == client_id)
            return &session;
    }

    return nullptr;
}

Session *SessionManager::GetBySessionId(const uint64_t session_id) {
    for (auto &&session : sessions_) {
        if (session.session_id == session_id)
            return &session;
    }

    return nullptr;
}

Session *SessionManager::GetByFd(const int fd) {
    for (auto &&session : sessions_) {
        if (session.fd == fd)
            return &session;
    }

    return nullptr;
}

void SessionManager::DeleteBySessionId(const uint64_t session_id) {
    for (auto it(sessions_.begin()); sessions_.end() != it; ++it) {
        if (it->session_id == session_id) {
            sessions_.erase(it);

            return;
        }
    }
}

atomic_uint32_t SessionManager::s_session_num{0};


void SessionRouter::Add(const uint64_t session_id, const int idx) {
    lock_guard<mutex> lock(mutex_);
    session_id2thread_index_map_[session_id] = idx;
}

int SessionRouter::Get(const uint64_t session_id) const {
    lock_guard<mutex> lock(mutex_);
    const auto &it(session_id2thread_index_map_.find(session_id));
    if (session_id2thread_index_map_.end() == it) {
        return -1;
    }

    return it->second;
}

void SessionRouter::Delete(const uint64_t session_id) {
    lock_guard<mutex> lock(mutex_);
    session_id2thread_index_map_.erase(session_id);
}


EventLoopServerIO::EventLoopServerIO(const int idx, const int max_epoll_events,
                                     const EventLoopServerConfig *config, phxrpc::DataFlow *data_flow,
                                     phxrpc::HshaServerStat *server_stat, phxrpc::HshaServerQos *server_qos,
                                     phxrpc::WorkerPool *worker_pool, SessionManager *session_mgr,
                                     SessionRouter *session_router,
                                     phxrpc::BaseMessageHandlerFactory *const factory)
        : idx_(idx), max_epoll_events_(max_epoll_events), config_(config), data_flow_(data_flow),
          server_stat_(server_stat), server_qos_(server_qos), worker_pool_(worker_pool),
          session_mgr_(session_mgr), session_router_(session_router), factory_(factory) {
    out_thread_ = thread(&EventLoopServerIO::OutThreadRunFunc, this);
}

EventLoopServerIO::~EventLoopServerIO() {
    out_thread_.join();
}

void EventLoopServerIO::RunForever() {
    epoll_fd_ = epoll_create(max_epoll_events_);
    if (-1 == epoll_fd_) {
        fprintf(stderr, "epoll_create err %d %s\n", errno, strerror(errno));

        assert(-1 != epoll_fd_);
    }

    struct epoll_event *events{
            (struct epoll_event *)calloc(max_epoll_events_, sizeof(struct epoll_event))};
    while (true) {
        int nfds{epoll_wait(epoll_fd_, events, max_epoll_events_, EPOLL_TIMEOUT)};
        if (-1 == nfds) {
            phxrpc::log(LOG_ERR, "epoll_wait err %d %s", errno, strerror(errno));

            continue;
        }

        for (int i{0}; i < nfds; ++i) {
            InFunc(events[i].data.fd);
        }
    }
    free(events);
}

void EventLoopServerIO::OutThreadRunFunc() {
    while (true) {
        if (data_flow_->CanPluckResponse()) {
            void *args{nullptr};
            phxrpc::BaseResponse *resp{nullptr};
            int queue_wait_time_ms{data_flow_->PluckResponse(args, resp)};
            if (!resp) {
                return;
            }
            server_stat_->outqueue_wait_time_costs_ += queue_wait_time_ms;
            server_stat_->outqueue_wait_time_costs_count_++;

            OutFunc(args, resp);
        }

        usleep(4);
    }
}

bool EventLoopServerIO::AddAcceptedFd(const int accepted_fd) {
    phxrpc::BaseTcpUtils::SetNonBlock(accepted_fd, true);
    phxrpc::BaseTcpUtils::SetNoDelay(accepted_fd, true);

    Session *session{session_mgr_->Create(accepted_fd)};
    if (!session) {
        phxrpc::log(LOG_ERR, "Create err fd %d", accepted_fd);
        close(accepted_fd);

        return false;
    }

    if (-1 == AddEpoll(EPOLLIN, accepted_fd)) {
        close(accepted_fd);

        return false;
    }

    return true;
}

int EventLoopServerIO::AddEpoll(const int events, const int fd) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd;
    int ret{epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev)};
    if (-1 == ret) {
        phxrpc::log(LOG_ERR, "epoll_ctl add err %d %s", errno, strerror(errno));
    }

    return ret;
}

int EventLoopServerIO::DelEpoll(const int fd) {
    struct epoll_event ev;
    int ret{epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, &ev)};
    if (-1 == ret) {
        phxrpc::log(LOG_ERR, "epoll_ctl del err %d %s", errno, strerror(errno));
    }

    return ret;
}

void EventLoopServerIO::InFunc(const int fd) {
    //char buf[1024];

    //ssize_t nr_read{read(fd, buf, sizeof(buf))};
    //switch (nr_read) {
    //case 0:
    //    fprintf(stderr, "fd %d closed\n", fd);
    //    break;
    //case -1:
    //    fprintf(stderr, "recv: %s\n", strerror(errno));
    //    break;
    //default:
    //    fprintf(stderr, "received %zd bytes\n", nr_read);
    //    fprintf(stderr, "%x\t%x\t%x\t%x\t%x\t%x\t%x\t%x\t%x\t%x\n",
    //            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9]);
    //    fprintf(stderr, "%c\t%c\t%c\t%c\t%c\t%c\t%c\t%c\t%c\t%c\n",
    //            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9]);
    //    break;
    //}

    //if (0 != nr_read)
    //    return;

    //write(fd, buf, nr_read);

    //return;

    const auto &session(session_mgr_->GetByFd(fd));
    if (!session) {
        phxrpc::log(LOG_ERR, "GetByFd err fd %d", fd);

        return;
    }

    phxrpc::BaseMessageHandler *msg_handler(factory_->Create(*(session->stream)));
    if (!msg_handler) {
        phxrpc::log(LOG_ERR, "GetProtocol err, client closed or no msg handler accept");

        // client closed or no msg handler accept

        // a client disconnection is signalled by a EOF condition on the file descriptor.
        // the system considers EOF to be a state in which the file descriptor is 'readable'.
        // read returns 0 bytes read.
        // should close fd to prevent from being epolled again.

        DelEpoll(fd);
        session_router_->Delete(session->session_id);
        session_mgr_->DeleteBySessionId(session->session_id);
        close(fd);

        return;
    }

    phxrpc::HshaServerStat::TimeCost time_cost;

    server_stat_->io_read_requests_++;

    // will be deleted by worker
    phxrpc::BaseRequest *req{nullptr};
    phxrpc::ReturnCode ret{msg_handler->ServerRecv(*(session->stream), req)};
    if (phxrpc::ReturnCode::ERROR_STREAM_NOT_GOOD == ret) {
        phxrpc::log(LOG_ERR, "ServerRecv err, client maybe closed");

        // client closed
        DelEpoll(fd);
        session_router_->Delete(session->session_id);
        session_mgr_->DeleteBySessionId(session->session_id);
        close(fd);

        server_stat_->hold_fds_--;

        return;
    }

    // TODO: remove
    printf("session_id %" PRIx64 " ServerRecv ret %d idx %d fd %d\n",
           session->session_id, static_cast<int>(ret), idx_, fd);
    if (phxrpc::ReturnCode::OK != ret) {
        delete req;
        server_stat_->io_read_fails_++;
        server_stat_->rpc_time_costs_count_++;
        server_stat_->rpc_time_costs_ += time_cost.Cost();
        phxrpc::log(LOG_ERR, "%s read request fail fd %d", __func__, fd);

        return;
    }

    server_stat_->io_read_bytes_ += req->GetContent().size();

    if (!data_flow_->CanPushRequest(config_->GetMaxQueueLength())) {
        delete req;
        server_stat_->queue_full_rejected_after_accepted_fds_++;

        return;
    }

    if (!server_qos_->CanEnqueue()) {
        // fast reject don't cal rpc_time_cost;
        delete req;
        server_stat_->enqueue_fast_rejects_++;
        phxrpc::log(LOG_ERR, "%s fast reject, can't enqueue fd %d",
                    __func__, fd);

        return;
    }

    // if have enqueue, request will be deleted after pop.
    //const bool is_keep_alive{0 != req->IsKeepAlive()};
    //const string version(req->GetVersion() != nullptr ? req->GetVersion() : "");

    server_stat_->inqueue_push_requests_++;
    SessionContext *context{new SessionContext};
    context->session_id = session->session_id;
    context->session_attribute.set_expire_time_ms(session->expire_time_ms());
    data_flow_->PushRequest((void *)context, req);
    // if is uthread worker mode, need notify.
    // req deleted by worker after this line
    worker_pool_->NotifyEpoll();
    //UThreadSetArgs(*socket, nullptr);

    //UThreadWait(*socket, config_->GetSocketTimeoutMS());
    //if (UThreadGetArgs(*socket) == nullptr) {
    //    // timeout
    //    server_stat_->worker_timeouts_++;
    //    server_stat_->rpc_time_costs_count_++;
    //    server_stat_->rpc_time_costs_ += time_cost.Cost();

    //    // because have enqueue, so socket will be closed after pop.
    //    socket = stream.DetachSocket();
    //    UThreadLazyDestory(*socket);

    //    phxrpc::log(LOG_ERR, "%s timeout, fd %d sockettimeoutms %d",
    //                __func__, fd, config_->GetSocketTimeoutMS());
    //    break;
    //}

    //server_stat_->io_write_responses_++;
    //{
    //    BaseResponse *resp((BaseResponse *)UThreadGetArgs(*socket));
    //    if (!resp->fake()) {
    //        ret = resp->ModifyResp(is_keep_alive, version);
    //        ret = resp->Send(stream);
    //        server_stat_->io_write_bytes_ += resp->GetContent().size();
    //    }
    //    delete resp;
    //}

    //server_stat_->rpc_time_costs_count_++;
    //server_stat_->rpc_time_costs_ += time_cost.Cost();

    //if (ReturnCode::OK != ret) {
    //    server_stat_->io_write_fails_++;
    //}

    //if (!is_keep_alive || (ReturnCode::OK != ret)) {
    //    break;
    //}
}

void EventLoopServerIO::OutFunc(void *args, phxrpc::BaseResponse *resp) {
    SessionContext *context{(SessionContext *)args};
    if (!context) {
        phxrpc::log(LOG_ERR, "context nullptr");
        delete resp;

        return;
    }

    // 1. update session
    const auto &session(session_mgr_->GetBySessionId(context->session_id));
    if (!session) {
        phxrpc::log(LOG_ERR, "GetBySessionId err session_id %" PRIx64, context->session_id);
        delete context;
        delete resp;

        return;
    }
    if (context->destroy_session) {
        // mqtt disconnect
        session_router_->Delete(session->session_id);
        session_mgr_->DeleteBySessionId(session->session_id);

        delete context;
        delete resp;

        return;
    }

    if (context->init_session) {
        // mqtt connect: if client_id exist, close old session
        const auto &old_session(session_mgr_->GetByClientId(
                context->session_attribute.client_id));
        if (old_session) {
            if (old_session->session_id == session->session_id) {
                // mqtt-3.1.0-2: disconnect current connection

                session_router_->Delete(session->session_id);
                session_mgr_->DeleteBySessionId(session->session_id);

                delete context;
                delete resp;

                return;
            } else {
                // mqtt-3.1.4-2: disconnect other connection with same client_id
                session_router_->Delete(old_session->session_id);
                session_mgr_->DeleteBySessionId(old_session->session_id);
            }
        }

        // mqtt connect: set client_id and init
        session->session_attribute = context->session_attribute;
        session->Heartbeat();
        session_router_->Add(session->session_id, idx_);
    }

    if (context->heartbeat_session) {
        // mqtt ping
        session->Heartbeat();
    }

    // 2. send response
    if (!resp->fake()) {
        phxrpc::ReturnCode ret{resp->Send(*(session->stream))};
        // TODO: remove
        printf("session_id %" PRIx64 " Send client_id \"%s\" ret %d idx %d\n",
               context->session_id, session->session_attribute.client_id.c_str(), ret, idx_);
        server_stat_->io_write_bytes_ += resp->GetContent().size();
    }
    delete context;
    delete resp;
}

//int EventLoopServerIO::SetNonBlock(const int fd, const bool flag) {
//    int temp{fcntl(fd, F_GETFL, 0)};
//    int ret{-1};
//    if (flag) {
//        ret = fcntl(fd, F_SETFL, temp | O_NONBLOCK);
//    } else {
//        ret = fcntl(fd, F_SETFL, temp & (~O_NONBLOCK));
//    }
//    if (-1 == ret) {
//        phxrpc::log(LOG_ERR, "fcntl nonblock err %d %s", errno, strerror(errno));
//
//        return ret;
//    }
//
//    return ret;
//}
//
//int EventLoopServerIO::SetNoDelay(const int fd, const bool flag) {
//    int temp{flag ? 1 : 0};
//    int ret{setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&temp, sizeof(temp))};
//    if (-1 == ret) {
//        phxrpc::log(LOG_ERR, "setsockopt nodelay err %d %s", errno, strerror(errno));
//
//        return ret;
//    }
//
//    return ret;
//}


EventLoopServerUnit::EventLoopServerUnit(
        const int idx,
        EventLoopServer *const event_loop_server,
        int worker_thread_count,
        int worker_uthread_count_per_thread,
        int worker_uthread_stack_size,
        phxrpc::Dispatch_t dispatch,
        void *const args, SessionRouter *session_router,
        phxrpc::BaseMessageHandlerFactory *const factory)
        : event_loop_server_(event_loop_server),
#ifndef __APPLE__
          scheduler_(8 * 1024, 1000000, false),
#else
          scheduler_(32 * 1024, 1000000, false),
#endif
          session_mgr_(idx),
          worker_pool_(idx, &scheduler_, event_loop_server_->config_,
                       worker_thread_count, worker_uthread_count_per_thread,
                       worker_uthread_stack_size, this,
                       &data_flow_, &event_loop_server_->server_stat_, dispatch, args),
          server_io_(idx, 1000000, event_loop_server_->config_, &data_flow_,
                     &event_loop_server_->server_stat_, &event_loop_server_->server_qos_,
                     &worker_pool_, &session_mgr_, session_router, factory),
          thread_(&EventLoopServerUnit::RunFunc, this)  {
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
    //server_io_.hsha_server_stat_.outqueue_push_responses_++;
}


EventLoopServerAcceptor::EventLoopServerAcceptor(EventLoopServer *server)
        : server_(server) {
}

EventLoopServerAcceptor::~EventLoopServerAcceptor() {
}

void EventLoopServerAcceptor::LoopAccept(const char *bind_ip, const int port) {
    int listen_fd{-1};
    int ret{Listen(bind_ip, port, listen_fd)};
    if (0 != ret) {
        fprintf(stderr, "listen err %d addr %s:%d\n", ret, bind_ip, port);

        assert(0 == ret);
    }

    printf("listen %s:%d ok\n", bind_ip, port);

#ifndef __APPLE__
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    pid_t thread_id = 0;
    ret = sched_setaffinity(thread_id, sizeof(mask), &mask);
    if (0 != ret) {
        fprintf(stderr, "sched_setaffinity err\n");
    }
#endif

    while (true) {
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        int accepted_fd{accept(listen_fd, (struct sockaddr *)&addr, &socklen)};
        if (-1 == accepted_fd) {
            server_->server_stat_.accept_fail_++;
            phxrpc::log(LOG_ERR, "%s accept err %d %s", __func__, errno, strerror(errno));
        } else {
            if (!server_->server_qos_.CanAccept()) {
                server_->server_stat_.rejected_fds_++;
                phxrpc::log(LOG_ERR, "%s too many connection reject accept fd %d", __func__, accepted_fd);
                close(accepted_fd);

                continue;
            }

            idx_ %= server_->server_unit_list_.size();
            if (!server_->server_unit_list_[idx_++]->AddAcceptedFd(accepted_fd)) {
                server_->server_stat_.rejected_fds_++;
                phxrpc::log(LOG_ERR, "%s accept queue full reject accept fd %d", __func__, accepted_fd);
                close(accepted_fd);

                continue;
            }

            server_->server_stat_.accepted_fds_++;
            server_->server_stat_.hold_fds_++;
        }
    }

    close(listen_fd);
}

int EventLoopServerAcceptor::Listen(const char *const ip, uint16_t port, int &listen_fd) {
    int temp_fd{socket(AF_INET, SOCK_STREAM, 0)};
    if (-1 == temp_fd) {
        phxrpc::log(LOG_ERR, "socket err %d %s", errno, strerror(errno));

        return -1;
    }

    int optval{1};
    if (-1 == setsockopt(temp_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval))) {
        phxrpc::log(LOG_WARNING, "setsockopt reuseaddr err %d %s", errno, strerror(errno));
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int ret{0};

    if ('\0' != *ip) {
        addr.sin_addr.s_addr = inet_addr(ip);
        if (INADDR_NONE == addr.sin_addr.s_addr) {
            phxrpc::log(LOG_CRIT, "inet_addr err ip %s", ip);

            ret = -1;
        }
    }

    if (0 == ret) {
        if (-1 == ::bind(temp_fd, (struct sockaddr *)&addr, sizeof(addr))) {
            phxrpc::log(LOG_ERR, "bind err %d %s", errno, strerror(errno));

            ret = -1;
        }
    }

    if (0 == ret) {
        if (-1 == listen(temp_fd, 1024)) {
            phxrpc::log(LOG_CRIT, "listen err %d %s", errno, strerror(errno));

            ret = -1;
        }
    }

    if (0 != ret && -1 != temp_fd)
        close(temp_fd);

    if (0 == ret) {
        listen_fd = temp_fd;
        phxrpc::log(LOG_NOTICE, "listen port %u", port);
    }

    return ret;
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
        auto hsha_server_unit =
            new EventLoopServerUnit(i, this, (int)worker_thread_count_per_io,
                    config.GetWorkerUThreadCount(), worker_uthread_stack_size,
                    dispatch, args, &session_router_, factory);
        assert(hsha_server_unit != nullptr);
        server_unit_list_.push_back(hsha_server_unit);
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

    accept_thread_.join();
}

void EventLoopServer::RunForever() {
    accept_thread_ = thread(&EventLoopServerAcceptor::LoopAccept, server_acceptor_,
                            config_->GetBindIP(), config_->GetPort());
}

void EventLoopServer::SendResponse(const uint64_t session_id, phxrpc::BaseResponse *resp) {
    // push to server unit outqueue
    int server_unit_idx{session_router_.Get(session_id)};
    SessionContext *context{new SessionContext};
    context->session_id = session_id;
    // forward req and do not delete here
    server_unit_list_[server_unit_idx]->SendResponse(context, (phxrpc::BaseResponse *)resp);
}

