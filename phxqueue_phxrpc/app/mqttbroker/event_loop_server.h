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

#pragma once

#include <netinet/in.h>
#include <vector>

#include "phxrpc/rpc.h"

#include "event_loop_server_config.h"


struct RetainMessage final {
    std::string topic_name;
    std::string content;
    uint32_t qos{0u};
};


class SessionAttribute {
  public:
    bool IsExpired();
    void set_expire_time_ms(const uint64_t expire_time_ms);

    std::string client_id;
    uint32_t keep_alive{10};

  private:
    uint64_t expire_time_ms_{0uLL};
};

struct SessionContext {
    uint64_t session_id{0uL};
    bool init_session{false};
    bool heartbeat_session{false};
    bool destroy_session{false};
    SessionAttribute session_attribute;
};


class Session final {
  public:
    void Heartbeat();
    bool IsExpired();
    uint64_t expire_time_ms() { return expire_time_ms_; }

    uint64_t session_id{0uLL};
    int fd{-1};
    std::unique_ptr<phxrpc::BlockTcpStream> stream;
    SessionAttribute session_attribute;
    std::vector<RetainMessage> retain_messages;

  private:
    uint64_t expire_time_ms_{0uLL};
};


class SessionManager final {
  public:
    SessionManager(const int idx);
    ~SessionManager();

    Session *Create(const int fd);
    Session *GetByClientId(const std::string &client_id);
    Session *GetBySessionId(const uint64_t session_id);
    Session *GetByFd(const int fd);
    void DeleteBySessionId(const uint64_t session_id);

  private:
    static std::atomic_uint32_t s_session_num;

    int idx_{-1};
    std::list<Session> sessions_;
};


class SessionRouter final {
  public:
    void Add(const uint64_t session_id, const int idx);
    int Get(const uint64_t session_id) const;
    void Delete(const uint64_t session_id);

  private:
    mutable std::mutex mutex_;
    std::map<uint64_t, int> session_id2thread_index_map_;
};


class EventLoopServerIO final {
  public:
    EventLoopServerIO(const int idx, const int max_epoll_events,
                      const EventLoopServerConfig *config, phxrpc::DataFlow *data_flow,
                      phxrpc::HshaServerStat *server_stat, phxrpc::HshaServerQos *server_qos,
                      phxrpc::WorkerPool *worker_pool, SessionManager *session_mgr,
                      SessionRouter *session_router,
                      phxrpc::BaseMessageHandlerFactory *const factory);
    ~EventLoopServerIO();

    void RunForever();
    void OutThreadRunFunc();
    bool AddAcceptedFd(const int accepted_fd);
    int AddEpoll(const int events, const int fd);
    int DelEpoll(const int fd);
    void InFunc(const int fd);
    void OutFunc(void *args, phxrpc::BaseResponse *resp);
    //int SetNonBlock(const int fd, const bool flag);
    //int SetNoDelay(const int fd, const bool flag);

  private:
    int idx_{-1};
    int epoll_fd_{-1};
    int max_epoll_events_{};
    const EventLoopServerConfig *config_{nullptr};
    phxrpc::DataFlow *data_flow_{nullptr};
    phxrpc::HshaServerStat *server_stat_{nullptr};
    phxrpc::HshaServerQos *server_qos_{nullptr};
    phxrpc::WorkerPool *worker_pool_{nullptr};
    SessionManager *session_mgr_{nullptr};
    SessionRouter *session_router_{nullptr};
    phxrpc::BaseMessageHandlerFactory *factory_{nullptr};
    std::thread out_thread_;
};


class EventLoopServer;

class EventLoopServerUnit : public phxrpc::BaseServerUnit {
  public:
    EventLoopServerUnit(const int idx,
            EventLoopServer *const event_loop_server,
            int worker_thread_count,
            int worker_uthread_count_per_thread,
            int worker_uthread_stack_size,
            phxrpc::Dispatch_t dispatch, void *const args,
            SessionRouter *session_router, phxrpc::BaseMessageHandlerFactory *const factory);
    virtual ~EventLoopServerUnit() override;

    void RunFunc();
    bool AddAcceptedFd(const int accepted_fd);
    void SendResponse(void *const args, phxrpc::BaseResponse *const resp);

  private:
    EventLoopServer *event_loop_server_{nullptr};
    phxrpc::UThreadEpollScheduler scheduler_;
    SessionManager session_mgr_;
    phxrpc::WorkerPool worker_pool_;
    EventLoopServerIO server_io_;
    std::thread thread_;
};


class EventLoopServer;

class EventLoopServerAcceptor final {
  public:
    EventLoopServerAcceptor(EventLoopServer *server);
    ~EventLoopServerAcceptor();

    void LoopAccept(const char *bind_ip, const int port);
    int Listen(const char *const ip, uint16_t port, int &listen_fd);

  private:
    EventLoopServer *server_{nullptr};
    size_t idx_{0};
};


class EventLoopServer : public phxrpc::BaseServer {
  public:
    EventLoopServer(const EventLoopServerConfig &config,
                    const phxrpc::Dispatch_t &dispatch, void *args,
                    phxrpc::BaseMessageHandlerFactory *const factory);
    virtual ~EventLoopServer();

    virtual void RunForever() override;
    void SendResponse(const uint64_t session_id, phxrpc::BaseResponse *const resp);

  private:
    friend class EventLoopServerAcceptor;
    friend class EventLoopServerUnit;

    const EventLoopServerConfig *config_{nullptr};
    phxrpc::ServerMonitorPtr server_monitor_;
    phxrpc::HshaServerStat server_stat_;
    phxrpc::HshaServerQos server_qos_;
    EventLoopServerAcceptor server_acceptor_;
    std::vector<EventLoopServerUnit *> server_unit_list_;
    SessionRouter session_router_;
    std::thread accept_thread_;
};

