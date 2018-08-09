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

#include "phxrpc/rpc.h"

#include "event_loop_server_config.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


class Session final {
  public:
    static int GetServerUnitIdx(const uint64_t session_id);

    Session();
    ~Session();

    bool active{true};
    uint64_t session_id{0uLL};
    // not use unique_ptr because socket is own by stream
    phxrpc::UThreadSocket_t *in_socket{nullptr};
    phxrpc::UThreadSocket_t *out_socket{nullptr};
    std::unique_ptr<phxrpc::UThreadTcpStream> in_stream;
    std::unique_ptr<phxrpc::UThreadTcpStream> out_stream;
    std::queue<phxrpc::BaseResponse *> resps;
};


class SessionMgr final {
  public:
    SessionMgr(const int idx, phxrpc::UThreadEpollScheduler *const scheduler,
               const EventLoopServerConfig *config, phxrpc::HshaServerStat *server_stat);
    ~SessionMgr();

    std::shared_ptr<Session> CreateSession(const int fd);
    std::shared_ptr<Session> GetSession(const uint64_t session_id);
    void DestroySession(const uint64_t session_id);

  private:
    static std::atomic_uint32_t s_seq;

    int idx_{-1};
    phxrpc::UThreadEpollScheduler *scheduler_{nullptr};
    const EventLoopServerConfig *config_{nullptr};
    phxrpc::HshaServerStat *server_stat_{nullptr};
    std::map<uint64_t, std::shared_ptr<Session>> session_id2session_map_;
};


class EventLoopServerIO final {
  public:
    EventLoopServerIO(const int idx, phxrpc::UThreadEpollScheduler *const scheduler,
               const EventLoopServerConfig *config, phxrpc::DataFlow *data_flow,
               phxrpc::HshaServerStat *server_stat, phxrpc::HshaServerQos *server_qos,
               phxrpc::WorkerPool *worker_pool,
               phxrpc::BaseMessageHandlerFactoryCreateFunc msg_handler_factory_create_func);
    ~EventLoopServerIO();

    void RunForever();
    bool AddAcceptedFd(const int accepted_fd);
    void HandlerAcceptedFd();
    phxrpc::UThreadSocket_t *ActiveSocketFunc();
    void UThreadIFunc(const uint64_t session_id);
    void UThreadOFunc(const uint64_t session_id);
    void DestroySession(const uint64_t session_id);

  private:
    int idx_{-1};
    phxrpc::UThreadEpollScheduler *scheduler_{nullptr};
    const EventLoopServerConfig *config_{nullptr};
    phxrpc::DataFlow *data_flow_{nullptr};
    phxrpc::HshaServerStat *server_stat_{nullptr};
    phxrpc::HshaServerQos *server_qos_{nullptr};
    phxrpc::WorkerPool *worker_pool_{nullptr};
    std::unique_ptr<phxrpc::BaseMessageHandlerFactory> msg_handler_factory_;
    SessionMgr session_mgr_;
    std::queue<int> accepted_fd_list_;
    std::mutex queue_mutex_;
};


class EventLoopServer;

class EventLoopServerUnit {
  public:
    EventLoopServerUnit(const int idx,
                        EventLoopServer *const event_loop_server,
                        const int worker_thread_count,
                        const int worker_uthread_count_per_thread,
                        const int worker_uthread_stack_size,
                        phxrpc::Dispatch_t dispatch, void *const args);
    virtual ~EventLoopServerUnit();

    void RunFunc();
    bool AddAcceptedFd(const int accepted_fd);
    int SendResponse(const uint64_t session_id, phxrpc::BaseResponse *const resp);
    void DestroySession(const uint64_t session_id);

  private:
    EventLoopServer *server_{nullptr};
    phxrpc::UThreadEpollScheduler scheduler_;
    phxrpc::DataFlow data_flow_;
    phxrpc::WorkerPool worker_pool_;
    EventLoopServerIO server_io_;

    std::thread thread_;
};


class EventLoopServerAcceptor final {
  public:
    EventLoopServerAcceptor(EventLoopServer *event_loop_server);
    ~EventLoopServerAcceptor();

    void LoopAccept(const char *bind_ip, const int port);

  private:
    EventLoopServer *server_{nullptr};
    size_t idx_{0};
};


class EventLoopServer {
  public:
    EventLoopServer(const EventLoopServerConfig &config,
                    const phxrpc::Dispatch_t &dispatch, void *args,
                    phxrpc::BaseMessageHandlerFactoryCreateFunc msg_handler_factory_create_func);
    virtual ~EventLoopServer();

    void RunForever();

    int SendResponse(const uint64_t session_id, phxrpc::BaseResponse *const resp);
    void DestroySession(const uint64_t session_id);

  private:
    friend class EventLoopServerAcceptor;
    friend class EventLoopServerUnit;

    const EventLoopServerConfig *config_{nullptr};
    phxrpc::BaseMessageHandlerFactoryCreateFunc msg_handler_factory_create_func_;
    phxrpc::ServerMonitorPtr server_monitor_;
    phxrpc::HshaServerStat server_stat_;
    phxrpc::HshaServerQos server_qos_;
    EventLoopServerAcceptor server_acceptor_;

    std::vector<EventLoopServerUnit *> server_unit_list_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

