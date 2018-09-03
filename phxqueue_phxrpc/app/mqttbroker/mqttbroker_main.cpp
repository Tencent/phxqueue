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

#include <iostream>
#include <memory>
#include <signal.h>
#include <unistd.h>

#include "phxrpc/file.h"
#include "phxrpc/http.h"
#include "phxrpc/msg.h"
#include "phxrpc/rpc.h"

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"

#include "phxqueue_phxrpc/plugin.h"

#include "event_loop_server.h"
#include "mqtt/mqtt_msg_handler_factory.h"
#include "mqtt/mqtt_packet_id.h"
#include "mqtt/mqtt_session.h"
#include "mqttbroker_server_config.h"
#include "mqttbroker_service_impl.h"
#include "phxrpc_mqttbroker_dispatcher.h"
#include "publish/publish_mgr.h"
#include "publish/publish_memory.h"
#include "server_mgr.h"


using namespace std;


static phxqueue::comm::LogFunc g_log_func = nullptr;


static int MakeArgs(ServiceArgs_t &args, const MqttBrokerServerConfig &config) {
    args.config = &config;

    phxqueue::comm::Logger::GetInstance()->SetLogFunc(g_log_func);

    string phxqueue_global_config_path(config.phxqueue_global_config_path());
    phxqueue_phxrpc::plugin::ConfigFactory::SetConfigFactoryCreateFunc(
            [phxqueue_global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
                return unique_ptr<phxqueue::plugin::ConfigFactory>(
                        new phxqueue_phxrpc::plugin::ConfigFactory(phxqueue_global_config_path));
            }
    );

    return 0;
}


void Dispatch(const phxrpc::BaseRequest &req,
              phxrpc::BaseResponse *const resp,
              phxrpc::DispatcherArgs_t *const args) {
    ServiceArgs_t *service_args{(ServiceArgs_t *)(args->service_args)};

    MqttBrokerServiceImpl service(*service_args, args->server_worker_uthread_scheduler,
            *(uint64_t *)args->data_flow_args);
    MqttBrokerDispatcher dispatcher(service, args);

    phxrpc::BaseDispatcher<MqttBrokerDispatcher> base_dispatcher(
            dispatcher, MqttBrokerDispatcher::GetURIFuncMap());
    if (!base_dispatcher.Dispatch(req, resp)) {
        resp->SetFake(phxrpc::BaseResponse::FakeReason::DISPATCH_ERROR);
    }
}

void ShowUsage(const char *program) {
    printf("\n");
    printf("Usage: %s [-c <config>] [-d] [-l <log level>] [-v]\n", program);
    printf("\n");

    exit(0);
}

int main(int argc, char **argv) {
    const char *config_file{nullptr};
    bool daemonize{false};
    int log_level{-1};
    extern char *optarg;
    int c;
    while (EOF != (c = getopt(argc, argv, "c:vl:d"))) {
        switch (c) {
            case 'c': config_file = optarg; break;
            case 'd': daemonize = true; break;
            case 'l': log_level = atoi(optarg); break;

            case 'v':
            default: ShowUsage(argv[0]); break;
        }
    }

    if (daemonize) phxrpc::ServerUtils::Daemonize();

    assert(signal(SIGPIPE, SIG_IGN) != SIG_ERR);

    // set customize log / monitor
    phxrpc::setvlog(phxqueue::comm::LogFuncForPhxRpc);

    if (nullptr == config_file) ShowUsage(argv[0]);

    MqttBrokerServerConfig config;
    if (!config.Read(config_file)) ShowUsage(argv[0]);

    if (log_level > 0) config.GetHshaServerConfig().SetLogLevel(log_level);

    //phxqueue::plugin::LoggerSys::GetLogger(program_invocation_short_name,
    //                                       config.GetHshaServerConfig().GetLogLevel(),
    //                                       daemonize, g_log_func);  // syslog
    phxqueue::plugin::LoggerGoogle::GetLogger(program_invocation_short_name,
                                              config.GetHshaServerConfig().GetLogDir(),
                                              config.GetHshaServerConfig().GetLogLevel(), g_log_func);  // glog

    // init instance
    phxqueue_phxrpc::mqttbroker::ServerMgr::SetInstance(
            new phxqueue_phxrpc::mqttbroker::ServerMgr(&(config.GetHshaServerConfig())));
    phxqueue_phxrpc::mqttbroker::MqttSessionMgr::GetInstance();
    phxqueue_phxrpc::mqttbroker::MqttPacketIdMgr::GetInstance();
    phxqueue_phxrpc::mqttbroker::PublishQueue::SetInstance(
            new phxqueue_phxrpc::mqttbroker::PublishQueue(config.max_publish_queue_size()));
    phxqueue_phxrpc::mqttbroker::PublishLruCache::SetInstance(
            new phxqueue_phxrpc::mqttbroker::PublishLruCache(config.max_publish_lru_cache_size()));
    phxqueue_phxrpc::mqttbroker::PublishMgr::SetInstance(
            new phxqueue_phxrpc::mqttbroker::PublishMgr(&config));

    ServiceArgs_t service_args;
    int ret{MakeArgs(service_args, config)};
    if (0 != ret) {
        printf("ERR: MakeArgs ret %d\n", ret);

        exit(-1);
    }

    phxrpc::HshaServer hsha_server(config.GetHshaServerConfig(), Dispatch, &service_args);
    phxqueue_phxrpc::mqttbroker::EventLoopServer event_loop_server(
            config.GetEventLoopServerConfig(), Dispatch, &service_args,
            []()->unique_ptr<phxqueue_phxrpc::mqttbroker::MqttMessageHandlerFactory> {
        return unique_ptr<phxqueue_phxrpc::mqttbroker::MqttMessageHandlerFactory>(
                new phxqueue_phxrpc::mqttbroker::MqttMessageHandlerFactory);
    });
    phxqueue_phxrpc::mqttbroker::ServerMgr::GetInstance()->set_hsha_server(&hsha_server);
    phxqueue_phxrpc::mqttbroker::ServerMgr::GetInstance()->set_event_loop_server(&event_loop_server);

    thread hsha_thread([](phxrpc::HshaServer *const server) {
        server->RunForever();
    }, &hsha_server);
    thread event_loop_thread([](phxqueue_phxrpc::mqttbroker::EventLoopServer *const server) {
        server->RunForever();
    }, &event_loop_server);
    event_loop_thread.join();
    hsha_thread.join();

    phxrpc::closelog();

    return 0;
}

