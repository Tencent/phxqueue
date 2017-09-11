#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <iostream>
#include <signal.h>

#include "phxqueue/test/simplehandler.h"
#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/consumer.h"
#include "phxqueue_phxrpc/plugin.h"
#include "phxqueue_phxrpc/test/echo_handler.h"

#include "phxrpc/rpc.h"


using namespace std;


extern char *program_invocation_short_name;

void ShowUsage(const char *program) {
    printf("\n");
    printf("Usage: %s [-c <config>] [-d] [-v]\n", program);
    printf("\n");

    exit(0);
}

int main(int argc, char ** argv) {

    const char *module_name{program_invocation_short_name};

    const char *config_file{nullptr};
    bool daemonize{false};
    extern char *optarg;
    int c ;
    while ((c = getopt(argc, argv, "c:vd")) != EOF) {
        switch (c) {
            case 'c': config_file = optarg; break;
            case 'd': daemonize = true; break;

            case 'v':
            default: ShowUsage(argv[0]); break;
        }
    }

    if (daemonize) phxrpc::ServerUtils::Daemonize();

    assert(signal(SIGPIPE, SIG_IGN) != SIG_ERR);

    // set customize log / monitor
    phxrpc::setvlog(phxqueue::comm::LogFuncForPhxRpc);

    if (nullptr == config_file) ShowUsage(argv[0]);


    phxqueue_phxrpc::consumer::ConsumerServerConfig config(config_file);
    if (!config.LoadIfModified()) {
        printf("ERR: ConsumerServerConfig::LoadIfModified fail. config_file %s", config_file);
        ShowUsage(argv[0]);
    }

    phxqueue::comm::LogFunc log_func;
    phxqueue::plugin::LoggerGoogle::GetLogger(module_name, config.GetProto().log().path(),
                                              config.GetProto().log().level(), log_func);
    phxqueue::comm::Logger::GetInstance()->SetLogFunc(log_func);

    phxqueue::consumer::ConsumerOption opt;
    opt.topic = config.GetProto().consumer().topic();
    opt.ip = config.GetProto().consumer().ip();
    opt.port = config.GetProto().consumer().port();
    opt.nprocs = config.GetProto().consumer().nproc();
    opt.proc_pid_path = config.GetProto().consumer().proc_pid_path();
    opt.lock_path_base = config.GetProto().consumer().lock_path_base();
    opt.use_store_master_client_on_get = 1;
    opt.use_store_master_client_on_add = 1;


    string phxqueue_global_config_path(config.GetProto().consumer().phxqueue_global_config_path());
    opt.config_factory_create_func =
            [phxqueue_global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
                return unique_ptr<phxqueue::plugin::ConfigFactory>(
                        new phxqueue_phxrpc::plugin::ConfigFactory(phxqueue_global_config_path));
            };

    const int simple_handle_id{1};
    const int echo_handle_id{2};
    phxqueue_phxrpc::consumer::Consumer consumer(opt);
    consumer.AddHandlerFactory(simple_handle_id,
            new phxqueue::comm::DefaultHandlerFactory<phxqueue::test::SimpleHandler>());
    consumer.AddHandlerFactory(echo_handle_id,
            new phxqueue::comm::DefaultHandlerFactory<phxqueue_phxrpc::test::EchoHandler>());
    consumer.Run();

    phxrpc::closelog();

    return 0;
}

