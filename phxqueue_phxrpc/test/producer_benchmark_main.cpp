#include <iostream>
#include <signal.h>

#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/plugin.h"
#include "phxqueue_phxrpc/test/producer_benchmark.h"


using namespace std;


extern char *program_invocation_short_name;

void ShowUsage(const char *program) {
    printf("\n");
    printf("Usage: %s <qps> <nthread> <nroutine_per_thread> <buf_size> [<ndaemon_batch_thread>]\n", program);
    printf("\n");

    exit(0);
}

int main(int argc, char **argv) {
    const char *module_name{program_invocation_short_name};

    if (argc < 5) {
        ShowUsage(module_name);
    }

    const int qps{strtol(argv[1], nullptr, 10)};
    const int nthread{strtol(argv[2], nullptr, 10)};
    const int nroutine{strtol(argv[3], nullptr, 10)};
    const int buf_size{strtol(argv[4], nullptr, 10)};
    const int ndaemon_batch_thread{((argc > 5) ? strtol(argv[5], nullptr, 10) : 0)};

    printf("ndaemon_batch_thread %d\n", ndaemon_batch_thread);

    const string global_config_path("./etc/globalconfig.conf");

    phxqueue::plugin::ConfigFactory::SetConfigFactoryCreateFunc(
            [global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
                return unique_ptr<phxqueue::plugin::ConfigFactory>(
                        new phxqueue_phxrpc::plugin::ConfigFactory(global_config_path));
            });

    phxqueue_phxrpc::test::ProducerBenchMark bm(qps, nthread, nroutine, buf_size, ndaemon_batch_thread);
    bm.Run();
    //bm.TaskFunc(1);

    return 0;
}

