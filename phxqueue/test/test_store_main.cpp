#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/store.h"
#include "phxqueue/plugin.h"


using namespace phxqueue;
using namespace std;


void TestAdd(store::Store &store) {
    NLVerb("begin");

    comm::RetCode ret;

    const int topic_id = 1000;
    const int store_id = 1;
    const int queue_id = 0;
    const int pub_id = 1;
    const int handle_id = 1;
    const uint64_t uin = 123;
    const string buffer = "123";
    const int buffer_type = 0;
    const uint64_t sub_ids = 3;

    comm::proto::AddRequest req;
    comm::proto::AddResponse resp;
    req.set_topic_id(topic_id);
    req.set_store_id(store_id);
    req.set_queue_id(queue_id);

    auto &&item = req.add_items();
    auto &&meta = item->mutable_meta();
    meta->set_topic_id(topic_id);
    meta->set_handle_id(handle_id);
    meta->set_uin(uin);
    item->set_buffer(buffer);
    item->set_buffer_type(buffer_type);
    item->set_sub_ids(sub_ids);
    item->set_pub_id(pub_id);
    item->set_atime(time(nullptr));

    if (comm::RetCode::RET_OK != (ret = store.Add(req, resp))) {
        NLErr("Store Add ret %d", as_integer(ret));
        return;
    }

    NLVerb("cursor_id %" PRIu64, (uint64_t)resp.cursor_id());
}

void StoreRun(const int vpid) {
    comm::RetCode ret;

    auto proc_name = string("test_store.") + to_string(vpid);

    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger(proc_name, "/tmp/phxqueue/log", 3, log_func);
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    config::StoreConfig store_config;
    store_config.Load();

    vector<shared_ptr<const config::proto::Store> > stores;
    if (comm::RetCode::RET_OK != (ret = store_config.GetAllStore(stores))) {
        NLErr("GetAllStore ret %d", as_integer(ret));
        return;
    }
    if (0 == stores.size()) {
        NLErr("stores.size 0");
        return;
    }
    if (3 != stores[0]->addrs_size()) {
        NLErr("stores[0]->addrs_size %zu != 3", stores[0]->addrs_size());
        return;
    }

    auto path_base = string("/tmp/phxqueue/store.") + to_string(vpid);

    store::StoreOption opt;
    opt.topic = "test";
    opt.data_dir_path = path_base;
    opt.ip = stores[0]->addrs(vpid).ip();
    opt.port = stores[0]->addrs(vpid).port();
    opt.paxos_port = stores[0]->addrs(vpid).paxos_port();
    opt.ngroup = 1;
    opt.nsub = 2;
    opt.log_func = log_func;

    NLVerb("store %d opt done", vpid);

    store::Store store(opt);
    if (comm::RetCode::RET_OK != (ret = store.Init())) {
        NLErr("Store Init ret %d", as_integer(ret));
        return;
    }

    NLVerb("store %d init ret %d", vpid, as_integer(ret));

    sleep(10);

    if (vpid == 0) {
        while (1) {
            TestAdd(store);
            sleep(10);
        }
    }

    sleep(1000);
}

int main(int argc, char ** argv) {
    for (int vpid{0}; vpid < 3; ++vpid) {
        pid_t pid = fork();
        if (pid == 0) {
            /* send SIGHUP to me if parent dies. */
            prctl(PR_SET_PDEATHSIG, SIGHUP);
            StoreRun(vpid);
            exit(0);
        }
    }
    sleep(1000);

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

