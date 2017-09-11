#include <cinttypes>
#include <iostream>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/lock.h"
#include "phxqueue/plugin.h"


using namespace phxqueue;
using namespace std;


void TestGetLockInfo(lock::Lock &lock, const int topic_id, const int lock_id,
                     const string &lock_key, uint64_t &version) {
    NLVerb("begin");

    comm::proto::GetLockInfoRequest req;
    comm::proto::GetLockInfoResponse resp;

    req.set_topic_id(topic_id);
    req.set_lock_id(lock_id);
    req.set_lock_key(lock_key);

    comm::RetCode ret{lock.GetLockInfo(req, resp)};
    if (comm::RetCode::RET_OK != ret) {
        NLErr("lock GetLockInfo ret %d", as_integer(ret));

        return;
    }

    NLVerb("lock_key %s version %" PRIu64 " client_id %s lease_time_ms %" PRIu64,
           resp.lock_info().lock_key().c_str(), resp.lock_info().version(),
           resp.lock_info().client_id().c_str(), resp.lock_info().lease_time_ms());

    version = resp.lock_info().version();
}

void TestAcquireLock(lock::Lock &lock, const int topic_id, const int lock_id,
                     const string &lock_key, const uint64_t version) {
    NLVerb("begin");

    const string client_id{"test_client_123"};
    const uint64_t lease_time_ms{10000};

    comm::proto::AcquireLockRequest req;
    comm::proto::AcquireLockResponse resp;

    req.set_topic_id(topic_id);
    req.set_lock_id(lock_id);

    comm::proto::LockInfo *lock_info{req.mutable_lock_info()};
    lock_info->set_lock_key(lock_key);
    lock_info->set_version(version);
    lock_info->set_client_id(client_id);
    lock_info->set_lease_time_ms(lease_time_ms);

    comm::RetCode ret{lock.AcquireLock(req, resp)};
    if (comm::RetCode::RET_OK != ret) {
        NLErr("lock AcquireLock ret %d", as_integer(ret));

        return;
    }

    NLVerb("succ");
}

void LockRun(const int vpid) {
    comm::RetCode ret;

    auto proc_name(string("test_lock.") + to_string(vpid));

    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger(proc_name, "/tmp/phxqueue/log", 3, log_func);
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    config::LockConfig lock_config;
    lock_config.Load();

    vector<shared_ptr<const config::proto::Lock>> locks;
    if (comm::RetCode::RET_OK != (ret = lock_config.GetAllLock(locks))) {
        NLErr("GetAllLock ret %d", as_integer(ret));

        return;
    }
    if (0 == locks.size()) {
        NLErr("nr_lock 0");

        return;
    }
    if (3 != locks.at(0)->addrs_size()) {
        NLErr("locks[0]->nr_addr %zu != 3", locks[0]->addrs_size());

        return;
    }

    auto path_base(string("/tmp/phxqueue/lock.") + to_string(vpid));

    lock::LockOption opt;
    opt.topic = "test";
    opt.data_dir_path = path_base;
    opt.ip = locks.at(0)->addrs(vpid).ip();
    opt.port = locks.at(0)->addrs(vpid).port();
    opt.paxos_port = locks.at(0)->addrs(vpid).paxos_port();
    opt.log_func = log_func;
    opt.nr_group = 2;

    NLVerb("lock %d opt done", vpid);

    lock::Lock lock(opt);
    if (comm::RetCode::RET_OK != (ret = lock.Init())) {
        NLErr("lock %d Init ret %d", vpid, as_integer(ret));

        return;
    }

    NLVerb("lock %d Init ret %d", vpid, as_integer(ret));

    sleep(10);

    while (true) {
        const int topic_id{1};
        const int lock_id{1};
        const string lock_key{string("test_lock_") + to_string(vpid)};
        uint64_t version{0};
        TestGetLockInfo(lock, topic_id, lock_id, lock_key, version);
        sleep(1);
        TestAcquireLock(lock, topic_id, lock_id, lock_key, version);
        sleep(10);
    }

    sleep(1000);
}

int main(int argc, char **argv) {
    if (SIG_ERR == signal(SIGCHLD, SIG_IGN)) {
        perror("signal error");

        exit(EXIT_FAILURE);
    }

    for (int vpid{0}; vpid < 3; ++vpid) {
        pid_t pid = fork();
        if (pid == 0) {
            /* send SIGHUP to me if parent dies. */
            prctl(PR_SET_PDEATHSIG, SIGHUP);
            LockRun(vpid);
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
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/test/test_lock_main.cpp $ $Id: test_lock_main.cpp 2124396 2017-06-15 03:58:06Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

