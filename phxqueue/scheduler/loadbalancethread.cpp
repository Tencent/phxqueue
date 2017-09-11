#include "phxqueue/scheduler/loadbalancethread.h"

#include <thread>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/scheduler/schedulermgr.h"


namespace phxqueue {

namespace scheduler {


using namespace std;


class LoadBalanceThread::LoadBalanceThreadImpl {
  public:
    LoadBalanceThreadImpl() {}
    virtual ~LoadBalanceThreadImpl() {}

    Scheduler *scheduler{nullptr};

    unique_ptr<thread> t{nullptr};
    bool stop{true};
};


LoadBalanceThread::LoadBalanceThread(Scheduler *const scheduler)
        : impl_(new LoadBalanceThreadImpl()) {
    impl_->scheduler = scheduler;
}

LoadBalanceThread::~LoadBalanceThread() {
    Stop();
}

void LoadBalanceThread::Run() {
    if (!impl_->t) {
        impl_->stop = false;
        impl_->t = unique_ptr<thread>(new thread(&LoadBalanceThread::DoRun, this));
    }
    assert(impl_->t);
}

void LoadBalanceThread::Stop() {
    if (impl_->t) {
        impl_->stop = true;
        impl_->t->join();
        impl_->t.release();
    }
}

void LoadBalanceThread::DoRun() {
    comm::RetCode ret;
    const int topic_id{impl_->scheduler->GetTopicID()};

    while (true) {

        shared_ptr<const config::TopicConfig> topic_config;
        if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
            QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
            sleep(1);
            continue;
        }

        const uint64_t now{comm::utils::Time::GetSteadyClockMS()};

        if (!impl_->scheduler->GetSchedulerMgr()->IsMaster(now)) {
            QLInfo("not master");
            sleep(topic_config->GetProto().topic().scheduler_load_balance_interval_s());

            continue;
        }

        ret = impl_->scheduler->GetSchedulerMgr()->LoadBalance(now);
        if (comm::RetCode::RET_OK != ret) {
            QLErr("LoadBalance err %d", ret);
        }

        sleep(topic_config->GetProto().topic().scheduler_load_balance_interval_s());
    }
}


}  // namespace scheduler

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/scheduler/loadbalancethread.cpp $ $Id: loadbalancethread.cpp 2107662 2017-06-01 04:14:24Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

