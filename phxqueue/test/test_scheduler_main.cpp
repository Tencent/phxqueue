/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cinttypes>
#include <iostream>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/plugin.h"

#include "phxqueue/test/simplescheduler.h"


using namespace phxqueue;
using namespace std;


void TestGetAddrScale(scheduler::Scheduler &scheduler) {
    NLVerb("begin");

    constexpr int NR_CPU{7};
    const int cpu1s[NR_CPU]{11, 15, 19, 13, 12, 17, 10};
    const int cpu2s[NR_CPU]{51, 55, 59, 53, 52, 57, 50};
    static uint32_t cpu_offset{0};
    ++cpu_offset;
    if (NR_CPU <= cpu_offset) {
        cpu_offset = 0;
    }

    const int topic_id{1000};

    const string ip1{"127.0.0.1"};
    const int port1{8001};
    const int paxos_port1{0};

    const string ip2{"127.0.0.1"};
    const int port2{8002};
    const int paxos_port2{0};

    comm::proto::GetAddrScaleRequest req;
    comm::proto::GetAddrScaleResponse resp;

    req.set_topic_id(topic_id);

    comm::proto::Addr *addr{req.mutable_addr()};
    addr->set_ip(ip1);
    addr->set_port(port1);
    addr->set_paxos_port(paxos_port1);

    comm::proto::LoadInfo *load_info{req.mutable_load_info()};
    load_info->set_cpu(cpu1s[cpu_offset]);

    comm::RetCode ret{scheduler.GetAddrScale(req, resp)};
    if (comm::RetCode::RET_OK != ret) {
        NLErr("scheduler GetAddrScale ret %d", as_integer(ret));

        return;
    }

    string s(comm::utils::AddrScalesToString(resp.addr_scales()));
    NLVerb("1st scales {%s}", s.c_str());

    req.Clear();

    req.set_topic_id(topic_id);

    addr = req.mutable_addr();
    addr->set_ip(ip2);
    addr->set_port(port2);
    addr->set_paxos_port(paxos_port2);
    load_info->set_cpu(cpu2s[cpu_offset]);

    ret = scheduler.GetAddrScale(req, resp);
    if (comm::RetCode::RET_OK != ret) {
        NLErr("scheduler GetAddrScale ret %d", as_integer(ret));

        return;
    }

    s = comm::utils::AddrScalesToString(resp.addr_scales());
    NLVerb("2nd scales {%s}", s.c_str());
}

void SchedulerRun(const int vpid) {
    comm::RetCode ret;

    auto proc_name(string("test_scheduler.") + to_string(vpid));

    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger(proc_name, "/tmp/phxqueue/log", 3, log_func);
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    config::SchedulerConfig scheduler_config;
    scheduler_config.Load();

    shared_ptr<const config::proto::Scheduler> template_scheduler;
    if (comm::RetCode::RET_OK != (ret = scheduler_config.GetScheduler(template_scheduler))) {
        NLErr("GetAllScheduler ret %d", as_integer(ret));

        return;
    }
    if (3 != template_scheduler->addrs_size()) {
        NLErr("scheduler->nr_addr %zu != 3", template_scheduler->addrs_size());

        return;
    }

    auto path_base(string("/tmp/phxqueue/scheduler.") + to_string(vpid));

    scheduler::SchedulerOption opt;
    opt.ip = template_scheduler->addrs(vpid).ip();
    opt.port = template_scheduler->addrs(vpid).port();
    opt.log_func = log_func;

    NLVerb("scheduler %d opt done", vpid);

    test::SimpleScheduler simple_scheduler(opt);
    if (comm::RetCode::RET_OK != (ret = simple_scheduler.Init())) {
        NLErr("scheduler %d Init ret %d", vpid, as_integer(ret));

        return;
    }

    NLVerb("scheduler %d Init ret %d", vpid, as_integer(ret));

    sleep(10);

    if (vpid == 0) {
        while (true) {
            TestGetAddrScale(simple_scheduler);
            sleep(2);
        }
    }

    sleep(1000);
}

int main(int argc, char **argv) {
    if (SIG_ERR == signal(SIGCHLD, SIG_IGN)) {
        perror("signal error");

        exit(EXIT_FAILURE);
    }

    for (int vpid{0}; vpid < 3; ++vpid) {
        pid_t pid{fork()};
        if (0 == pid) {
            /* send SIGHUP to me if parent dies. */
            prctl(PR_SET_PDEATHSIG, SIGHUP);
            SchedulerRun(vpid);
            exit(0);
        }
    }

    sleep(1000);

    return 0;
}

