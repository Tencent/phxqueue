/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/utils/other_util.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "phxqueue/comm/logger.h"


namespace phxqueue {

namespace comm {

namespace utils {


class CpuInfo {
  public:
    unsigned long long use{0};
    unsigned long long nice{0};
    unsigned long long sys{0};
    unsigned long long idle{0};
    unsigned long long iowait{0};
    unsigned long long hardirq{0};
    unsigned long long softirq{0};

    unsigned long long userate;
    unsigned long long sysrate;
    unsigned long long iowaitrate;
    unsigned long long hardirqrate;
    unsigned long long softirqrate;

    unsigned long long usage;
};

class CpuRate {
  public:
    unsigned long long userate{0};
    unsigned long long sysrate{0};
    unsigned long long iowaitrate{0};
    unsigned long long hardirqrate{0};
    unsigned long long softirqrate{0};
};

void GetCpuInfo(CpuInfo &cpu_info) {
    CpuInfo new_cpu_info;

    char *line{nullptr};
    size_t len{0};
    ssize_t read;

    bool is_read_cpu_info{false};

    FILE *proc_fp = fopen("/proc/stat", "r");
    if(NULL == proc_fp) {
        cpu_info = CpuInfo();
        return;
    }

    while ((read = getline(&line, &len, proc_fp)) != -1) {
        if (strncmp(line, "cpu", 3) == 0) {
            char *p = line;

            while (isspace(*p))
                p++;
            while (*p && !isspace(*p))
                p++;

            new_cpu_info.use = strtoull(p, &p, 10);
            new_cpu_info.nice = strtoull(p, &p, 10);
            new_cpu_info.sys = strtoull(p, &p, 10);
            new_cpu_info.idle = strtoull(p, &p, 10);
            new_cpu_info.iowait = strtoull(p, &p, 10);
            new_cpu_info.hardirq = strtoull(p, &p, 10);
            new_cpu_info.softirq = strtoull(p, &p, 10);

            double all_new = (double) (new_cpu_info.use + new_cpu_info.nice + new_cpu_info.sys
                                        + new_cpu_info.idle + new_cpu_info.hardirq + new_cpu_info.softirq
                                        + new_cpu_info.iowait);
            double all_old = (double) (cpu_info.use + cpu_info.nice
                                        + cpu_info.sys + cpu_info.idle + cpu_info.hardirq
                                        + cpu_info.softirq + cpu_info.iowait);

            double all = all_new - all_old;
            if (all <= 0.0001) {
                is_read_cpu_info = true;
                break;
            }

            double rate{0};

            rate = 100.0 * (new_cpu_info.use - cpu_info.use) / all;
            if (rate > 0) {
                new_cpu_info.userate = (unsigned long long) round(rate);
            }

            rate = 100.0 * (new_cpu_info.sys - cpu_info.sys) / all;
            if (rate > 0) {
                new_cpu_info.sysrate = (unsigned long long) round(rate);
            }

            rate = 100.0 * (new_cpu_info.iowait - cpu_info.iowait) / all;
            if (rate > 0) {
                new_cpu_info.iowaitrate = (unsigned long long) round(rate);
            }

            rate = 100.0 * (new_cpu_info.hardirq - cpu_info.hardirq) / all;
            if (rate > 0) {
                new_cpu_info.hardirqrate = (unsigned long long) round(rate);
            }

            rate = 100.0 * (new_cpu_info.softirq - cpu_info.softirq) / all;
            if (rate > 0) {
                new_cpu_info.softirqrate = (unsigned long long) round(rate);
            }

            rate = 100.0 * (new_cpu_info.use + new_cpu_info.sys + new_cpu_info.softirq + new_cpu_info.iowait - cpu_info.use - cpu_info.sys - cpu_info.softirq - cpu_info.iowait) / all;
            if (rate > 0) {
                new_cpu_info.usage = (unsigned long long) round(rate);
            }

            cpu_info = new_cpu_info;

            is_read_cpu_info = true;
            break;
        }
    }

    if (line) {
        free(line);
    }

    if(!is_read_cpu_info) {
        NLErr("read cpu info failed");
        cpu_info = CpuInfo();
    }
    fclose(proc_fp);
}


void GetCpuRate(CpuInfo &oldinfo, CpuInfo &newinfo, CpuRate &rate) {
    int64_t alldiff = newinfo.use - oldinfo.use +
        newinfo.nice - oldinfo.nice +
        newinfo.sys - oldinfo.sys +
        newinfo.idle - oldinfo.idle +
        newinfo.iowait - oldinfo.iowait +
        newinfo.hardirq - oldinfo.hardirq +
        newinfo.softirq - oldinfo.softirq;
    if ( alldiff > 0 )
    {
        rate.userate = 100*(newinfo.use - oldinfo.use)/alldiff;
        rate.sysrate = 100*(newinfo.sys - oldinfo.sys)/alldiff;
        rate.iowaitrate = 100*(newinfo.iowait - oldinfo.iowait)/alldiff;
        rate.hardirqrate = 100*(newinfo.hardirq - oldinfo.hardirq)/alldiff;
        rate.softirqrate = 100*(newinfo.softirq - oldinfo.softirq)/alldiff;
        oldinfo = newinfo;
        NLInfo("use %llu sys %llu soft %llu", rate.userate, rate.sysrate, rate.softirqrate);
    }
}

int GetCpu() {
    static int cpu = 0;
    static time_t last_update_time = 0;
    const time_t now = time(nullptr);
    if (now != last_update_time) {
        last_update_time = now;

        static CpuInfo cpu_info_old;;
        CpuInfo cpu_info;
        CpuRate cpu_rate;
        GetCpuInfo(cpu_info);
        GetCpuRate(cpu_info_old, cpu_info, cpu_rate);

        cpu = cpu_rate.userate + cpu_rate.sysrate + cpu_rate.softirqrate;
    }
    NLInfo("cpu %d", cpu);
    return cpu;
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

