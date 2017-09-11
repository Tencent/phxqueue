/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/multiproc.h"

#include <cstdlib>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>


namespace phxqueue {

namespace comm {


void MultiProc::ForkAndRun(const int procs) {
    // start child process

    children_.resize(procs);

    for (int i(0); procs > i; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            // send SIGHUP to me if parent dies
            prctl(PR_SET_PDEATHSIG, SIGHUP);
            ChildRun(i);
            exit(0);
        }
        children_[i] = pid;
    }


    while (true) {
        int st;
        pid_t pid = waitpid(-1, &st, WNOHANG);
        if (pid <= 0) {
            sleep(5);
            continue;
        }

        for (int i(0); procs > i; ++i) {

            if (children_[i] == pid) {

                pid_t pid = fork();
                if (pid == 0) {
                    prctl(PR_SET_PDEATHSIG, SIGHUP);
                    ChildRun(i);
                    exit(0);
                }
                children_[i] = pid;
                break;
            }
        }

        usleep(100000);
    }
}


}  //namespace comm

}  // namespace phxqueue

