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


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phoenix/phxqueue/lib/consumer/multiproc.cpp $ $Id: multiproc.cpp 1858830 2016-12-09 10:48:07Z unixliang $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

