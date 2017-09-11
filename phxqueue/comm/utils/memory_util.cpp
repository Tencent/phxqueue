#include "phxqueue/comm/utils/memory_util.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>


namespace phxqueue {

namespace comm {

namespace utils {


bool MemStat::Stat(pid_t pid) {
    char fileName[256];
    if (pid == 0) {
        strcpy(fileName, "/proc/self/statm");
    } else {
        snprintf(fileName, sizeof(fileName), "/proc/%d/statm", pid);
    }

    FILE *file = fopen(fileName, "r");
    if (file) {
        if (0 == fscanf(file, "%lu %lu %lu %lu %lu %lu %lu", &size, &resident, &share, &text, &lib, &data, &dt)) {
            ;
        }
        fclose(file);
        return true;
    }
    return false;
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/utils/memory_util.cpp $ $Id: memory_util.cpp 2063374 2017-04-27 02:56:51Z unixliang $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

