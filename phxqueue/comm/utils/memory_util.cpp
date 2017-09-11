/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

