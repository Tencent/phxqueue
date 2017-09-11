/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "phxqueue_phxrpc/test/simpleconfig.h"


using namespace std;


int main() {
    phxqueue_phxrpc::test::SimpleConfig simple_config("./etc/simpleconfig.conf");

    while (1) {
        sleep(1);

        bool is_modified{false};
        simple_config.LoadIfModified(is_modified);

        if (is_modified) cout << "modified" << endl;
        cout << simple_config.GetProto().DebugString() << endl;
        cout << simple_config.GetProto().bars_size() << endl;
    }

    return 0;
}

