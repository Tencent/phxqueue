/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>
#include <thread>

#include "phxqueue/comm.h"
#include "phxqueue/producer.h"

using namespace std;

int main(int argc, char **argv) {
    unique_ptr<phxqueue::comm::Notifier> notifier = move(phxqueue::comm::NotifierPool::GetInstance()->Get());
    assert(notifier != nullptr);

    auto t = new thread([&notifier]()->void {
            cout << "thread 2 begin wait notify ..." << endl;
            phxqueue::comm::RetCode retcode;
            notifier->Wait(retcode);
            cout << "thread 2 recv notify. retcode " << as_integer(retcode) << endl;
        });

    sleep(1);

    phxqueue::comm::RetCode retcode = phxqueue::comm::RetCode::RET_OK;
    cout << "thread 1 send notify. retcode " << as_integer(retcode) << endl;
    notifier->Notify(retcode);

    sleep(1);

    return 0;
}

