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

