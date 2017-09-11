#include "phxqueue/comm/notifierpool.h"

#include <cstdlib>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include "phxqueue/comm/logger.h"
#include "phxqueue/comm/utils.h"


namespace phxqueue {

namespace comm {

using namespace std;

class Notifier::NotifierImpl {
public:
    NotifierImpl() {}
    ~NotifierImpl() {}

    int fds[2];
};

Notifier::Notifier() : impl_(new NotifierImpl()){
    impl_->fds[0] = impl_->fds[1] = -1;
}

Notifier::~Notifier() {
    for (int i{0}; i < 2; ++i) {
        if (-1 != impl_->fds[i]) close(impl_->fds[i]);
        impl_->fds[i] = -1;
    }
}

bool Notifier::Init() {
    if (0 != pipe(impl_->fds)) {
        QLErr("pipe fail");
        return false;
    }

    // set nonblock for CoRead/CoWrite
    {
        for (int i{0}; i < 2; ++i) {
            auto flags = fcntl(impl_->fds[i], F_GETFL, 0);
            fcntl(impl_->fds[i], F_SETFL, flags | O_NONBLOCK);
        }
    }
    return true;
}

void Notifier::Notify(const comm::RetCode retcode) {
    int iretcode = as_integer(retcode);
    while (!comm::utils::CoWrite(impl_->fds[1], reinterpret_cast<char*>(&iretcode), sizeof(int))) {
        QLErr("CoWrite fail");
        poll(nullptr, 0, 100);
    }
}

void Notifier::Wait(comm::RetCode &retcode) {
    int iretcode;
    while (!comm::utils::CoRead(impl_->fds[0], reinterpret_cast<char*>(&iretcode), sizeof(int))) {
        QLErr("CoRead fail");
        poll(nullptr, 0, 100);
    }
    retcode = static_cast<comm::RetCode>(iretcode);
}

//////////////////////////

class NotifierPool::NotifierPoolImpl {
public:
    mutex lock;
    queue<unique_ptr<Notifier>> notifiers;
};

NotifierPool::NotifierPool() : impl_(new NotifierPoolImpl()) {}

NotifierPool::~NotifierPool() {}


NotifierPool *NotifierPool::GetInstance() {
    static NotifierPool pool;
    return &pool;
}

unique_ptr<Notifier> NotifierPool::Get() {
    lock_guard<mutex> lock_guard(impl_->lock);

    unique_ptr<Notifier> notifier = nullptr;
    if (!impl_->notifiers.empty()) {
        notifier = move(impl_->notifiers.front());
        impl_->notifiers.pop();

    } else {
        notifier.reset(new Notifier());
        if (!notifier->Init()) notifier = nullptr;
    }
    assert(notifier != nullptr);

    return notifier;
}

void NotifierPool::Put(unique_ptr<Notifier> &notifier) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (notifier == nullptr) return;
    impl_->notifiers.push(move(notifier));
}


}  //namespace comm

}  // namespace phxqueue



//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

