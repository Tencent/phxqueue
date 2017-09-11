#pragma once

#include <map>
#include <queue>
#include <memory>
#include <random>
#include <sstream>
#include <mutex>

#include "phxqueue/comm/errdef.h"

namespace phxqueue {

namespace comm {

class Notifier {
public:
    Notifier();
    ~Notifier();

    bool Init();
    void Notify(const comm::RetCode retcode);
    void Wait(comm::RetCode &retcode);

private:
    class NotifierImpl;
    std::unique_ptr<NotifierImpl> impl_;
};

class NotifierPool {
public:
    NotifierPool();
    ~NotifierPool();

    static NotifierPool *GetInstance();

    std::unique_ptr<Notifier> Get();
    void Put(std::unique_ptr<Notifier> &notifier);

private:
    class NotifierPoolImpl;
    std::unique_ptr<NotifierPoolImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue

