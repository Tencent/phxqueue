#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>

#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class CheckPointStat {
  public:
    CheckPointStat();
    virtual ~CheckPointStat();

    comm::RetCode Init(const std::string &dir, const std::string &file);

    comm::RetCode GetCheckPoint(uint64_t &cp);

    comm::RetCode UpdateCheckPointAndFlush(const uint64_t cp);

    std::string GetDir() const;
    std::string GetFile() const;

  private:
    class CheckPointStatImpl;
    std::unique_ptr<CheckPointStatImpl> impl_;
};


class CheckPointStatMgr {
  public:
    CheckPointStatMgr(Store *const store);
    virtual ~CheckPointStatMgr();

    comm::RetCode Init();

    CheckPointStat *GetCheckPointStat(const int paxos_group_id);

  private:
    class CheckPointStatMgrImpl;
    std::unique_ptr<CheckPointStatMgrImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

