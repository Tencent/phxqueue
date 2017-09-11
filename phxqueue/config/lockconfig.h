#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/lockconfig.pb.h"


namespace phxqueue {

namespace config {


struct LockConfigImpl_t;

class LockConfig : public BaseConfig<proto::LockConfig> {
  public:
    LockConfig();

    virtual ~LockConfig();

    comm::RetCode GetAllLock(std::vector<std::shared_ptr<const proto::Lock> > &locks) const;

    comm::RetCode GetAllLockID(std::set<int> &lock_ids) const;

    comm::RetCode GetLockByLockID(const int lock_id, std::shared_ptr<const proto::Lock> &lock) const;

    comm::RetCode GetLockIDByAddr(const comm::proto::Addr &addr, int &lock_id) const;

    comm::RetCode GetLockByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Lock> &lock) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::LockConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class LockConfigImpl;
    std::unique_ptr<LockConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

