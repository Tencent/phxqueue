#pragma once

#include <memory>
#include <vector>
#include <set>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/storeconfig.pb.h"


namespace phxqueue {

namespace config {


struct StoreConfigImpl_t;

class StoreConfig : public BaseConfig<proto::StoreConfig> {
  public:
    StoreConfig();

    virtual ~StoreConfig();

    comm::RetCode GetAllStore(std::vector<std::shared_ptr<const proto::Store>> &stores) const;

    comm::RetCode GetAllStoreID(std::set<int> &store_ids) const;

    comm::RetCode GetStoreByStoreID(const int store_id, std::shared_ptr<const proto::Store> &store) const;

    comm::RetCode GetStoreIDByAddr(const comm::proto::Addr &addr, int &store_id) const;

    comm::RetCode GetStoreByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Store> &store) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::StoreConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class StoreConfigImpl;
    std::unique_ptr<StoreConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

