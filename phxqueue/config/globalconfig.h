#pragma once

#include <cassert>
#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/consumerconfig.h"
#include "phxqueue/config/lockconfig.h"
#include "phxqueue/config/proto/globalconfig.pb.h"
#include "phxqueue/config/schedulerconfig.h"
#include "phxqueue/config/storeconfig.h"
#include "phxqueue/config/topicconfig.h"


namespace phxqueue {

namespace config {


class GlobalConfig : public BaseConfig<proto::GlobalConfig>{
  public:
    GlobalConfig();

    virtual ~GlobalConfig();

    static GlobalConfig *GetThreadInstance();

    comm::RetCode GetTopicIDByTopicName(const std::string &topic_name, int &topic_id) const;

    comm::RetCode GetTopicConfigByTopicID(const int topic_id, std::shared_ptr<const TopicConfig> &topic_config);

    comm::RetCode GetAllTopicConfig(std::vector<std::shared_ptr<const TopicConfig> > &topic_confs) const;

    comm::RetCode GetAllTopicID(std::set<int> &topic_ids) const;

    comm::RetCode GetTopicIDByHandleID(const int handle_id, int &topic_id) const;

    comm::RetCode GetConsumerConfig(const int topic_id, std::shared_ptr<const ConsumerConfig> &consumer_config);

    comm::RetCode GetStoreConfig(const int topic_id, std::shared_ptr<const StoreConfig> &store_config);

    comm::RetCode GetSchedulerConfig(const int topic_id, std::shared_ptr<const SchedulerConfig> &scheduler_config);

    comm::RetCode GetLockConfig(const int topic_id, std::shared_ptr<const LockConfig> &lock_config);

    uint64_t GetLastModTime(const int topic_id);

  protected:
    virtual comm::RetCode ReadConfig(proto::GlobalConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    struct GlobalConfigImpl_t;
    std::unique_ptr<struct GlobalConfigImpl_t> impl_;
};


}  // namespace config

}  // namespace phxqueue

