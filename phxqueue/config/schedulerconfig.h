#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/schedulerconfig.pb.h"


namespace phxqueue {

namespace config {


struct SchedulerConfigImpl_t;

class SchedulerConfig : public BaseConfig<proto::SchedulerConfig> {
  public:
    SchedulerConfig();

    virtual ~SchedulerConfig();

    comm::RetCode GetScheduler(std::shared_ptr<const proto::Scheduler> &scheduler) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::SchedulerConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class SchedulerConfigImpl;
    std::unique_ptr<SchedulerConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

