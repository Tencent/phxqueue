#pragma once

#include <memory>

#include "phxqueue/config.h"
#include "phxqueue/plugin.h"


namespace phxqueue_phxrpc {

namespace plugin {


class ConfigFactory : public phxqueue::plugin::ConfigFactory {
  public:
    ConfigFactory(const std::string &global_config_path);
    virtual ~ConfigFactory();

    virtual std::unique_ptr<phxqueue::config::GlobalConfig> NewGlobalConfig();
    virtual std::unique_ptr<phxqueue::config::TopicConfig>
    NewTopicConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::ConsumerConfig>
    NewConsumerConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::StoreConfig>
    NewStoreConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::SchedulerConfig>
    NewSchedulerConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::LockConfig>
    NewLockConfig(const int topic_id, const std::string &path);

  private:
    class ConfigFactoryImpl;
    std::unique_ptr<ConfigFactoryImpl> impl_;
};


}  // namespace plugin

}  // namespace phxqueue_phxrpc

