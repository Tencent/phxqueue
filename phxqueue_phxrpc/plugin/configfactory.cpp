#include "phxqueue_phxrpc/plugin/configfactory.h"

#include <string>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/config.h"


namespace phxqueue_phxrpc {

namespace plugin {


using namespace std;


class ConfigFactory::ConfigFactoryImpl {
  public:
    ConfigFactoryImpl() {}
    virtual ~ConfigFactoryImpl() {}

    string global_config_path;
};

ConfigFactory::ConfigFactory(const string &global_config_path)
        : phxqueue::plugin::ConfigFactory(), impl_(new ConfigFactoryImpl()) {
    impl_->global_config_path = global_config_path;
}

ConfigFactory::~ConfigFactory() {}

unique_ptr<phxqueue::config::GlobalConfig> ConfigFactory::NewGlobalConfig() {
    QLVerb("path %s", impl_->global_config_path.c_str());

    auto &&conf(new config::GlobalConfig());
    conf->SetFileIfUnset(impl_->global_config_path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::GlobalConfig>(conf);
}

unique_ptr<phxqueue::config::TopicConfig>
ConfigFactory::NewTopicConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::TopicConfig());
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::TopicConfig>(conf);
}

unique_ptr<phxqueue::config::ConsumerConfig>
ConfigFactory::NewConsumerConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::ConsumerConfig());
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::ConsumerConfig>(conf);
}

unique_ptr<phxqueue::config::StoreConfig>
ConfigFactory::NewStoreConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::StoreConfig());
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::StoreConfig>(conf);
}

unique_ptr<phxqueue::config::SchedulerConfig>
ConfigFactory::NewSchedulerConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::SchedulerConfig());
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::SchedulerConfig>(conf);
}

unique_ptr<phxqueue::config::LockConfig>
ConfigFactory::NewLockConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::LockConfig());
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::LockConfig>(conf);
}


}  // namespace plugin

}  // namespace phxqueue_phxrpc


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

