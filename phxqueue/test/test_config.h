#pragma once

#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


class TestConfig {
  public:
    static void TestGlobalConfig(config::GlobalConfig *global_config);
    static void TestTopicConfig(const config::TopicConfig *topic_config);
    static void TestConsumerConfig(const config::ConsumerConfig *consumer_config);
    static void TestStoreConfig(const config::StoreConfig *store_config);
    static void TestSchedulerConfig(const config::SchedulerConfig *scheduler_config);
    static void TestLockConfig(const config::LockConfig *lock_config);
};


}  // namespace test

}  // namespace phxqueue

