#include "phxqueue/test/test_configfactory.h"

#include <cassert>

#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


void TestConfigFactory::Process() {
    assert(plugin::ConfigFactory::GetInstance()->NewGlobalConfig());
    assert(plugin::ConfigFactory::GetInstance()->NewTopicConfig(1000, ""));
    assert(plugin::ConfigFactory::GetInstance()->NewConsumerConfig(1000, ""));
    assert(plugin::ConfigFactory::GetInstance()->NewStoreConfig(1000, ""));
}


}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

