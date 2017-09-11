#pragma once

#include "phxqueue/test/test_config.h"


namespace phxqueue_phxrpc {

namespace test {


class TestConfig : public phxqueue::test::TestConfig {
  public:
    static void Process();
};


}  // namespace test

}  // namespace phxqueue_phxrpc

