#include "phxqueue/test/simpleproducer.h"
#include "phxqueue/test/test_producer.h"

#include <iostream>
#include <memory>


namespace phxqueue {

namespace test {


using namespace std;


void TestProducer::Process() {
    producer::ProducerOption opt;

    test::SimpleProducer producer(opt);
    producer.Enqueue(1000, 123, 1, "buffer", 1);
}


}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

