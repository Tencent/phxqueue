#include "phxqueue/test/simplehandler.h"

#include <cinttypes>
#include <string>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace test {


using namespace std;


comm::HandleResult SimpleHandler::Handle(const comm::proto::ConsumerContext &cc,
                                         comm::proto::QItem &item, string &uncompressed_buffer) {
    QLVerb("cc: sub_id %d store_id %d queue_id %d. item uin %" PRIu64,
           cc.sub_id(), cc.store_id(), cc.queue_id(), (uint64_t)item.meta().uin());
    return comm::HandleResult::RES_OK;
}


}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

