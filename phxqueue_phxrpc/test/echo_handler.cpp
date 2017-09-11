#include "phxqueue_phxrpc/test/echo_handler.h"

#include <cinttypes>
#include <string>

#include "phxqueue/comm.h"


namespace phxqueue_phxrpc {

namespace test {


using namespace phxqueue;
using namespace std;


comm::HandleResult EchoHandler::Handle(const comm::proto::ConsumerContext &cc,
                                       comm::proto::QItem &item, string &uncompressed_buffer) {
    printf("consume echo \"%s\" succeeded! sub_id %d store_id %d queue_id %d item_uin %" PRIu64 "\n",
           item.buffer().c_str(), cc.sub_id(), cc.store_id(), cc.queue_id(), (uint64_t)item.meta().uin());
    fflush(stdout);
    return comm::HandleResult::RES_OK;
}


}  // namespace test

}  // namespace phxqueue_phxrpc


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/phxqueue_phxrpc/test/echo_handler.cpp $ $Id: echo_handler.cpp 2232606 2017-09-04 09:52:28Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

