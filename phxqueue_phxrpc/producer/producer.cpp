#include "phxqueue_phxrpc/producer.h"

#include "phxqueue_phxrpc/app/store/store_client.h"

namespace phxqueue_phxrpc {

namespace producer {

using namespace std;

Producer::Producer(const phxqueue::producer::ProducerOption &opt)
        : phxqueue::producer::Producer(opt) {}

Producer::~Producer() {}

void Producer::CompressBuffer(const string &buffer, string &compressed_buffer, int &buffer_type) {
    compressed_buffer = buffer;
    buffer_type = 0;
}

phxqueue::comm::RetCode Producer::Add(const phxqueue::comm::proto::AddRequest &req, phxqueue::comm::proto::AddResponse &resp) {
    StoreClient store_client;
    auto ret = store_client.ProtoAdd(req, resp);

    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAdd ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}  // namespace producer

}  // namespace phxqueue_phxrpc

