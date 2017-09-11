#include "phxqueue_phxrpc/consumer/proto/consumerserverconfig.pb.h"

#include <iostream>

#include <google/protobuf/util/json_util.h>

#include "phxqueue_phxrpc/comm.h"


namespace phxqueue_phxrpc {

namespace consumer {


class ConsumerServerConfig : public comm::FileLoader {
  public:
    ConsumerServerConfig(const std::string &file) : comm::FileLoader(file) {}
    virtual ~ConsumerServerConfig() override {}

    proto::ConsumerServerConfig & GetProto() {
        return proto_;
    }

    virtual bool LoadFile(const std::string &file, bool is_reload) {
        comm::FileConfig config(file);
        if (!config.Read()) {
            return false;
        }

        std::string json;
        if (!config.GetContent(json)) {
            return false;
        }

        google::protobuf::util::JsonParseOptions opt;
        opt.ignore_unknown_fields = true;
        auto status = JsonStringToMessage(json, &proto_, opt);
        if (!status.ok()) {
            return false;
        }

        return true;
    }

  private:
    proto::ConsumerServerConfig proto_;
};


}  // namespace test

}  // namespace phxqueue_phxrpc
