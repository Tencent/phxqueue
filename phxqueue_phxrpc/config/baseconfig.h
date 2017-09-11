#pragma once

#include <map>
#include <set>
#include <google/protobuf/util/json_util.h>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/comm.h"


namespace phxqueue_phxrpc {

namespace config {


template <typename ConfigType> class BaseConfig : public comm::FileLoader, public ConfigType {
  public:
    BaseConfig() : comm::FileLoader(), ConfigType() {}
    virtual ~BaseConfig() {}

    virtual bool LoadFile(const std::string &file, bool is_reload) {
        comm::FileConfig config(file);
        if (!config.Read()) {
            QLErr("FileConfig::Read fail. file %s", file.c_str());
            return false;
        }

        std::string content;
        if (!config.GetContent(content)) {
            QLErr("FileConfig::GetContent fail. content %s", content.c_str());
            return false;
        }

        typename ConfigType::ProtoType proto;

        google::protobuf::util::JsonParseOptions opt;
        opt.ignore_unknown_fields = true;
        auto status = JsonStringToMessage(content, &proto, opt);
        if (!status.ok()) {
            QLErr("JsonStringToMessage fail. %s", status.ToString().c_str());
            return false;
        }

        ConfigType::FillConfig(std::move(proto));

        return true;
    }

    virtual phxqueue::comm::RetCode ReadConfig(typename ConfigType::ProtoType &proto) {
        return phxqueue::comm::RetCode::RET_OK;
    }

    virtual bool IsModified() {
        bool modified = false;
        comm::FileLoader::LoadIfModified(modified);
        modified |= ConfigType::IsModified();
        return modified;
    }
};


}  // namespace config

}  // namespace phxqueue_phxrpc

