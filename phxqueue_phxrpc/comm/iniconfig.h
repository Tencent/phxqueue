#pragma once

#include <memory>
#include <string>

#include "phxqueue_phxrpc/comm/fileconfig.h"


namespace phxqueue_phxrpc {

namespace comm {


class INIConfig : public FileConfig {
  public:
    INIConfig();
    INIConfig(const std::string &file);
    virtual ~INIConfig();

    bool GetValue(const std::string &section, const std::string &key, std::string &val);

private:
    virtual void ParseContent(const std::string &content);

    class INIConfigImpl;
    std::unique_ptr<INIConfigImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue_phxrpc

