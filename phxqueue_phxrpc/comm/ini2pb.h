#pragma once

#include <cstdlib>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <string>


namespace phxqueue_phxrpc {

namespace comm {


using INIReadFunc = std::function<void (const std::string &section, const std::string &key, std::string &val)>;


class INI2Pb {
  public:
    INI2Pb(INIReadFunc);
    ~INI2Pb();
    bool Parse(google::protobuf::Message *message, int idx = -1);

  private:
    bool ParseField(google::protobuf::Message *message, const std::string &section_name,
                    const google::protobuf::FieldDescriptor *field_descriptor);
    bool ParseMessage(google::protobuf::Message *message, const std::string &section_name);

    INIReadFunc func_ = nullptr;
};


}  // namespace comm

}  // namespace phxqueeu_phxrpc

