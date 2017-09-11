/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <cinttypes>
#include <cstdio>
#include <sstream>

#include "phxqueue_phxrpc/comm/ini2pb.h"


namespace phxqueue_phxrpc {

namespace comm {


using namespace std;


INI2Pb::INI2Pb(INIReadFunc func) : func_(func){}

INI2Pb::~INI2Pb() {}

static void StrSplitList(const string &str, const string &delimiters, vector<string> &results) {
    results.clear();
    auto last = 0;
    auto found = str.find_first_of(delimiters);
    while (string::npos != found) {
        auto r = str.substr(last, found - last);
        last = found + 1;
        found = str.find_first_of(delimiters, last);
        if (!r.empty()) results.push_back(r);
    }
    auto r = str.substr(last);
    if (!r.empty()) results.push_back(r);
}


bool INI2Pb::ParseField(google::protobuf::Message *message, const string &section_name,
                        const google::protobuf::FieldDescriptor *field_descriptor) {
    const google::protobuf::Reflection *reflection(message->GetReflection());

    string val;
    vector<string> val_list;

    func_(section_name, field_descriptor->name(), val);

    if (!val.empty()) {

        StrSplitList(val, " ", val_list);

        if (field_descriptor->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED) {
            switch (field_descriptor->type()) {
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            {
                for (size_t i{0}; val_list.size() > i; ++i) {
                    int64_t real_val = stoll(val_list[i]);
                    reflection->AddInt64(message, field_descriptor, real_val);
                }
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            {
                for (size_t i{0}; val_list.size() > i; ++i) {
                    uint64_t real_val = stoull(val_list[i]);
                    reflection->AddUInt64(message, field_descriptor, real_val);
                }
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            {
                for (size_t i(0); val_list.size() > i; ++i) {
                    int real_val = stoi(val_list[i]);
                    reflection->AddInt32(message, field_descriptor, real_val);
                }
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            {
                for (size_t i(0); val_list.size() > i; ++i) {
                    uint32_t real_val = stoul(val_list[i]);
                    reflection->AddUInt32(message, field_descriptor, real_val);
                }
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_STRING:
            {
                for (size_t i(0); val_list.size() > i; ++i) {
                    std::string &real_val(val_list[i]);
                    reflection->AddString(message, field_descriptor, real_val);
                }
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
            {
                for (size_t i(0); val_list.size() > i; ++i) {
                    int real_val = stoi(val_list[i]);
                    reflection->AddBool(message, field_descriptor, (bool)real_val);
                }
                break;
            }
            default:
                return false;
            };
        } else {
            switch (field_descriptor->type()) {
            case google::protobuf::FieldDescriptor::TYPE_FIXED64:
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            {
                int64_t real_val = stoll(val);
                reflection->SetInt64(message, field_descriptor, real_val);
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_UINT64:
            {
                uint64_t real_val = stoull(val);
                reflection->SetUInt64(message, field_descriptor, real_val);
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_FIXED32:
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            {
                int real_val = stoi(val);
                reflection->SetInt32(message, field_descriptor, real_val);
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_UINT32:
            {
                uint32_t real_val = stoul(val);
                reflection->SetUInt32(message, field_descriptor, real_val);
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_STRING:
            {
                std::string &real_val(val);
                reflection->SetString(message, field_descriptor, real_val);
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_BOOL:
            {
                int real_val = stoi(val);
                reflection->SetBool(message, field_descriptor, (bool)real_val);
                break;
            }
            default:
                return false;
            };
        }

        return true;
    }

    if (google::protobuf::FieldDescriptor::TYPE_MESSAGE == field_descriptor->type()) {
        if (google::protobuf::FieldDescriptor::LABEL_REPEATED == field_descriptor->label()) {
            int idx(0);
            do {
                google::protobuf::Message *sub_message(reflection->AddMessage(message, field_descriptor));
                if (!Parse(sub_message, idx)) {
                    reflection->RemoveLast(message, field_descriptor);
                    break;
                }
            } while(++idx);

            return idx ? true : false;
        }
        else
        {
            return Parse(reflection->MutableMessage(message, field_descriptor));
        }
    }

    return false;
}

bool INI2Pb::ParseMessage(google::protobuf::Message *message, const string &section_name) {
    const google::protobuf::Descriptor* descriptor = message->GetDescriptor();

    int nvalid(0);
    for (int i(0); descriptor->field_count() > i; ++i) {
        const google::protobuf::FieldDescriptor* field_descriptor = descriptor->field(i);
        if (ParseField(message, section_name, field_descriptor)) ++nvalid;
    }
    if (nvalid) return true;
    return false;
}


bool INI2Pb::Parse(google::protobuf::Message *message, int idx) {
    if (!func_) return false;

    const google::protobuf::Descriptor *descriptor(message->GetDescriptor());

    std::string section_name(descriptor->name());
    if (0 <= idx) {
        ostringstream oss;
        oss << idx;
        section_name += oss.str();
    }

    return ParseMessage(message, section_name);
}


}  // namespace comm

}  // namespace phxqueue_phxrpc

