/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/comm/iniparser.h"

#include <cstring>
#include <map>
#include <sys/stat.h>

#include "phxqueue/comm.h"


namespace phxqueue_phxrpc {

namespace comm {


using namespace std;


class INIParser::INIParserImpl {
  public:
    pthread_rwlock_t rwlock;
    map<pair<string, string>, string> section_key2val;
    bool del_after_get = false;
};


INIParser::INIParser() : impl_(new INIParserImpl()) {
    assert(impl_);
    pthread_rwlock_init(&impl_->rwlock, nullptr);
}

INIParser::~INIParser() {}


static string Slice2String(const char *st, const char *ed) {
    return string(st, ed - st);
}

static void StrTrim(const string &delimiters, string &str) {
    auto st = str.find_first_not_of(delimiters);
    if (string::npos != st) str = str.substr(st);

    auto ed = str.find_last_not_of(delimiters);
    if (string::npos != ed) str = str.substr(0, ed + 1);
}

void INIParser::ParseContent(const string &content) {
    phxqueue::comm::utils::RWLock rwlock_read(&impl_->rwlock, phxqueue::comm::utils::RWLock::LockMode::WRITE);

    string section, key, val;

    impl_->section_key2val.clear();

    if (content.empty()) return;
    const char *src = content.c_str();
    int len = content.length();

    const char *eol, *tmp;

    for (const char *ptr = src; len > 0;) {
        eol = strchr(ptr, '\n');  // for each line

        if (eol == NULL) eol = ptr + len;  // last line

        len -= ( eol - ptr +1);

        if (ptr < eol) {
            while (ptr < eol && (*ptr == ' ' || *ptr == '\t')) ptr++;  // ltrim
            if (*ptr == '[') {  //section
                ptr += 1;
                tmp = ptr;
                for (; *ptr != ']'  && *ptr != '\n' && ptr < eol; ptr++);

                section = Slice2String(tmp, ptr);
                StrTrim("\t ", section);
            } else if (nullptr == strchr("#;\n",*ptr)) {  //item
                tmp = ptr;
                for (; *ptr != '=' && ptr < eol; ptr++);

                key = Slice2String(tmp, ptr);
                StrTrim("\t ", key);

                if (*ptr == '=' && !section.empty()) {
                    ptr += 1;

                    tmp = ptr;
                    for (; *ptr != '\n' && *ptr != '\r' && ptr < eol ; ptr++);

                    val = Slice2String(tmp, ptr);
                    StrTrim("\t ", val);

                    impl_->section_key2val.emplace(make_pair(section, key), val);
                }
            }
        }

        ptr = eol + 1;
    }
}


bool INIParser::GetValue(const string &section, const string &key, string &val) {
    phxqueue::comm::utils::RWLock rwlock_read(&impl_->rwlock, phxqueue::comm::utils::RWLock::LockMode::READ);

    val = "";
    auto &&it = impl_->section_key2val.find(make_pair(section, key));
    if (impl_->section_key2val.end() == it) {
        return false;
    }
    val = it->second;
    if (impl_->del_after_get) impl_->section_key2val.erase(it);
    return true;
}

void INIParser::SetDelAfterGet(bool del_after_get) {
    impl_->del_after_get = del_after_get;
}

void INIParser::GetAllValue(std::vector<std::tuple<std::string, std::string, std::string>> &vals) {
    vals.clear();
    for (auto &&kv : impl_->section_key2val) {
        vals.push_back(make_tuple(kv.first.first, kv.first.second, kv.second));
    }
}



}  // namespace comm

}  // namespace phxqueue_phxrpc

