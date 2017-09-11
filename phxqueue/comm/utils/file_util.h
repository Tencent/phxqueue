/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstring>
#include <string>
#include <vector>


namespace phxqueue {

namespace comm {

namespace utils {


int ExtractFilePath(const std::string &file_path, std::string *dir, std::string *file_name);

int RemoveFile(const std::string &file_path);

int CopyFile(const std::string &src_file_path, const std::string &dst_file_path);

int RemoveDir(const std::string &dir_path);

int CopyDir(const std::string &src_dir_path, const std::string &dst_dir_path);

int RecursiveListDir(const std::string &dir_path, std::vector<std::string> *file_paths, std::vector<std::string> *sub_dir_paths,
        const bool recursive = false);

int RecursiveRemoveDir(const std::string &dir_path,
        const bool recursive, const bool remove_root = true);

int RecursiveCopyDir(const std::string &src_dir_path, const std::string &dst_dir_path,
        const bool recursive, const bool copy_root = true);

bool AccessDir(const std::string &dir_path);

bool CreateDir(const std::string &dir_path);


}  // namespace utils

}  // namespace comm

}  //namespace phxqueue

