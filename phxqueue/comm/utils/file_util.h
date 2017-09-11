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

