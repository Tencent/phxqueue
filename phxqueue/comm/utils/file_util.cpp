/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/utils/file_util.h"

#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <ftw.h>
#include <functional>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;


constexpr int FTW_MAX_FD{20};

int DoTravelDir(const string &root_src_dir_path, const string &relative_src_dir_path, const string &root_dst_dir_path,
        vector<string> *file_paths, vector<string> *sub_dir_paths, const bool recursive,
        const function<int (const string &, const string &, const string &)> &file_before_callback = {},
        const function<int (const string &, const string &, const string &)> &file_after_callback = {},
        const function<int (const string &, const string &, const string &)> &sub_dir_before_callback = {},
        const function<int (const string &, const string &, const string &)> &sub_dir_after_callback = {},
        const int level = 0) {
    string absolute_src_dir_path{root_src_dir_path};
    if (!relative_src_dir_path.empty())
        absolute_src_dir_path += "/" + relative_src_dir_path;

    DIR *dir{nullptr};
    dirent *dirent{nullptr};
    if (!(dir = opendir(absolute_src_dir_path.c_str()))) {
        // TODO:
        //QLErr("level %d opendir \"%s\" err %d", level, absolute_src_dir_path.c_str(), errno);

        return errno;
    }

    while (dirent = readdir(dir)) {
        //QLVerb("level %d sub \"%s\"", level, dirent->d_name);

        const string absolute_sub_path{absolute_src_dir_path + "/" + dirent->d_name};
        if (DT_DIR == dirent->d_type) {
            //QLVerb("level %d subdir \"%s\"", level, dirent->d_name);

            // directory
            if (0 == strcmp(dirent->d_name, ".") || 0 == strcmp(dirent->d_name, ".."))
                continue;

            string relative_sub_dir_path;
            if (relative_src_dir_path.empty())
                relative_sub_dir_path = dirent->d_name;
            else
                relative_sub_dir_path = relative_src_dir_path + "/" + dirent->d_name;

            int ret{0};
            int call_back_ret{0};
            if (sub_dir_before_callback) {
                call_back_ret = sub_dir_before_callback(root_src_dir_path, relative_sub_dir_path, root_dst_dir_path);
                if (0 != call_back_ret) {
                    // TODO:
                    //QLErr("sub_dir_before_callback err %d", call_back_ret);
                }
            }
            if (sub_dir_paths) {
                sub_dir_paths->emplace_back(relative_sub_dir_path);
            }
            if (recursive) {
                ret = DoTravelDir(root_src_dir_path, relative_sub_dir_path, root_dst_dir_path,
                        file_paths, sub_dir_paths, recursive,
                        file_before_callback, file_after_callback, sub_dir_before_callback, sub_dir_after_callback,
                        level + 1);
            }
            if (sub_dir_after_callback) {
                call_back_ret = sub_dir_after_callback(root_src_dir_path, relative_sub_dir_path, root_dst_dir_path);
                if (0 != call_back_ret) {
                    // TODO:
                    //QLErr("sub_dir_after_callback err %d", call_back_ret);
                }
            }

            if (0 != ret) {
                closedir(dir);
                // TODO:
                //QLErr("DoTravelDir \"%s\" err %d", dirent->d_name, ret);

                return ret;
            }

        } else if (DT_REG == dirent->d_type) {
            //QLVerb("level %d file \"%s\"", level, dirent->d_name);

            // regular file
            string relative_file_path;
            if (relative_src_dir_path.empty())
                relative_file_path = dirent->d_name;
            else
                relative_file_path = relative_src_dir_path + "/" + dirent->d_name;

            int call_back_ret{0};
            if (file_before_callback) {
                call_back_ret = file_before_callback(root_src_dir_path, relative_file_path, root_dst_dir_path);
                if (0 != call_back_ret) {
                    // TODO:
                    //QLErr("file_before_callback err %d", call_back_ret);
                }
            }
            if (file_paths) {
                file_paths->emplace_back(relative_file_path);
            }
            if (file_after_callback) {
                call_back_ret = file_after_callback(root_src_dir_path, relative_file_path, root_dst_dir_path);
                if (0 != call_back_ret) {
                    // TODO:
                    //QLErr("%s:%d file_after_callback err %d", call_back_ret);
                }
            }

        } else {
            // default
            // TODO:
            //QLVerb("level %d skip \"%s\" unsupport type", level, dirent->d_name);
        }
    }  // while (dirent = readdir(dir))

    if (-1 == closedir(dir)) {
        // TODO:
        //QLErr("level %d close %s err %d", level, absolute_src_dir_path.c_str(), errno);

        return errno;
    }

    return 0;
}


int ExtractFilePath(const string &file_path, string *dir, string *file_name) {
    const auto pos(file_path.find_last_of("/"));
    if (string::npos == pos) {
        if (dir) {
            *dir = "";
        }
        if (file_name) {
            *file_name = file_path;
        }
    } else {
        if (dir) {
            *dir = file_path.substr(0, pos - 1);
        }
        if (file_name) {
            *file_name = file_path.substr(pos + 1);
        }
    }

    return 0;
}

int RemoveFile(const string &file_path) {
    int ret{0};
    if (-1 == unlink(file_path.c_str())) {
        ret = errno;
        // TODO:
        //QLErr("unlink \"%s\" err %d", file_path.c_str(), ret);
    }
    // TODO:
    //QLInfo("unlink \"%s\" ok", file_path.c_str());

    return ret;
}

int CopyFile(const string &src_file_path, const string &dst_file_path) {
    try {
        ifstream src(src_file_path, ios::binary);
        ofstream dst(dst_file_path, ios::binary);
        if (src.is_open() && dst.is_open() && src.good() && dst.good()) {
            dst << src.rdbuf();
        }
        if (dst.bad()) {
            // if no characters were inserted, executes failbit
            // <http://en.cppreference.com/w/cpp/io/basic_ostream/operator_ltlt>
            // TODO:
            //QLogErr("iofstream \"%s\" -> \"%s\" err",
            //        src_file_path.c_str(), dst_file_path.c_str());

            return -1;
        }
        // TODO:
        //QLInfo("iofstream \"%s\" -> \"%s\" ok", src_file_path.c_str(), dst_file_path.c_str());
    } catch(const exception &e) {
        // TODO:
        //QLErr("iofstream \"%s\" -> \"%s\" e \"%s\"",
        //      src_file_path.c_str(), dst_file_path.c_str(), e.what());

        return -1;
    }

    return 0;
}

int RemoveDir(const string &dir_path) {
    if (-1 == rmdir(dir_path.c_str())) {
        int ret{errno};
        // TODO:
        //QLErr("rmdir \"%s\" err %d", dir_path.c_str(), ret);

        return ret;
    }
    // TODO:
    //QLInfo("rmdir \"%s\" ok", dir_path.c_str());

    return 0;
}

int CopyDir(const string &src_dir_path, const string &dst_dir_path) {
    // struct required, rationale: function stat() exists also
    struct stat src_stat;
    if (-1 == stat(src_dir_path.c_str(), &src_stat)) {
        int ret{errno};
        // TODO:
        //QLErr("stat \"%s\" err %d", src_dir_path.c_str(), ret);

        return ret;
    }

    if (-1 == mkdir(dst_dir_path.c_str(), src_stat.st_mode)) {
        int ret{errno};
        // TODO:
        //QLErr("mkdir \"%s\" err %d", dst_dir_path.c_str(), ret);

        return ret;
    }
    // TODO:
    //QLInfo("mkdir \"%s\" ok", dst_dir_path.c_str());

    return 0;
}

int RecursiveListDir(const string &dir_path, vector<string> *file_paths, vector<string> *sub_dir_paths,
        const bool recursive) {
    if (file_paths)
        file_paths->clear();
    if (sub_dir_paths)
        sub_dir_paths->clear();

    return DoTravelDir(dir_path, "", "", file_paths, sub_dir_paths, recursive);
}

int RecursiveRemoveDir(const string &dir_path, const bool recursive, const bool remove_root) {
    int ret{-1};

    ret = DoTravelDir(dir_path, "", "", nullptr, nullptr, recursive,
            // file before
            [](const string &root_dir_path, const string &relative_file_path, const string &) -> int {
                const string absolute_file_path{root_dir_path + "/" + relative_file_path};

                return RemoveFile(move(absolute_file_path));
            },
            // file after
            {},
            // sub dir before
            {},
            // sub dir after
            [](const string &root_dir_path, const string &relative_sub_dir_path, const string &) -> int {
                const string absolute_sub_dir_path{root_dir_path + "/" + relative_sub_dir_path};

                return RemoveDir(move(absolute_sub_dir_path));
            });

    if (0 != ret) {
        // TODO:
        //QLErr("DoTravelDir \"%s\" err %d", dir_path.c_str(), ret);

        return ret;
    }

    if (remove_root) {
        return RemoveDir(move(dir_path));
    }

    return 0;
}

int RecursiveCopyDir(const string &src_dir_path, const string &dst_dir_path, const bool recursive, const bool copy_root) {
    int ret{-1};

    if (copy_root) {
        ret = CopyDir(move(src_dir_path), move(dst_dir_path));

        if (0 != ret)
            return ret;
    }

    ret = DoTravelDir(src_dir_path, "", dst_dir_path, nullptr, nullptr, recursive,
            // file before
            {},
            // file after
            [](const string &root_src_dir_path, const string &relative_file_path,
                const string &root_dst_dir_path) -> int {
                const string absolute_src_file_path{root_src_dir_path + "/" + relative_file_path};
                const string absolute_dst_file_path{root_dst_dir_path + "/" + relative_file_path};

                return CopyFile(move(absolute_src_file_path), move(absolute_dst_file_path));
            },
            // sub dir before
            [](const string &root_src_dir_path, const string &relative_sub_dir_path,
                const string &root_dst_dir_path) -> int {
                const string absolute_src_sub_dir_path{root_src_dir_path + "/" + relative_sub_dir_path};
                const string absolute_dst_sub_dir_path{root_dst_dir_path + "/" + relative_sub_dir_path};

                return CopyDir(move(absolute_src_sub_dir_path), move(absolute_dst_sub_dir_path));
            },
            // sub dir after
            {});

    if (0 != ret) {
        // TODO:
        //QLErr("DoTravelDir \"%s\" -> \"%s\" err %d",
        //      src_dir_path.c_str(), dst_dir_path.c_str(), ret);

        return ret;
    }

    return 0;
}

// deprecated functions

int CopyFile2(const string &src_file_path, const string &dst_file_path) {
    int src_file{open(src_file_path.c_str(), O_RDONLY, 0)};
    int dst_file{open(dst_file_path.c_str(), O_WRONLY | O_CREAT, 0644)};
    int ret{0};

    if (src_file < 0 || dst_file < 0) {
        ret = errno;
        return ret;
    }

    // struct required, rationale: function stat() exists also
    struct stat src_stat;
    if (-1 == fstat(src_file, &src_stat)) {
        ret = errno;
        // TODO:
        //QLErr("fstat \"%s\" err %d", src_file_path.c_str(), ret);

        close(src_file);
        close(dst_file);

        return ret;
    }

    // TODO(walnuthe): may write fewer bytes than requested
    // should be prepared to retry the call if there were unsent bytes
    // <http://man7.org/linux/man-pages/man2/sendfile.2.html>
    if (-1 == sendfile(dst_file, src_file, 0, src_stat.st_size)) {
        ret = errno;
        // TODO:
        //QLErr("sendfile \"%s\" -> \"%s\" err %d",
        //      src_file_path.c_str(), dst_file_path.c_str(), ret);

        close(src_file);
        close(dst_file);

        return ret;
    }

    if (-1 == close(dst_file)) {
        ret = errno;
        // TODO:
        //QLErr("close \"%s\" err %d", dst_file_path.c_str(), ret);
    }
    if (-1 == close(src_file)) {
        ret = errno;
        // TODO:
        //QLErr("close \"%s\" err %d", src_file_path.c_str(), ret);
    }

    return ret;
}

int RemoveDir2(const string &dir_path, const bool recursive) {
    vector<string> file_paths;
    vector<string> sub_dir_paths;
    int ret{-1};
    int sub_ret{0};

    if (recursive) {
        ret = RecursiveListDir(dir_path, &file_paths, &sub_dir_paths, recursive);
        if (0 != ret) {
            // TODO:
            //QLErr("RecursiveListDir \"%s\" err %d", dir_path.c_str(), ret);

            return ret;
        }

        for (auto &&file_path : file_paths) {
            const string full_file_path{dir_path + "/" + file_path.c_str()};
            if (-1 == unlink(full_file_path.c_str())) {
                if (0 == sub_ret)
                    sub_ret = errno;
                // TODO:
                //QLErr("unlink \"%s\" err %d", file_path.c_str(), errno);
            }
        }

        for (auto it(sub_dir_paths.crbegin()); sub_dir_paths.crend() != it; ++it) {
            const string full_sub_dir_path{dir_path + "/" + it->c_str()};
            if (-1 == rmdir(full_sub_dir_path.c_str())) {
                if (0 == sub_ret)
                    sub_ret = errno;
                // TODO:
                //QLErr("rmdir \"%s\" err %d", it->c_str(), errno);
            }
        }
    }

    if (-1 == rmdir(dir_path.c_str())) {
        // TODO:
        //QLErr("rmdir \"%s\" err %d", dir_path.c_str(), errno);

        return errno;
    }

    if (0 != ret)
        return ret;

    return sub_ret;
}

int DoCopyDir2(const char *relative_src_path, const struct stat *src_stat, int typeflag) {
    // TODO(walnuthe): dest dir
    string dst_dir_path;
    const string relative_dst_path{dst_dir_path + relative_src_path};
    switch(typeflag) {
        case FTW_D:
            mkdir(relative_dst_path.c_str(), src_stat->st_mode);

            break;
        case FTW_F:
            ifstream src(relative_src_path, ios::binary);
            ofstream dst(relative_dst_path, ios::binary);
            dst << src.rdbuf();

            break;
    }

    return 0;
}

int CopyDir2(const string &src_dir_path, const string &dst_dir_path) {
    if (-1 == ftw(src_dir_path.c_str(), DoCopyDir2, FTW_MAX_FD)) {
        // TODO:
        //QLErr("ftw \"%s\" -> \"%s\" err %d",
        //      src_dir_path.c_str(), dst_dir_path.c_str(), errno);

        return errno;
    }

    return 0;
}

bool AccessDir(const std::string &dir_path) {
    return access(dir_path.c_str(), F_OK) == 0;
}

bool CreateDir(const std::string &dir_path) {
    if (!AccessDir(dir_path)) {
        if (mkdir(dir_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
            return false;
        }
    }
    return true;
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

