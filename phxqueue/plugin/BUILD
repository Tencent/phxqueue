from mm_envs import *

cc_library(
    name = 'plugin',
    srcs = [
        'breakpointfactory.cpp',
        'configfactory.cpp',
        'logger_google.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/third_party/glog:glog',
    ],
    export_incs = [ '.' ],
    optimize = [
        'O2',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ],
)

