from mm_envs import *

cc_library(
    name = 'comm',
    srcs = [
        'proto/comm.pb.cc',
        'breakpoint.cpp',
        'masterclient.cpp',
        'multiproc.cpp',
        'logger.cpp',
        'notifierpool.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        '//phxqueue/third_party/protobuf:protobuf',
        '//phxqueue/phxqueue/comm/utils:utils',

    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)
