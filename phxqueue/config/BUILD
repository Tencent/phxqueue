from mm_envs import *



cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'config',
    srcs = [
        'consumerconfig.cpp',
        'globalconfig.cpp',
        'lockconfig.cpp',
        'schedulerconfig.cpp',
        'storeconfig.cpp',
        'topicconfig.cpp',
        'utils/pub_util.cpp',
        'utils/sub_util.cpp',
        'proto/consumerconfig.pb.cc',
        'proto/globalconfig.pb.cc',
        'proto/lockconfig.pb.cc',
        'proto/schedulerconfig.pb.cc',
        'proto/storeconfig.pb.cc',
        'proto/topicconfig.pb.cc',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/plugin:plugin',
    ],
    export_incs = [ '.', ],
    optimize = [
        'O2',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

