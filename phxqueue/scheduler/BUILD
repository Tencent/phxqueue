from mm_envs import *

cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'scheduler',
    srcs = [
        'keepmasterthread.cpp',
        'loadbalancethread.cpp',
        'scheduler.cpp',
        'schedulermgr.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/config:config',
    ],
    optimize = [
        'O2',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ],
)

