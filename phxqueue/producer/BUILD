from mm_envs import *


cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'producer',
    srcs = [
        'producer.cpp',
        'selector.cpp',
        'batchhelper.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/config:config',
        '#rt',
        '#z',
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

