from mm_envs import *

cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'consumer',
    srcs = [
        'hblock.cpp',
        'consumer.cpp',
        'freqman.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/comm:comm',
        #'//basic/colib:colib',
        '//phxqueue/third_party/colib:colib',
        '#rt',
        '#z',
        '#dl',
    ],
    optimize = [
        'O2',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
   ]
)

