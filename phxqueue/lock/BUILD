from mm_envs import *

cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'lock',
    srcs = [
        'proto/lock.pb.cc',
        'cleanthread.cpp',
        'keepmasterthread.cpp',
        'lock.cpp',
        'lockdb.cpp',
        'lockmgr.cpp',
        'locksm.cpp',
        'lockutils.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/config:config',

        '//third_party/leveldb:leveldb',
        '//phxpaxos:phxpaxos',
        '//phxpaxos/plugin/include:include',
        '//phxpaxos/plugin:phxpaxos_plugin',
        '//phxpaxos/skplugin/include:include',
        '//phxpaxos/skplugin:phxpaxos_skplugin',
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

