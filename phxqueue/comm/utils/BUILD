cc_library(
    name = 'utils',
    srcs = [
        'addr_util.cpp',
        'file_util.cpp',
        'load_util.cpp',
        'memory_util.cpp',
        'other_util.cpp',
        'string_util.cpp',
        'time_util.cpp',
        'benchmark_util.cpp',
        'co_util.cpp',
        'hash_util.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/third_party/colib:colib',
        '#pthread',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)
