cc_library(
    name = 'utils',
    srcs = [
        'pub_util.cpp',
        'sub_util.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '#pthread',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)
