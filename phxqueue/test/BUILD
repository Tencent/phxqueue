cc_binary(
    name = 'test_config_main',
    srcs = [
        'test_config_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        ':test_config',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_consumer_main',
    srcs = [
        'test_consumer_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        ':simpleconsumer',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_producer_main',
    srcs = [
        'test_producer_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        ':simpleproducer',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_store_main',
    srcs = [
        'test_store_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/store:store',
        '//phxqueue/phxqueue/plugin:plugin',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_scheduler_main',
    srcs = [
        'test_scheduler_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        ':simplescheduler',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_lock_main',
    srcs = [
        'test_lock_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/lock:lock',
        '//phxqueue/phxqueue/plugin:plugin',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)


cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'test_config',
    srcs = [
        'test_config.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/config:config',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)


cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'simpleconsumer',
    srcs = [
        'simpleconsumer.cpp',
        'simplehandler.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/config:config',
        '//phxqueue/phxqueue/consumer:consumer',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'simpleproducer',
    srcs = [
        'simpleproducer.cpp'
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/producer:producer',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'simplescheduler',
    srcs = [
        'simplescheduler.cpp'
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/scheduler:scheduler',
    ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)







cc_binary(
    name = 'test_plugin_main',
    srcs = [
        'test_plugin_main.cpp',
    ],
    deps = [
        ':test_configfactory',
        ':test_log',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)


cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'test_configfactory',
    srcs = [
        'test_configfactory.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/config:config',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)


cc_library(
    target_bases = [ 'mmcomm', ],
    name = 'test_log',
    srcs = [
        'test_log.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        '//phxqueue/phxqueue/comm:comm',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)


cc_binary(
    name = 'test_notifierpool_main',
    srcs = [
        'test_notifierpool_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        '//phxqueue/phxqueue/comm:comm',
    ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)

cc_binary(
    name = 'test_get_queues_by_pub_id_main',
    srcs = [
        'test_get_queues_by_pub_id_main.cpp',
    ],
    deps = [
        '//phxqueue:include',
        '//phxqueue/phxqueue/plugin:plugin',
        '//phxqueue/phxqueue/comm:comm',
        '//phxqueue/phxqueue/config:config',
        '//phxqueue/phxqueue/consumer:consumer',
   ],
    export_incs = [ '.', ],
    extra_cppflags = [
        '-Wall',
        '-Werror',
        '-std=c++11',
    ]
)
