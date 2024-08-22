LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cypress_file_block_device.cpp
    dynamic_table_block_device.cpp
    memory_block_device.cpp
    profiler.cpp
    random_access_file_reader.cpp
    server.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/ytlib
    yt/yt_proto/yt/client
)

END()
