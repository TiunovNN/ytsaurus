# Use UNION instead PACKAGE to avoid artifact duplication; details in DEVTOOLSSUPPORT-15693
UNION()

INCLUDE(${ARCADIA_ROOT}/yt/packages/ya.make.common)

FROM_SANDBOX(
    # YT_ALL
    FILE 6862365046
    OUT ytserver-all RENAME result/ytserver-all
    EXECUTABLE
)

FROM_SANDBOX(
    # YT_LOCAL_BIN
    FILE 6862365585
    OUT yt_local RENAME result/yt_local
    EXECUTABLE
)

END()
