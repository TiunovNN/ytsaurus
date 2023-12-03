INCLUDE(../../pycxx.inc)

PY23_NATIVE_LIBRARY()

CXXFLAGS(
    ${PYCXX_FLAGS}
)

SRCS(
    GLOBAL parquet.cpp
)

PEERDIR(
    yt/yt/python/common
    contrib/libs/pycxx
    contrib/libs/apache/arrow
)

ADDINCL(
    GLOBAL contrib/libs/pycxx
)

END()
