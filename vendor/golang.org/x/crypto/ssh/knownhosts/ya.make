GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.27.0)

SRCS(
    knownhosts.go
)

GO_TEST_SRCS(knownhosts_test.go)

END()

RECURSE(
    gotest
)
