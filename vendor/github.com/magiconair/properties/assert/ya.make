GO_LIBRARY()

LICENSE(BSD-2-Clause)

VERSION(v1.8.7)

SRCS(
    assert.go
)

GO_TEST_SRCS(assert_test.go)

END()

RECURSE(
    gotest
)
