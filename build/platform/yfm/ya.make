RESOURCES_LIBRARY()

IF(NOT HOST_OS_LINUX AND NOT HOST_OS_WINDOWS AND NOT HOST_OS_DARWIN)
    MESSAGE(FATAL_ERROR Unsupported platform for YFM tool)
ENDIF()

DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE(
    YFM_TOOL
    sbr:7414121829 FOR DARWIN-ARM64
    sbr:7414121829 FOR DARWIN
    sbr:7414134699 FOR LINUX
    sbr:7414146467 FOR WIN32
)

END()
