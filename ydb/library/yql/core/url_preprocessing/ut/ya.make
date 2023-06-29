UNITTEST_FOR(ydb/library/yql/core/url_preprocessing)

SRCS(
    pattern_group_ut.cpp
    url_mapper_ut.cpp
    url_preprocessing_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/proto
    ydb/library/yql/core/file_storage/proto
    library/cpp/resource
    library/cpp/protobuf/util
    contrib/libs/protobuf
)

RESOURCE(
    yql/cfg/tests/gateways.conf gateways.conf
)

END()
