# Generated by devtools/yamaker from nixpkgs 22.05.

LIBRARY()

VERSION(2.0.0)

ORIGINAL_SOURCE(https://github.com/google/flatbuffers/archive/v2.0.0.tar.gz)

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

ADDINCL(
    contrib/libs/flatbuffers/include
)

NO_COMPILER_WARNINGS()

NO_UTIL()

CFLAGS(
    -DFLATBUFFERS_LOCALE_INDEPENDENT=1
)

SRCS(
    src/idl_gen_text.cpp
    src/idl_parser.cpp
    src/reflection.cpp
    src/util.cpp
)

END()

RECURSE(
    flatc
    samples
)