#include "url_mapper.h"

#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/str.h>

#include <google/protobuf/text_format.h>

using namespace NYql;

namespace {
void RemapAndCheck(const TUrlMapper& m, const TString& input, const TString& expectedOutput) {
    TString output;
    UNIT_ASSERT_C(m.MapUrl(input, output), input);
    UNIT_ASSERT_VALUES_EQUAL(output, expectedOutput);
}
}

Y_UNIT_TEST_SUITE(TUrlMapperTests) {
    Y_UNIT_TEST(All) {
        TGatewaysConfig config;
        {
            auto data = NResource::Find("gateways.conf");
            TStringInput in(data);
            ParseFromTextFormat(in, config, EParseFromTextFormatOption::AllowUnknownField);
        }

        TUrlMapper m;
        for (const auto& sc : config.GetFs().GetCustomSchemes()) {
            m.AddMapping(sc.GetPattern(), sc.GetTargetUrl());
        }

        TString tmp;
        UNIT_ASSERT(!m.MapUrl("http://ya.ru", tmp));

        RemapAndCheck(m, "sbr:654321", "https://proxy.sandbox.yandex-team.ru/654321");
        RemapAndCheck(m, "sbr://654321", "https://proxy.sandbox.yandex-team.ru/654321");

        // compatibility
        RemapAndCheck(m, "yt://hahn/statbox/cube/files/dicts/UrlToGroups.yaml@t=9797-164F64-3FE0190-DD3C4184A", "yt://hahn/statbox/cube/files/dicts/UrlToGroups.yaml?transaction_id=9797-164F64-3FE0190-DD3C4184A");

        RemapAndCheck(m, "https://hahn.yt.yandex-team.ru/api/v3/read_file?path=//statbox/cube/files/dicts/UrlToGroups.yaml&disposition=attachment&dump_error_into_response=true", "yt://hahn/?path=//statbox/cube/files/dicts/UrlToGroups.yaml&disposition=attachment&dump_error_into_response=true");
        RemapAndCheck(m, "https://hahn.yt.yandex-team.ru/api/v3/read_file?path=%2F%2Fstatbox%2Fcube%2Ffiles%2Fdicts%2FUrlToGroups.yaml&disposition=attachment&dump_error_into_response=true&transaction_id=0-0-0-0", "yt://hahn/?path=%2F%2Fstatbox%2Fcube%2Ffiles%2Fdicts%2FUrlToGroups.yaml&disposition=attachment&dump_error_into_response=true&transaction_id=0-0-0-0");
        RemapAndCheck(m, "https://hahn.yt.yandex.net/api/v3/read_file?disposition=attachment&path=//tmp/nile/files/parser.so&transaction_id=266ee-5bfd14-3fe0004-57b3", "yt://hahn/?disposition=attachment&path=//tmp/nile/files/parser.so&transaction_id=266ee-5bfd14-3fe0004-57b3");

        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/#page=navigation&path=//home/yql/dev/1.txt", "yt://hahn/?path=//home/yql/dev/1.txt");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/#page=navigation&path=//home/yql/dev/1.txt&t=0-0-0-0", "yt://hahn/?path=//home/yql/dev/1.txt&t=0-0-0-0");

        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/navigation?path=//statbox/cube/files/dicts/UrlToGroups.yaml", "yt://hahn/?path=//statbox/cube/files/dicts/UrlToGroups.yaml");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/navigation?t=0-0-0-0&path=//statbox/cube/files/dicts/UrlToGroups.yaml", "yt://hahn/?t=0-0-0-0&path=//statbox/cube/files/dicts/UrlToGroups.yaml");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/navigation?t=0-0-0-0&path=//statbox/cube/files/dicts/UrlToGroups.yaml&a=1&b=2", "yt://hahn/?t=0-0-0-0&path=//statbox/cube/files/dicts/UrlToGroups.yaml&a=1&b=2");
        RemapAndCheck(m, "https://yt.yandex-team.ru/hahn/navigation?path=//statbox/cube/files/dicts/UrlToGroups.yaml&t=0-0-0-0", "yt://hahn/?path=//statbox/cube/files/dicts/UrlToGroups.yaml&t=0-0-0-0");

        // with dash
        RemapAndCheck(m, "https://yt.yandex-team.ru/seneca-sas/navigation?path=//statbox/cube/files/dicts/UrlToGroups.yaml", "yt://seneca-sas/?path=//statbox/cube/files/dicts/UrlToGroups.yaml");
        // with underscore and digits
        RemapAndCheck(m, "https://yt.yandex-team.ru/seneca_sas0123456789/navigation?path=//tmp/bsistat02i_202624_1523866684_inclusion_already_moderating", "yt://seneca_sas0123456789/?path=//tmp/bsistat02i_202624_1523866684_inclusion_already_moderating");

        RemapAndCheck(m, "https://a.yandex-team.ru/arc/trunk/arcadia/yql/ya.make?rev=5530789", "arc:/yql/ya.make?rev=5530789&branch=trunk");
        RemapAndCheck(m, "https://a.yandex-team.ru/arc/trunk/arcadia/yql/ya.make?rev=r5530789", "arc:/yql/ya.make?rev=5530789&branch=trunk");
        RemapAndCheck(m, "https://a.yandex-team.ru/arc/branches/yql/yql-stable-2019-08-16/arcadia/yql/ya.make?rev=5530789", "arc:/yql/ya.make?rev=5530789&branch=branches/yql/yql-stable-2019-08-16");
        // New arcanum url style
        RemapAndCheck(m, "https://a.yandex-team.ru/svn/trunk/arcadia/yql/ya.make?rev=5530789", "arc:/yql/ya.make?rev=5530789&branch=trunk");
        RemapAndCheck(m, "https://a.yandex-team.ru/arcadia/yql/ya.make?rev=r9095022", "arc:/yql/ya.make?rev=9095022");
        RemapAndCheck(m, "https://a.yandex-team.ru/arcadia/yql/ya.make?rev=094b915a5eff5f9f535affe8d000d8e825a43ffe", "arc:/yql/ya.make?hash=094b915a5eff5f9f535affe8d000d8e825a43ffe");
        // Arcanum domain
        RemapAndCheck(m, "https://arcanum.yandex-team.ru/arcadia/yql/ya.make?rev=r9095022", "arc:/yql/ya.make?rev=9095022");
        // First path component with underscore (YQL-15137)
        RemapAndCheck(m, "https://a.yandex-team.ru/arcadia/client_method/post_campaign/geo.sql?rev=r11117238", "arc:/client_method/post_campaign/geo.sql?rev=11117238");

        UNIT_ASSERT(!m.MapUrl("https://a.yandex-team.ru/arc/trunk/arcadia/yql/ya.make", tmp));
    }
}
