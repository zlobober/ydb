#include "url_preprocessing.h"

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/stream/file.h>

using namespace NYql;
using namespace NThreading;

Y_UNIT_TEST_SUITE(TUrlPreprocessingTests) {

    Y_UNIT_TEST(AllowedUrls) {
        {
            TGatewaysConfig cfg;
            cfg.MutableFs()->AddAllowedUrls()->SetPattern("^XXXX$");
            cfg.MutableFs()->AddExternalAllowedUrls()->SetPattern("^http://localhost:");
            {
                // not accepted
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(false, cfg);

                UNIT_ASSERT_EXCEPTION_CONTAINS(preproc->Preprocess("http://localhost:9999"), std::exception, "It is not allowed to download url http://localhost:");
            }
            {
                // accepted for external
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(true, cfg);

                UNIT_ASSERT_VALUES_EQUAL(std::make_pair(TString("http://localhost:9999"), TString()), preproc->Preprocess("http://localhost:9999"));
            }
        }

        {
            TGatewaysConfig cfg;
            cfg.MutableFs()->AddAllowedUrls()->SetPattern("^http://localhost:");
            cfg.MutableFs()->AddExternalAllowedUrls()->SetPattern("^XXXX$");

            {
                // accepted
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(false, cfg);

                UNIT_ASSERT_VALUES_EQUAL(std::make_pair(TString("http://localhost:9999"), TString()), preproc->Preprocess("http://localhost:9999"));
            }
            {
                // not accepted for external
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(true, cfg);

                UNIT_ASSERT_EXCEPTION_CONTAINS(preproc->Preprocess("http://localhost:9999"), std::exception, "It is not allowed to download url http://localhost:");
            }
        }

        {
            TGatewaysConfig cfg;
            {
                // not accepted for external without any allowed
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(true, cfg);

                UNIT_ASSERT_EXCEPTION_CONTAINS(preproc->Preprocess("http://localhost:9999"), std::exception, "It is not allowed to download url http://localhost:");
            }
        }

        {
            TGatewaysConfig cfg;
            {
                // accepted without any allowed
                TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
                preproc->Configure(false, cfg);

                UNIT_ASSERT_VALUES_EQUAL(std::make_pair(TString("http://localhost:9999"), TString()), preproc->Preprocess("http://localhost:9999"));
            }
        }

        {
            TGatewaysConfig cfg;
            auto sch = cfg.MutableFs()->AddCustomSchemes();
            sch->SetPattern("^local");
            sch->SetTargetUrl("http://localhost:9999");
            cfg.MutableFs()->AddAllowedUrls()->SetPattern("^http://localhost:");

            // accepted
            TUrlPreprocessing::TPtr preproc = new TUrlPreprocessing();
            preproc->Configure(false, cfg);

            UNIT_ASSERT_VALUES_EQUAL(std::make_pair(TString("http://localhost:9999"), TString()), preproc->Preprocess("local"));
        }
    }
}
