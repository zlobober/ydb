#include "dq_yt_rpc_reader.h"

#include "yt/cpp/mapreduce/common/helpers.h"

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/api/rpc_proxy/client_impl.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/row_stream.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

namespace {
    NYT::NYPath::TRichYPath ConvertYPathFromOld(const NYT::TRichYPath& richYPath) {
        NYT::NYPath::TRichYPath tableYPath(richYPath.Path_);
        const auto& rngs = richYPath.GetRanges();
        if (rngs) {
            TVector<NYT::NChunkClient::TReadRange> ranges;
            for (const auto& rng: *rngs) {
                auto& range = ranges.emplace_back();
                if (rng.LowerLimit_.Offset_) {
                    range.LowerLimit().SetOffset(*rng.LowerLimit_.Offset_);
                }

                if (rng.LowerLimit_.TabletIndex_) {
                    range.LowerLimit().SetTabletIndex(*rng.LowerLimit_.TabletIndex_);
                }

                if (*rng.LowerLimit_.RowIndex_) {
                    range.LowerLimit().SetRowIndex(*rng.LowerLimit_.RowIndex_);
                }

                if (rng.UpperLimit_.Offset_) {
                    range.UpperLimit().SetOffset(*rng.UpperLimit_.Offset_);
                }

                if (rng.UpperLimit_.TabletIndex_) {
                    range.UpperLimit().SetTabletIndex(*rng.UpperLimit_.TabletIndex_);
                }

                if (*rng.UpperLimit_.RowIndex_) {
                    range.UpperLimit().SetRowIndex(*rng.UpperLimit_.RowIndex_);
                }
            }
            tableYPath.SetRanges(std::move(ranges));
        }

        if (richYPath.Columns_) {
            tableYPath.SetColumns(richYPath.Columns_->Parts_);
        }

        return tableYPath;
    }
}

namespace {
class TRPCRawReader : public NYT::TRawTableReader {
public:
    TRPCRawReader(NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& inputStream
                , NYT::NApi::IConnectionPtr connection
                , NYT::TIntrusivePtr<NYT::NApi::NRpcProxy::TClient> client)
                : NYT::TRawTableReader()
                , InputStream_(inputStream)
                , Connection_(connection)
                , Client_(client)
    {

    }

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex) override {
        Y_UNUSED(rangeIndex);
        Y_UNUSED(rowIndex);
        return false;
    }

    void ResetRetries() override {

    }

    bool HasRangeIndices() const override {
        return true;
    };

    size_t DoRead(void* buf, size_t len) override {
        if (IsEof_) {
            return 0;
        }

        for(;;) {

            if (!PayloadStream_.Exhausted()) {
                return PayloadStream_.Read(buf, len);
            }

            CurrentPayload_.Reset();
            while (auto block = NYT::NConcurrency::WaitFor(InputStream_->Read()).ValueOrThrow()) {
                NYT::NApi::NRpcProxy::NProto::TRowsetDescriptor descriptor;
                NYT::NApi::NRpcProxy::NProto::TRowsetStatistics statistics;

                CurrentPayload_ = NYT::NApi::NRpcProxy::DeserializeRowStreamBlockEnvelope(block, &descriptor, &statistics);

                // skip no-skiff data
                if (descriptor.rowset_format() != NYT::NApi::NRpcProxy::NProto::RF_FORMAT) {
                    CurrentPayload_.Reset();
                    continue;
                }
                if (CurrentPayload_.Empty()) {
                    break;
                }

                PayloadStream_ = TMemoryInput(CurrentPayload_.Begin(), CurrentPayload_.Size());

                break;
            }

            if (CurrentPayload_.Empty()) {
                IsEof_ = true;
                return 0;
            }
        }
    };

    virtual ~TRPCRawReader() override {
    }
private:
    const NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr InputStream_;
    const NYT::NApi::IConnectionPtr Connection_;
    const NYT::TIntrusivePtr<NYT::NApi::NRpcProxy::TClient> Client_;
    NYT::TSharedRef CurrentPayload_;
    TMemoryInput PayloadStream_;
    bool IsEof_ = false;
};
}

void TDqYtReadWrapperRPC::MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->ClusterUrl = ClusterName;
    auto connection = CreateConnection(connectionConfig);

    auto clientOptions = NYT::NApi::TClientOptions();

    if (Token) {
        clientOptions.Token = Token;
    }

    auto client = DynamicPointerCast<NYT::NApi::NRpcProxy::TClient>(connection->CreateClient(clientOptions));
    Y_VERIFY(client);
    auto apiServiceProxy = client->CreateApiServiceProxy();

    TVector<NYT::TFuture<NYT::TRawTableReaderPtr>> waitFor;

    for (auto [richYPath, format]: Tables) {
        auto request = apiServiceProxy.ReadTable();
        client->InitStreamingRequest(*request);

        TString ppath;
        auto tableYPath = ConvertYPathFromOld(richYPath);

        NYT::NYPath::ToProto(&ppath, tableYPath);
        request->set_path(ppath);
        request->set_desired_rowset_format(NYT::NApi::NRpcProxy::NProto::ERowsetFormat::RF_FORMAT);

        request->set_enable_table_index(true);
        request->set_enable_range_index(true);
        request->set_enable_row_index(true);

        // https://a.yandex-team.ru/arcadia/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto?rev=r11519304#L2338
        if (!SamplingSpec.IsUndefined()) {
            TStringStream ss;
            SamplingSpec.Save(&ss);
            request->set_config(ss.Str());
        }

        if (richYPath.TransactionId_) {
            ui32* dw = richYPath.TransactionId_->dw;
            // P. S. No proper way to convert it
            request->mutable_transactional_options()->mutable_transaction_id()->set_first((ui64)dw[3] | (ui64(dw[2]) << 32));
            request->mutable_transactional_options()->mutable_transaction_id()->set_second((ui64)dw[1] | (ui64(dw[0]) << 32));
        }

        // Get skiff format yson string
        TStringStream fmt;
        format.Config.Save(&fmt);
        request->set_format(fmt.Str());

        waitFor.emplace_back(std::move(CreateRpcClientInputStream(std::move(request)).ApplyUnique(BIND([](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            return stream->Read().Apply(BIND([stream = std::move(stream)](const NYT::TSharedRef&) {
                return stream;
            }));
        })).ApplyUnique(BIND([connection, client](NYT::NConcurrency::IAsyncZeroCopyInputStreamPtr&& stream) {
            return std::move(NYT::TRawTableReaderPtr(MakeIntrusive<TRPCRawReader>(std::move(stream), connection, client)));
        }))));
    }

    TVector<NYT::TRawTableReaderPtr> rawReaders;
    std::vector<NYT::TRawTableReaderPtr> readers(std::move(NYT::NConcurrency::WaitFor(NYT::AllSucceeded(waitFor)).ValueOrThrow()));
    rawReaders.swap(readers);
    state = ctx.HolderFactory.Create<TDqYtReadWrapperBase<TDqYtReadWrapperRPC>::TState>(Specs, ctx.HolderFactory, std::move(rawReaders));
}
}
