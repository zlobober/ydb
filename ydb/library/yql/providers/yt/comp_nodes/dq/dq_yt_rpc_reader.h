#pragma once

#include "dq_yt_reader_impl.h"

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

class TDqYtReadWrapperRPC : public TDqYtReadWrapperBase<TDqYtReadWrapperRPC> {
public:
    TDqYtReadWrapperRPC(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables, NKikimr::NMiniKQL::IStatsRegistry* jobStats)
            : TDqYtReadWrapperBase<TDqYtReadWrapperRPC>(ctx, clusterName, token, inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables), jobStats) {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const;
};
}
