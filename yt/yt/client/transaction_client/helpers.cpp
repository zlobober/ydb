#include "helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/guid.h>

namespace NYT::NTransactionClient {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

bool IsMasterTransactionId(TTransactionId id)
{
    auto type = TypeFromId(id);
    // NB: Externalized transactions are for internal use only.
    return type == NObjectClient::EObjectType::Transaction ||
        type == NObjectClient::EObjectType::NestedTransaction ||
        type == EObjectType::UploadTransaction ||
        type == EObjectType::UploadNestedTransaction;
}

void ValidateTabletTransactionId(TTransactionId id)
{
    auto type = TypeFromId(id);
    if (type != EObjectType::Transaction &&
        type != EObjectType::AtomicTabletTransaction &&
        type != EObjectType::NonAtomicTabletTransaction)
    {
        THROW_ERROR_EXCEPTION("%v is not a valid tablet transaction id",
            id);
    }
}

void ValidateMasterTransactionId(TTransactionId id)
{
    if (!IsMasterTransactionId(id)) {
        THROW_ERROR_EXCEPTION("%v is not a valid master transaction id",
            id);
    }
}

std::pair<TInstant, TInstant> TimestampToInstant(TTimestamp timestamp)
{
    auto instant = TInstant::Seconds(UnixTimeFromTimestamp(timestamp));
    return {
        instant,
        instant + TDuration::Seconds(1)
    };
}

std::pair<TTimestamp, TTimestamp> InstantToTimestamp(TInstant instant)
{
    auto time = instant.Seconds();
    return {
        TimestampFromUnixTime(time),
        TimestampFromUnixTime(time + 1)
    };
}

std::pair<TDuration, TDuration> TimestampDiffToDuration(TTimestamp loTimestamp, TTimestamp hiTimestamp)
{
    YT_ASSERT(loTimestamp <= hiTimestamp);
    auto loInstant = TimestampToInstant(loTimestamp);
    auto hiInstant = TimestampToInstant(hiTimestamp);
    return std::make_pair(
        hiInstant.first >= loInstant.second ? hiInstant.first - loInstant.second : TDuration::Zero(),
        hiInstant.second - loInstant.first);
}

ui64 UnixTimeFromTimestamp(TTimestamp timestamp)
{
    return timestamp >> TimestampCounterWidth;
}

TTimestamp TimestampFromUnixTime(ui64 time)
{
    return time << TimestampCounterWidth;
}

TTimestamp EmbedCellTagIntoTimestamp(TTimestamp timestamp, NObjectClient::TCellTag cellTag)
{
    static_assert(sizeof(TCellTag) == 2, "Invalid TCellTag size");

    YT_VERIFY((timestamp & ((1ull << TimestampCounterWidth) - 1)) == 0);

    return timestamp ^ (cellTag << (TimestampCounterWidth - 16));
}

bool CanAdvanceTimestampWithEmbeddedCellTag(TTimestamp timestamp, int delta)
{
    static_assert(sizeof(TCellTag) == 2, "Invalid TCellTag size");

    return (timestamp >> (TimestampCounterWidth - 16)) ==
        ((timestamp + delta) >> (TimestampCounterWidth - 16));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
