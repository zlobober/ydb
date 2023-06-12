#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormat
{
public:
    TFormat();
    TFormat(const TFormat& other);
    TFormat(EFormatType type, const NYTree::IAttributeDictionary* attributes = nullptr);

    TFormat& operator = (const TFormat& other);

    DEFINE_BYVAL_RO_PROPERTY(EFormatType, Type);

    const NYTree::IAttributeDictionary& Attributes() const;

private:
    NYTree::IAttributeDictionaryPtr Attributes_;

};

void Serialize(const TFormat& value, NYson::IYsonConsumer* consumer);
void Deserialize(TFormat& value, NYTree::INodePtr node);
void Deserialize(TFormat& value, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessFormatWriter
    : public NTableClient::IUnversionedRowsetWriter
{
    virtual TBlob GetContext() const = 0;

    virtual i64 GetWrittenSize() const = 0;

    virtual void Flush() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

// This function historically creates format for reading dynamic tables.
// It slightly differs from format for static tables. :(
NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& Format,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IVersionedWriterPtr CreateVersionedWriterForFormat(
    const TFormat& Format,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateStaticTableWriterForFormat(
    const TFormat& format,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    IZeroCopyOutput* output);

NYson::TYsonProducer CreateProducerForFormat(
    const TFormat& format,
    EDataType dataType,
    IInputStream* input);

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    EDataType dataType,
    NYson::IYsonConsumer* consumer);

//! Create own parser for each value consumer.
std::vector<std::unique_ptr<IParser>> CreateParsersForFormat(
    const TFormat& format,
    const std::vector<NTableClient::IValueConsumer*>& valueConsumers);

//! Create parser for value consumer. Helper for previous method in singular case.
std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    NTableClient::IValueConsumer* valueConsumer);

////////////////////////////////////////////////////////////////////////////////

void ConfigureEscapeTable(const TSchemafulDsvFormatConfigPtr& config, TEscapeTable* escapeTable);

void ConfigureEscapeTables(
    const TDsvFormatConfigBasePtr& config,
    bool addCarriageReturn,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

void ConfigureEscapeTables(
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    bool escapingForWriter,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
