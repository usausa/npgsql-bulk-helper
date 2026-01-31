namespace NpgsqlBulkHelper;

using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.Json;

using Npgsql;

using NpgsqlTypes;

public interface IColumnWriter
{
    ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType);
}

public interface IColumnWriter<in T> : IColumnWriter
{
    ValueTask WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType);
}

//--------------------------------------------------------------------------------
// Generic
//--------------------------------------------------------------------------------

internal sealed class ConvertWriter<T> : IColumnWriter<T>
{
    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
    {
        if (value is T t)
        {
            await writer.WriteAsync(t, providerType).ConfigureAwait(false);
        }
        else
        {
            await writer.WriteAsync((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture), providerType).ConfigureAwait(false);
        }
    }

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class SameTypeWriter<T> : IColumnWriter<T>
{
    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((T)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// DateTime
//--------------------------------------------------------------------------------

internal sealed class DateTimeToTimeWriter : IColumnWriter
{
    public static DateTimeToTimeWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateTime)value).TimeOfDay, providerType).ConfigureAwait(false);
}

internal sealed class DateTimeToTimeTzWriter : IColumnWriter
{
    public static DateTimeToTimeTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(new DateTimeOffset((DateTime)value), providerType).ConfigureAwait(false);
}

internal sealed class DateTimeToTimestampWriter : IColumnWriter
{
    public static DateTimeToTimestampWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.SpecifyKind((DateTime)value, DateTimeKind.Unspecified), providerType).ConfigureAwait(false);
}

internal sealed class DateTimeToTimestampTzWriter : IColumnWriter
{
    public static DateTimeToTimestampTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateTime)value).ToUniversalTime(), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// DateTimeOffset
//--------------------------------------------------------------------------------

internal sealed class DateTimeOffsetToDateWriter : IColumnWriter
{
    public static DateTimeOffsetToDateWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateTimeOffset)value).DateTime, providerType).ConfigureAwait(false);
}

internal sealed class DateTimeOffsetToTimeWriter : IColumnWriter
{
    public static DateTimeOffsetToTimeWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateTimeOffset)value).TimeOfDay, providerType).ConfigureAwait(false);
}

internal sealed class DateTimeOffsetToTimestampWriter : IColumnWriter
{
    public static DateTimeOffsetToTimestampWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(new DateTime(((DateTimeOffset)value).Ticks, DateTimeKind.Unspecified), providerType).ConfigureAwait(false);
}

internal sealed class DateTimeOffsetToTimestampTzWriter : IColumnWriter
{
    public static DateTimeOffsetToTimestampTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(new DateTime(((DateTimeOffset)value).UtcTicks, DateTimeKind.Utc), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// DateOnly
//--------------------------------------------------------------------------------

internal sealed class DateOnlyToDateWriter : IColumnWriter
{
    public static DateOnlyToDateWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue), providerType).ConfigureAwait(false);
}

internal sealed class DateOnlyToTimeWriter : IColumnWriter
{
    public static DateOnlyToTimeWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue).TimeOfDay, providerType).ConfigureAwait(false);
}

internal sealed class DateOnlyToTimeTzWriter : IColumnWriter
{
    public static DateOnlyToTimeTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(new DateTimeOffset(((DateOnly)value).ToDateTime(TimeOnly.MinValue)), providerType).ConfigureAwait(false);
}

internal sealed class DateOnlyToTimestampWriter : IColumnWriter
{
    public static DateOnlyToTimestampWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.SpecifyKind(((DateOnly)value).ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified), providerType).ConfigureAwait(false);
}

internal sealed class DateOnlyToTimestampTzWriter : IColumnWriter
{
    public static DateOnlyToTimestampTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue).ToUniversalTime(), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// TimeOnly
//--------------------------------------------------------------------------------

internal sealed class TimeOnlyToTimeWriter : IColumnWriter
{
    public static TimeOnlyToTimeWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((TimeOnly)value).ToTimeSpan(), providerType).ConfigureAwait(false);
}

internal sealed class TimeOnlyToTimeTzWriter : IColumnWriter
{
    public static TimeOnlyToTimeTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(new DateTimeOffset(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value)), providerType).ConfigureAwait(false);
}

internal sealed class TimeOnlyToTimestampWriter : IColumnWriter
{
    public static TimeOnlyToTimestampWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.SpecifyKind(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value), DateTimeKind.Unspecified), providerType).ConfigureAwait(false);
}

internal sealed class TimeOnlyToTimestampTzWriter : IColumnWriter
{
    public static TimeOnlyToTimestampTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value).ToUniversalTime(), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// String
//--------------------------------------------------------------------------------

internal sealed class StringToDateWriter : IColumnWriter
{
    public static StringToDateWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture), providerType).ConfigureAwait(false);
}

internal sealed class StringToTimeWriter : IColumnWriter
{
    public static StringToTimeWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture).TimeOfDay, providerType).ConfigureAwait(false);
}

internal sealed class StringToTimeTzWriter : IColumnWriter
{
    public static StringToTimeTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTimeOffset.Parse((string)value, CultureInfo.InvariantCulture), providerType).ConfigureAwait(false);
}

internal sealed class StringToTimestampWriter : IColumnWriter
{
    public static StringToTimestampWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.SpecifyKind(DateTime.Parse((string)value, CultureInfo.InvariantCulture), DateTimeKind.Unspecified), providerType).ConfigureAwait(false);
}

internal sealed class StringToTimestampTzWriter : IColumnWriter
{
    public static StringToTimestampTzWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture).ToUniversalTime(), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// JSON
//--------------------------------------------------------------------------------

internal sealed class StringToJsonWriter : IColumnWriter
{
    public static StringToJsonWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((string)value, providerType).ConfigureAwait(false);
}

internal sealed class JsonDocumentToJsonWriter : IColumnWriter
{
    public static JsonDocumentToJsonWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((JsonDocument)value).RootElement.GetRawText(), providerType).ConfigureAwait(false);
}

internal sealed class JsonElementToJsonWriter : IColumnWriter
{
    public static JsonElementToJsonWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(((JsonElement)value).GetRawText(), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Network
//--------------------------------------------------------------------------------

internal sealed class IpAddressWriter : IColumnWriter
{
    public static IpAddressWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((IPAddress)value, providerType).ConfigureAwait(false);
}

internal sealed class StringToIpAddressWriter : IColumnWriter
{
    public static StringToIpAddressWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(IPAddress.Parse((string)value), providerType).ConfigureAwait(false);
}

internal sealed class PhysicalAddressWriter : IColumnWriter
{
    public static PhysicalAddressWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((PhysicalAddress)value, providerType).ConfigureAwait(false);
}

internal sealed class StringToPhysicalAddressWriter : IColumnWriter
{
    public static StringToPhysicalAddressWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(PhysicalAddress.Parse((string)value), providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Array
//--------------------------------------------------------------------------------

internal sealed class ArrayWriter<T> : IColumnWriter
{
    public static ArrayWriter<T> Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((T[])value, providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Geometry
//--------------------------------------------------------------------------------

internal sealed class PointWriter : IColumnWriter<NpgsqlPoint>
{
    public static PointWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlPoint)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPoint value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class LineWriter : IColumnWriter<NpgsqlLine>
{
    public static LineWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlLine)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlLine value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class LSegWriter : IColumnWriter<NpgsqlLSeg>
{
    public static LSegWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlLSeg)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlLSeg value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class BoxWriter : IColumnWriter<NpgsqlBox>
{
    public static BoxWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlBox)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlBox value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class PathWriter : IColumnWriter<NpgsqlPath>
{
    public static PathWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlPath)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPath value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class PolygonWriter : IColumnWriter<NpgsqlPolygon>
{
    public static PolygonWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlPolygon)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPolygon value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class CircleWriter : IColumnWriter<NpgsqlCircle>
{
    public static CircleWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlCircle)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlCircle value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Interval
//--------------------------------------------------------------------------------

internal sealed class IntervalWriter : IColumnWriter<NpgsqlInterval>
{
    public static IntervalWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlInterval)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlInterval value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

internal sealed class TimeSpanToIntervalWriter : IColumnWriter<TimeSpan>
{
    public static TimeSpanToIntervalWriter Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((TimeSpan)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, TimeSpan value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Range
//--------------------------------------------------------------------------------

internal sealed class RangeWriter<T> : IColumnWriter<NpgsqlRange<T>>
{
    public static RangeWriter<T> Instance { get; } = new();

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        await writer.WriteAsync((NpgsqlRange<T>)value, providerType).ConfigureAwait(false);

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, NpgsqlRange<T> value, NpgsqlDbType providerType) =>
        await writer.WriteAsync(value, providerType).ConfigureAwait(false);
}

//--------------------------------------------------------------------------------
// Custom Writer Wrapper
//--------------------------------------------------------------------------------

internal sealed class DelegateWriter<T> : IColumnWriter<T>
{
    private readonly Func<T, object> converter;

    public DelegateWriter(Func<T, object> converter)
    {
        this.converter = converter;
    }

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
    {
        var converted = value is T t ? converter(t) : converter((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
        await writer.WriteAsync(converted, providerType).ConfigureAwait(false);
    }

    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType)
    {
        var converted = converter(value);
        await writer.WriteAsync(converted, providerType).ConfigureAwait(false);
    }
}
