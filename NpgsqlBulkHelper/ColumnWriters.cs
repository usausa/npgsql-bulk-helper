namespace NpgsqlBulkHelper;

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Text.Json;

using Npgsql;

using NpgsqlTypes;

public interface IColumnWriter
{
    Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType);
}

public interface IColumnWriter<in T> : IColumnWriter
{
#pragma warning disable IDE0051
    Task WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType);
#pragma warning restore IDE0051
}

//--------------------------------------------------------------------------------
// Generic
//--------------------------------------------------------------------------------

internal sealed class ConvertWriter<T> : IColumnWriter<T>
{
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026", Justification = "Convert.ChangeType is only called for IConvertible primitive types registered in static constructor.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Convert.ChangeType is only called for IConvertible primitive types registered in static constructor.")]
    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
    {
        if (value is T t)
        {
            return writer.WriteAsync(t, providerType);
        }
        return writer.WriteAsync((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture), providerType);
    }

    public Task WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class SameTypeWriter<T> : IColumnWriter<T>
{
    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((T)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

//--------------------------------------------------------------------------------
// DateTime
//--------------------------------------------------------------------------------

internal sealed class DateTimeToTimeWriter : IColumnWriter
{
    public static DateTimeToTimeWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateTime)value).TimeOfDay, providerType);
}

internal sealed class DateTimeToTimeTzWriter : IColumnWriter
{
    public static DateTimeToTimeTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(new DateTimeOffset((DateTime)value), providerType);
}

internal sealed class DateTimeToTimestampWriter : IColumnWriter
{
    public static DateTimeToTimestampWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.SpecifyKind((DateTime)value, DateTimeKind.Unspecified), providerType);
}

internal sealed class DateTimeToTimestampTzWriter : IColumnWriter
{
    public static DateTimeToTimestampTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateTime)value).ToUniversalTime(), providerType);
}

//--------------------------------------------------------------------------------
// DateTimeOffset
//--------------------------------------------------------------------------------

internal sealed class DateTimeOffsetToDateWriter : IColumnWriter
{
    public static DateTimeOffsetToDateWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateTimeOffset)value).DateTime, providerType);
}

internal sealed class DateTimeOffsetToTimeWriter : IColumnWriter
{
    public static DateTimeOffsetToTimeWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateTimeOffset)value).TimeOfDay, providerType);
}

internal sealed class DateTimeOffsetToTimestampWriter : IColumnWriter
{
    public static DateTimeOffsetToTimestampWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(new DateTime(((DateTimeOffset)value).Ticks, DateTimeKind.Unspecified), providerType);
}

internal sealed class DateTimeOffsetToTimestampTzWriter : IColumnWriter
{
    public static DateTimeOffsetToTimestampTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(new DateTime(((DateTimeOffset)value).UtcTicks, DateTimeKind.Utc), providerType);
}

//--------------------------------------------------------------------------------
// DateOnly
//--------------------------------------------------------------------------------

internal sealed class DateOnlyToDateWriter : IColumnWriter
{
    public static DateOnlyToDateWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue), providerType);
}

internal sealed class DateOnlyToTimeWriter : IColumnWriter
{
    public static DateOnlyToTimeWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue).TimeOfDay, providerType);
}

internal sealed class DateOnlyToTimeTzWriter : IColumnWriter
{
    public static DateOnlyToTimeTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(new DateTimeOffset(((DateOnly)value).ToDateTime(TimeOnly.MinValue)), providerType);
}

internal sealed class DateOnlyToTimestampWriter : IColumnWriter
{
    public static DateOnlyToTimestampWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.SpecifyKind(((DateOnly)value).ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified), providerType);
}

internal sealed class DateOnlyToTimestampTzWriter : IColumnWriter
{
    public static DateOnlyToTimestampTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((DateOnly)value).ToDateTime(TimeOnly.MinValue).ToUniversalTime(), providerType);
}

//--------------------------------------------------------------------------------
// TimeOnly
//--------------------------------------------------------------------------------

internal sealed class TimeOnlyToTimeWriter : IColumnWriter
{
    public static TimeOnlyToTimeWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((TimeOnly)value).ToTimeSpan(), providerType);
}

internal sealed class TimeOnlyToTimeTzWriter : IColumnWriter
{
    public static TimeOnlyToTimeTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(new DateTimeOffset(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value)), providerType);
}

internal sealed class TimeOnlyToTimestampWriter : IColumnWriter
{
    public static TimeOnlyToTimestampWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.SpecifyKind(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value), DateTimeKind.Unspecified), providerType);
}

internal sealed class TimeOnlyToTimestampTzWriter : IColumnWriter
{
    public static TimeOnlyToTimestampTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateOnly.FromDateTime(DateTime.Now).ToDateTime((TimeOnly)value).ToUniversalTime(), providerType);
}

//--------------------------------------------------------------------------------
// String
//--------------------------------------------------------------------------------

internal sealed class StringToDateWriter : IColumnWriter
{
    public static StringToDateWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture), providerType);
}

internal sealed class StringToTimeWriter : IColumnWriter
{
    public static StringToTimeWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture).TimeOfDay, providerType);
}

internal sealed class StringToTimeTzWriter : IColumnWriter
{
    public static StringToTimeTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTimeOffset.Parse((string)value, CultureInfo.InvariantCulture), providerType);
}

internal sealed class StringToTimestampWriter : IColumnWriter
{
    public static StringToTimestampWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.SpecifyKind(DateTime.Parse((string)value, CultureInfo.InvariantCulture), DateTimeKind.Unspecified), providerType);
}

internal sealed class StringToTimestampTzWriter : IColumnWriter
{
    public static StringToTimestampTzWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(DateTime.Parse((string)value, CultureInfo.InvariantCulture).ToUniversalTime(), providerType);
}

//--------------------------------------------------------------------------------
// JSON
//--------------------------------------------------------------------------------

internal sealed class StringToJsonWriter : IColumnWriter
{
    public static StringToJsonWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((string)value, providerType);
}

internal sealed class JsonDocumentToJsonWriter : IColumnWriter
{
    public static JsonDocumentToJsonWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((JsonDocument)value).RootElement.GetRawText(), providerType);
}

internal sealed class JsonElementToJsonWriter : IColumnWriter
{
    public static JsonElementToJsonWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(((JsonElement)value).GetRawText(), providerType);
}

//--------------------------------------------------------------------------------
// Network
//--------------------------------------------------------------------------------

internal sealed class IpAddressWriter : IColumnWriter
{
    public static IpAddressWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((IPAddress)value, providerType);
}

internal sealed class StringToIpAddressWriter : IColumnWriter
{
    public static StringToIpAddressWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(IPAddress.Parse((string)value), providerType);
}

internal sealed class PhysicalAddressWriter : IColumnWriter
{
    public static PhysicalAddressWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((PhysicalAddress)value, providerType);
}

internal sealed class StringToPhysicalAddressWriter : IColumnWriter
{
    public static StringToPhysicalAddressWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync(PhysicalAddress.Parse((string)value), providerType);
}

//--------------------------------------------------------------------------------
// Array
//--------------------------------------------------------------------------------

internal sealed class ArrayWriter<T> : IColumnWriter
{
    public static ArrayWriter<T> Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((T[])value, providerType);
}

//--------------------------------------------------------------------------------
// Geometry
//--------------------------------------------------------------------------------

internal sealed class PointWriter : IColumnWriter<NpgsqlPoint>
{
    public static PointWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlPoint)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPoint value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class LineWriter : IColumnWriter<NpgsqlLine>
{
    public static LineWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlLine)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlLine value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class LSegWriter : IColumnWriter<NpgsqlLSeg>
{
    public static LSegWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlLSeg)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlLSeg value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class BoxWriter : IColumnWriter<NpgsqlBox>
{
    public static BoxWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlBox)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlBox value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class PathWriter : IColumnWriter<NpgsqlPath>
{
    public static PathWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlPath)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPath value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class PolygonWriter : IColumnWriter<NpgsqlPolygon>
{
    public static PolygonWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlPolygon)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlPolygon value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class CircleWriter : IColumnWriter<NpgsqlCircle>
{
    public static CircleWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlCircle)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlCircle value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

//--------------------------------------------------------------------------------
// Interval
//--------------------------------------------------------------------------------

internal sealed class IntervalWriter : IColumnWriter<NpgsqlInterval>
{
    public static IntervalWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlInterval)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlInterval value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

internal sealed class TimeSpanToIntervalWriter : IColumnWriter<TimeSpan>
{
    public static TimeSpanToIntervalWriter Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((TimeSpan)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, TimeSpan value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
}

//--------------------------------------------------------------------------------
// Range
//--------------------------------------------------------------------------------

internal sealed class RangeWriter<T> : IColumnWriter<NpgsqlRange<T>>
{
    public static RangeWriter<T> Instance { get; } = new();

    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType) =>
        writer.WriteAsync((NpgsqlRange<T>)value, providerType);

    public Task WriteAsync(NpgsqlBinaryImporter writer, NpgsqlRange<T> value, NpgsqlDbType providerType) =>
        writer.WriteAsync(value, providerType);
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

    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026", Justification = "Convert.ChangeType and WriteAsync(object) are only used for types registered via RegisterWriter<T>.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Convert.ChangeType and WriteAsync(object) are only used for types registered via RegisterWriter<T>.")]
    public Task WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
    {
        var converted = value is T t ? converter(t) : converter((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture));
        return writer.WriteAsync(converted, providerType);
    }

    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026", Justification = "WriteAsync(object) is only used for types registered via RegisterWriter<T>.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "WriteAsync(object) is only used for types registered via RegisterWriter<T>.")]
    public Task WriteAsync(NpgsqlBinaryImporter writer, T value, NpgsqlDbType providerType)
    {
        var converted = converter(value);
        return writer.WriteAsync(converted, providerType);
    }
}
