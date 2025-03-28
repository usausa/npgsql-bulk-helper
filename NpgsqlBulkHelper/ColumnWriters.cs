namespace NpgsqlBulkHelper;

using System.Globalization;

using Npgsql;

using NpgsqlTypes;

internal interface IColumnWriter
{
    ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType);
}

//--------------------------------------------------------------------------------
// Generic
//--------------------------------------------------------------------------------

internal sealed class ConvertWriter<T> : IColumnWriter
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
}

internal sealed class SameTypeWriter<T> : IColumnWriter
{
    public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
    {
        await writer.WriteAsync((T)value, providerType).ConfigureAwait(false);
    }
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
