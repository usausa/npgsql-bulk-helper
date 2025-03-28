namespace NpgsqlBulkHelper;

using System.Collections;
using System.Data.Common;
using System.Data;

using Npgsql;

using NpgsqlTypes;

public sealed class NpgsqlBulkCopy
{
    private static readonly Dictionary<Type, IColumnWriter> ConvertWriters = [];

    private static readonly Dictionary<Type, IColumnWriter> SameTypeWriters = [];

    private readonly NpgsqlConnection con;

    public string? DestinationTableName { get; set; }

    public int BulkCopyTimeout { get; set; }

    public List<NpgsqlBulkCopyColumnMapping> ColumnMappings { get; } = new();

    static NpgsqlBulkCopy()
    {
        SameTypeWriters[typeof(byte[])] = new SameTypeWriter<byte[]>();
        SameTypeWriters[typeof(DateTimeOffset)] = new SameTypeWriter<DateTimeOffset>();
        SameTypeWriters[typeof(TimeSpan)] = new SameTypeWriter<TimeSpan>();
        SameTypeWriters[typeof(BitArray)] = new SameTypeWriter<BitArray>();

        ConvertWriters[typeof(bool)] = new ConvertWriter<bool>();
        ConvertWriters[typeof(byte)] = new ConvertWriter<byte>();
        ConvertWriters[typeof(char)] = new ConvertWriter<char>();
        ConvertWriters[typeof(short)] = new ConvertWriter<short>();
        ConvertWriters[typeof(int)] = new ConvertWriter<int>();
        ConvertWriters[typeof(long)] = new ConvertWriter<long>();
        ConvertWriters[typeof(float)] = new ConvertWriter<float>();
        ConvertWriters[typeof(double)] = new ConvertWriter<double>();
        ConvertWriters[typeof(decimal)] = new ConvertWriter<decimal>();
        ConvertWriters[typeof(DateTime)] = new ConvertWriter<DateTime>();
        ConvertWriters[typeof(Guid)] = new ConvertWriter<Guid>();
        ConvertWriters[typeof(string)] = new ConvertWriter<string>();
    }

    public NpgsqlBulkCopy(NpgsqlConnection con)
    {
        this.con = con;
    }

    public async ValueTask<int> WriteToServerAsync(DataTable table, CancellationToken cancellationToken = default)
    {
        using var valuesEnumerator = new DataTableValuesEnumerator(table);
        return await WriteToServerAsync(valuesEnumerator, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<int> WriteToServerAsync(IDataReader reader, CancellationToken cancellationToken = default)
    {
        using var valuesEnumerator = reader is DbDataReader dbDataReader ? (IValuesEnumerator)new DbDataReaderEnumerator(dbDataReader) : new DataReaderEnumerator(reader);
        return await WriteToServerAsync(valuesEnumerator, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<int> WriteToServerAsync(IValuesEnumerator valuesEnumerator, CancellationToken cancellationToken = default)
    {
        var rowsInserted = 0;
        var closeConnection = false;
        try
        {
            if (con.State != ConnectionState.Open)
            {
                await con.OpenAsync(cancellationToken).ConfigureAwait(false);
                closeConnection = true;
            }

            // Prepare schema
#pragma warning disable CA2007
            await using var cmd = con.CreateCommand();
#pragma warning restore CA2007
            cmd.CommandType = CommandType.Text;
#pragma warning disable CA2100
            cmd.CommandText = $"SELECT * FROM {DestinationTableName} WHERE false";
#pragma warning restore CA2100
#pragma warning disable CA2007
            await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo, cancellationToken).ConfigureAwait(false);
#pragma warning restore CA2007
            using var schema = (await reader.GetSchemaTableAsync(cancellationToken).ConfigureAwait(false))!;

            var columns = new ColumnInfo[schema.Rows.Count];
            for (var i = 0; i < columns.Length; i++)
            {
                var row = schema.Rows[i];
                ref var column = ref columns[i];
                column.ColumnName = row["ColumnName"].ToString()!;
                column.ProviderType = (NpgsqlDbType)(int)row["ProviderType"];
                column.DataType = (Type)row["DataType"];
                column.SourceOrdinal = i;
            }

            await reader.CloseAsync().ConfigureAwait(false);

            // Update mapping
            foreach (var mapping in ColumnMappings)
            {
                int columnIndex;
                if (String.IsNullOrEmpty(mapping.DestinationColumn))
                {
                    columnIndex = mapping.DestinationOrdinal;
                }
                else
                {
                    columnIndex = -1;
                    for (var i = 0; i < columns.Length; i++)
                    {
                        ref var c = ref columns[i];
                        if (c.ColumnName == mapping.DestinationColumn)
                        {
                            columnIndex = i;
                            break;
                        }
                    }

                    if (columnIndex < 0)
                    {
                        throw new InvalidOperationException($"Destination column {mapping.DestinationColumn} is not found.");
                    }
                }

                ref var column = ref columns[columnIndex];
                column.SourceOrdinal = String.IsNullOrEmpty(mapping.SourceColumn) ? mapping.SourceOrdinal : valuesEnumerator.GetOrdinal(mapping.SourceColumn);
            }

            // Resolve writer
            for (var i = 0; i < columns.Length; i++)
            {
                ref var column = ref columns[i];
                var fieldType = valuesEnumerator.GetFieldType(column.SourceOrdinal);
                var w = FindWriter(column.ProviderType, column.DataType, fieldType);
                if (w is null)
                {
                    throw new InvalidOperationException($"Writer is not supported. fieldType=[{fieldType}], providerType=[{column.ProviderType}]");
                }

                column.Writer = w;
            }

            // Write data
            var values = new object[valuesEnumerator.FieldCount];
#pragma warning disable CA2007
            await using var writer = await con.BeginBinaryImportAsync($"COPY {DestinationTableName} FROM STDIN (FORMAT BINARY)", cancellationToken).ConfigureAwait(false);
#pragma warning restore CA2007
            writer.Timeout = TimeSpan.FromSeconds(BulkCopyTimeout);

            while (await valuesEnumerator.MoveNextAsync().ConfigureAwait(false))
            {
                valuesEnumerator.GetValues(values);

                await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
                for (var i = 0; i < columns.Length; i++)
                {
                    ref var column = ref columns[i];
                    var value = values[column.SourceOrdinal];
                    if (value is DBNull or null)
                    {
#pragma warning disable CA2016
                        // ReSharper disable once MethodSupportsCancellation
                        await writer.WriteNullAsync().ConfigureAwait(false);
#pragma warning restore CA2016
                    }
                    else
                    {
                        await column.Writer.WriteAsync(writer, value, column.ProviderType).ConfigureAwait(false);
                    }
                }

                rowsInserted++;
            }

            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);

            return rowsInserted;
        }
        finally
        {
            if (closeConnection)
            {
                await con.CloseAsync().ConfigureAwait(false);
            }
        }
    }

    private static IColumnWriter? FindWriter(NpgsqlDbType providerType, Type dbType, Type fieldType)
    {
        switch (providerType)
        {
            // date
            case NpgsqlDbType.Date when fieldType == typeof(DateTimeOffset):
                return DateTimeOffsetToDateWriter.Instance;
            case NpgsqlDbType.Date when fieldType == typeof(DateOnly):
                return DateOnlyToDateWriter.Instance;
            case NpgsqlDbType.Date when fieldType == typeof(string):
                return StringToDateWriter.Instance;
            // time
            case NpgsqlDbType.Time when fieldType == typeof(DateTime):
                return DateTimeToTimeWriter.Instance;
            case NpgsqlDbType.Time when fieldType == typeof(DateTimeOffset):
                return DateTimeOffsetToTimeWriter.Instance;
            case NpgsqlDbType.Time when fieldType == typeof(DateOnly):
                return DateOnlyToTimeWriter.Instance;
            case NpgsqlDbType.Time when fieldType == typeof(string):
                return StringToTimeWriter.Instance;
            // time with time zone
            case NpgsqlDbType.TimeTz when fieldType == typeof(DateTime):
                return DateTimeToTimeTzWriter.Instance;
            case NpgsqlDbType.TimeTz when fieldType == typeof(DateOnly):
                return DateOnlyToTimeTzWriter.Instance;
            case NpgsqlDbType.TimeTz when fieldType == typeof(string):
                return StringToTimeTzWriter.Instance;
            // timestamp
            case NpgsqlDbType.Timestamp when fieldType == typeof(DateTime):
                return DateTimeToTimestampWriter.Instance;
            case NpgsqlDbType.Timestamp when fieldType == typeof(DateTimeOffset):
                return DateTimeOffsetToTimeWriter.Instance;
            case NpgsqlDbType.Timestamp when fieldType == typeof(DateOnly):
                return DateOnlyToTimestampWriter.Instance;
            case NpgsqlDbType.Timestamp when fieldType == typeof(string):
                return StringToTimestampWriter.Instance;
            // timestamp with time zone
            case NpgsqlDbType.TimestampTz when fieldType == typeof(DateTime):
                return DateTimeToTimestampTzWriter.Instance;
            case NpgsqlDbType.TimestampTz when fieldType == typeof(DateTimeOffset):
                return DateTimeOffsetToTimestampTzWriter.Instance;
            case NpgsqlDbType.TimestampTz when fieldType == typeof(DateOnly):
                return DateOnlyToTimestampTzWriter.Instance;
            case NpgsqlDbType.TimestampTz when fieldType == typeof(string):
                return StringToTimestampTzWriter.Instance;
        }

        // Same type only
        if ((dbType == fieldType) && SameTypeWriters.TryGetValue(dbType, out var writer))
        {
            return writer;
        }

        // Can convert
        return ConvertWriters.GetValueOrDefault(dbType);
    }

    private struct ColumnInfo
    {
        public string ColumnName;

        public NpgsqlDbType ProviderType;

        public Type DataType;

        public int SourceOrdinal;

        public IColumnWriter Writer;
    }
}
