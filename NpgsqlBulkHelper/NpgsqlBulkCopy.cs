namespace NpgsqlBulkHelper;

using System.Data.Common;
using System.Data;
using System.Globalization;

using Npgsql;

using NpgsqlTypes;

public sealed class NpgsqlBulkCopy
{
    private static readonly Dictionary<Type, IColumnWriter> Writers = [];

    private readonly NpgsqlConnection con;

    public string? DestinationTableName { get; set; }

    public int BulkCopyTimeout { get; set; }

    public List<NpgsqlBulkCopyColumnMapping> ColumnMappings { get; } = new();

    static NpgsqlBulkCopy()
    {
        // TODO additional types support ?
        Writers[typeof(bool)] = new ColumnWriter<bool>();
        Writers[typeof(byte)] = new ColumnWriter<byte>();
        Writers[typeof(char)] = new ColumnWriter<char>();
        Writers[typeof(short)] = new ColumnWriter<short>();
        Writers[typeof(int)] = new ColumnWriter<int>();
        Writers[typeof(long)] = new ColumnWriter<long>();
        Writers[typeof(float)] = new ColumnWriter<float>();
        Writers[typeof(double)] = new ColumnWriter<double>();
        Writers[typeof(decimal)] = new ColumnWriter<decimal>();
        Writers[typeof(DateTime)] = new ColumnWriter<DateTime>();
        Writers[typeof(Guid)] = new ColumnWriter<Guid>();
        Writers[typeof(string)] = new ColumnWriter<string>();
        Writers[typeof(byte[])] = new ColumnWriter<byte[]>();
        Writers[typeof(char[])] = new ColumnWriter<char[]>();
    }

    public NpgsqlBulkCopy(NpgsqlConnection con)
    {
        this.con = con;
    }

    public async ValueTask<int> WriteToServerAsync(DataTable table, CancellationToken cancellationToken = default)
    {
        using var valuesEnumerator = new DataRowsValuesEnumerator(
            table.Rows.Cast<DataRow>().Where(static x => x is not null),
            table.Columns.Cast<DataColumn>().Select(static x => x.ColumnName).ToArray(),
            table.Columns.Count);
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
                var dataType = (Type)row["DataType"];
                if (!Writers.TryGetValue(dataType, out var w))
                {
                    throw new InvalidOperationException($"DataType {dataType} is not supported.");
                }

                ref var column = ref columns[i];
                column.ColumnName = row["ColumnName"].ToString()!;
                column.ProviderType = (NpgsqlDbType)(int)row["ProviderType"];
                column.SourceOrdinal = i;
                column.Writer = w;
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
                    await column.Writer.WriteAsync(writer, values[column.SourceOrdinal], column.ProviderType).ConfigureAwait(false);
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

    private struct ColumnInfo
    {
        public string ColumnName;

        public NpgsqlDbType ProviderType;

        public int SourceOrdinal;

        public IColumnWriter Writer;
    }

    private interface IColumnWriter
    {
        ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType);
    }

    private sealed class ColumnWriter<T> : IColumnWriter
    {
        public async ValueTask WriteAsync(NpgsqlBinaryImporter writer, object value, NpgsqlDbType providerType)
        {
            // TODO additional types support ?
            if (value is DBNull or null)
            {
                await writer.WriteNullAsync().ConfigureAwait(false);
            }
            else if (value is T t)
            {
                await writer.WriteAsync(t, providerType).ConfigureAwait(false);
            }
            else
            {
                await writer.WriteAsync((T)Convert.ChangeType(value, typeof(T), CultureInfo.InvariantCulture), providerType).ConfigureAwait(false);
            }
        }
    }
}

internal interface IValuesEnumerator : IDisposable
{
    int FieldCount { get; }

    int GetOrdinal(string name);

    ValueTask<bool> MoveNextAsync();

    void GetValues(object[] values);
}

internal sealed class DbDataReaderEnumerator : IValuesEnumerator
{
    private readonly DbDataReader dataReader;

    public int FieldCount { get; }

    public DbDataReaderEnumerator(DbDataReader dataReader)
    {
        this.dataReader = dataReader;
        FieldCount = dataReader.FieldCount;
    }

    public void Dispose()
    {
        dataReader.Dispose();
    }

    public int GetOrdinal(string name) => dataReader.GetOrdinal(name);

    public ValueTask<bool> MoveNextAsync() => new(dataReader.ReadAsync());

    public void GetValues(object[] values) => dataReader.GetValues(values);
}

internal sealed class DataReaderEnumerator : IValuesEnumerator
{
    private readonly IDataReader dataReader;

    public int FieldCount { get; }

    public DataReaderEnumerator(IDataReader dataReader)
    {
        this.dataReader = dataReader;
        FieldCount = dataReader.FieldCount;
    }

    public void Dispose()
    {
        dataReader.Dispose();
    }

    public int GetOrdinal(string name) => dataReader.GetOrdinal(name);

    public ValueTask<bool> MoveNextAsync() => new(dataReader.Read());

    public void GetValues(object[] values) => dataReader.GetValues(values);
}

internal sealed class DataRowsValuesEnumerator : IValuesEnumerator
{
    private readonly IEnumerator<DataRow> dataRows;

    private readonly string[] columnNames;

    public int FieldCount { get; }

    public DataRowsValuesEnumerator(IEnumerable<DataRow> dataRows, string[] columnNames, int fieldCount)
    {
        this.dataRows = dataRows.GetEnumerator();
        this.columnNames = columnNames;
        FieldCount = fieldCount;
    }

    public void Dispose()
    {
        dataRows.Dispose();
    }

    public int GetOrdinal(string name) => Array.IndexOf(columnNames, name);

    public ValueTask<bool> MoveNextAsync() => new(dataRows.MoveNext());

    public void GetValues(object[] values)
    {
        var row = dataRows.Current;
        for (var i = 0; i < FieldCount; i++)
        {
            values[i] = row[i];
        }
    }
}

public sealed class NpgsqlBulkCopyColumnMapping
{
    public int SourceOrdinal { get; }

    public int DestinationOrdinal { get; }

    public string? SourceColumn { get; }

    public string? DestinationColumn { get; }

    public NpgsqlBulkCopyColumnMapping(int sourceOrdinal, int destinationOrdinal)
    {
        SourceOrdinal = sourceOrdinal;
        DestinationOrdinal = destinationOrdinal;
    }

    public NpgsqlBulkCopyColumnMapping(int sourceOrdinal, string destinationColumn)
    {
        SourceOrdinal = sourceOrdinal;
        DestinationColumn = destinationColumn;
    }

    public NpgsqlBulkCopyColumnMapping(string sourceColumn, int destinationOrdinal)
    {
        SourceColumn = sourceColumn;
        DestinationOrdinal = destinationOrdinal;
    }

    public NpgsqlBulkCopyColumnMapping(string sourceColumn, string destinationColumn)
    {
        SourceColumn = sourceColumn;
        DestinationColumn = destinationColumn;
    }
}

public static class NpgsqlBulkCopyExtensions
{
    public static void AddMapping(this NpgsqlBulkCopy bulkCopy, int sourceOrdinal, int destinationOrdinal) =>
        bulkCopy.ColumnMappings.Add(new NpgsqlBulkCopyColumnMapping(sourceOrdinal, destinationOrdinal));

    public static void AddMapping(this NpgsqlBulkCopy bulkCopy, int sourceOrdinal, string destinationColumn) =>
        bulkCopy.ColumnMappings.Add(new NpgsqlBulkCopyColumnMapping(sourceOrdinal, destinationColumn));

    public static void AddMapping(this NpgsqlBulkCopy bulkCopy, string sourceColumn, int destinationOrdinal) =>
        bulkCopy.ColumnMappings.Add(new NpgsqlBulkCopyColumnMapping(sourceColumn, destinationOrdinal));

    public static void AddMapping(this NpgsqlBulkCopy bulkCopy, string sourceColumn, string destinationColumn) =>
        bulkCopy.ColumnMappings.Add(new NpgsqlBulkCopyColumnMapping(sourceColumn, destinationColumn));
}
