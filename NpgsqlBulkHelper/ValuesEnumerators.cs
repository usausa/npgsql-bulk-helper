namespace NpgsqlBulkHelper;

using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

internal interface IValuesEnumerator : IDisposable
{
    int FieldCount { get; }

    int GetOrdinal(string name);

    Type GetFieldType(int index);

    ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken);

    object GetValue(int index);
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
    }

    public int GetOrdinal(string name) => dataReader.GetOrdinal(name);

    public Type GetFieldType(int index) => dataReader.GetFieldType(index);

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => new(dataReader.ReadAsync(cancellationToken));

    public object GetValue(int index) => dataReader.GetValue(index);
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
        // Ownership of the reader remains with the caller
    }

    public int GetOrdinal(string name) => dataReader.GetOrdinal(name);

    public Type GetFieldType(int index) => dataReader.GetFieldType(index);

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(dataReader.Read());
    }

    public object GetValue(int index) => dataReader.GetValue(index);
}

[RequiresUnreferencedCode("DataTable requires unreferenced code for AOT compatibility.")]
[RequiresDynamicCode("DataTable requires dynamic code for AOT compatibility.")]
internal sealed class DataTableValuesEnumerator : IValuesEnumerator
{
    private readonly DataTable table;

    private readonly IEnumerator<DataRow> dataRows;

    public int FieldCount => table.Columns.Count;

    public DataTableValuesEnumerator(DataTable table)
    {
        this.table = table;
        dataRows = table.Rows.Cast<DataRow>().GetEnumerator();
    }

    public void Dispose()
    {
        dataRows.Dispose();
    }

    public int GetOrdinal(string name) => table.Columns.IndexOf(name);

    public Type GetFieldType(int index) => table.Columns[index].DataType;

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(dataRows.MoveNext());
    }

    public object GetValue(int index) => dataRows.Current[index];
}
