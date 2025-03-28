namespace NpgsqlBulkHelper;

using System.Data.Common;
using System.Data;

internal interface IValuesEnumerator : IDisposable
{
    int FieldCount { get; }

    int GetOrdinal(string name);

    Type GetFieldType(int index);

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

    public Type GetFieldType(int index) => dataReader.GetFieldType(index);

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

    public Type GetFieldType(int index) => dataReader.GetFieldType(index);

    public ValueTask<bool> MoveNextAsync() => new(dataReader.Read());

    public void GetValues(object[] values) => dataReader.GetValues(values);
}

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
