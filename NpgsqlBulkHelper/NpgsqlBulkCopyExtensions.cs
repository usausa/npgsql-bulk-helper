namespace NpgsqlBulkHelper;

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
