namespace NpgsqlBulkHelper;

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
