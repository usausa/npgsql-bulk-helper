namespace NpgsqlBulkHelper;

internal static class PostgresIdentifier
{
    // "schema.table" -> "schema"."table"
    public static string Quote(string name) =>
        String.Join('.', name.Split('.').Select(QuotePart));

    public static string QuotePart(string part) =>
        part.Length == 0
            ? throw new InvalidOperationException($"Invalid identifier: {part}")
            : $"\"{part.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
}
