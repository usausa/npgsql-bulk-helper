// ReSharper disable GrammarMistakeInComment
namespace Example;

using System.Diagnostics;

using Mofucat.DataToolkit;

using Npgsql;

using NpgsqlBulkHelper;

internal static class Program
{
    private const string ConnectionString = "Host=postgres-server;Database=test;Username=test;Password=test";

    public static async Task Main()
    {
        using var source = new ObjectDataReader<Data>(Enumerable.Range(1, 100000).Select(static x => new Data
        {
            Id = x,
            Name = $"Name-{x}",
            Option = x % 3 == 0 ? null : "Options",
            Flag = x % 2 == 0,
            CreatedAt = DateTime.Now
        }));

        await using var con = new NpgsqlConnection(ConnectionString);
        await con.OpenAsync();
        var bulkCopy = new NpgsqlBulkCopy(con)
        {
            DestinationTableName = "data"
        };

        var watch = Stopwatch.StartNew();
        var inserted = await bulkCopy.WriteToServerAsync(source);
        var elapsed = watch.Elapsed;

        Console.WriteLine($"Inserted: rows=[{inserted}], elapsed=[{elapsed.TotalMilliseconds}]");
    }
}

public class Data
{
    public int Id { get; set; }

    public string Name { get; set; } = default!;

    public string? Option { get; set; }

    public bool Flag { get; set; }

    public DateTime CreatedAt { get; set; }
}
