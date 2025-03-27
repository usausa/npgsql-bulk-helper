# NpgsqlBulkHelper

[![NuGet](https://img.shields.io/nuget/v/NpgsqlBulkHelper.svg)](https://www.nuget.org/packages/NpgsqlBulkHelper)

```csharp
await using var con = new NpgsqlConnection(ConnectionString);
await con.OpenAsync();
var bulkCopy = new NpgsqlBulkCopy(con)
{
    DestinationTableName = "data"
};

var inserted = await bulkCopy.WriteToServerAsync(source);
```
