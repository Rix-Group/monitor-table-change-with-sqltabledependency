using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;

namespace TableDependency.SqlClient.Test.Features.Recovery;

// Two listeners on one persistent id (an instance handing ownership to another) must share the queue: each change once, none lost when one stops.
public sealed class TwoListenersSharingAPersistentIdTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(TwoListenersSharingAPersistentIdTest);

    private sealed class Model
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]; " +
            $"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task SharedPersistentId_EachChangeArrivesOnce_AndNoneIsLostWhenOneListenerStops()
    {
        // ARRANGE
        var persistentId = $"shared_{Guid.NewGuid():N}";
        var received = new ConcurrentBag<string>();
        var first = await StartAsync(persistentId, received);
        var second = await StartAsync(persistentId, received);

        try
        {
            // ACT
            await InsertAsync("both", 20);
            await WaitForAsync(() => received.Count >= 20);
            await first.DisposeAsync();
            await InsertAsync("after", 5);
            await WaitForAsync(() => received.Count(n => n.StartsWith("after", StringComparison.Ordinal)) >= 5);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            // ASSERT
            Assert.Equal(25, received.Count);
            Assert.Equal(25, received.Distinct().Count());
        }
        finally
        {
            await first.DisposeAsync();
            await second.DisposeAsync();
            await second.DropDatabaseObjectsAsync();
        }
    }

    private async Task<SqlTableDependency<Model>> StartAsync(string persistentId, ConcurrentBag<string> received)
    {
        var dependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, persistentId: persistentId, ct: TestContext.Current.CancellationToken);
        dependency.OnChanged += e => received.Add(e.Entity.Name);
        await dependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
        return dependency;
    }

    private async Task InsertAsync(string prefix, int count)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        for (var i = 0; i < count; i++)
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES (@name);";
            sqlCommand.Parameters.AddWithValue("@name", $"{prefix}-{i}");
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (!condition() && DateTime.UtcNow < deadline)
            await Task.Delay(50, TestContext.Current.CancellationToken);
    }
}