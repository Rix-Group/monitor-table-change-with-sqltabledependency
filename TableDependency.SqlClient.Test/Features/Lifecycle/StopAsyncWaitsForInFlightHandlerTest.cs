#region License

// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

#endregion

using Microsoft.Data.SqlClient;
using TableDependency.SqlClient.Base.Enums;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

// StopAsync must wait for any in-flight OnChangedAsync handler to finish before returning, so a graceful cancellation can never drop a message that the library has already pulled off the queue and started delivering.
public sealed class StopAsyncWaitsForInFlightHandlerTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(StopAsyncWaitsForInFlightHandlerTest);

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
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Name] NVARCHAR(100) NOT NULL);";
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
    public async Task StopAsync_BlocksUntilInFlightOnChangedAsyncCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        SqlTableDependency<Model>? tableDependency = null;

        // The handler signals when it starts, then blocks on releaseHandler so the test can drive cancellation precisely while a delivery is in flight.
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCompletedAtTicks = 0L;
        Model? deliveredEntity = null;

        try
        {
            // ARRANGE
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                ct: ct);

            tableDependency.OnChangedAsync = async e =>
            {
                deliveredEntity = e.Entity;
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                Volatile.Write(ref handlerCompletedAtTicks, DateTime.UtcNow.Ticks);
            };

            await tableDependency.StartAsync(ct: ct);
            await WaitUntilAsync(() => tableDependency.Status is TableDependencyStatus.WaitingForNotification, TimeSpan.FromSeconds(15), ct);

            // ACT - insert a row, wait until the handler has actually started, then call StopAsync while the handler is parked.
            await InsertAsync("in-flight", ct);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

            var stopTask = tableDependency.StopAsync();
            await Task.Delay(TimeSpan.FromMilliseconds(500), ct); // Give StopAsync a moment to observe cancellation and try to wind down. It must not finish while the handler is still parked.

            // ASSERT
            // StopAsync is still pending, handler has not completed, status has not moved to a terminal "cancelled" state yet
            Assert.False(stopTask.IsCompleted, "StopAsync returned while an OnChangedAsync handler was still running — messages could be dropped on cancellation.");
            Assert.Equal(0L, Volatile.Read(ref handlerCompletedAtTicks));

            // Release the handler
            var releasedAtTicks = DateTime.UtcNow.Ticks;
            releaseHandler.SetResult();

            // StopAsync now drains and only finishes after the handler did.
            await stopTask.WaitAsync(TimeSpan.FromSeconds(30), ct);

            var completedTicks = Volatile.Read(ref handlerCompletedAtTicks);
            Assert.NotEqual(0L, completedTicks);
            Assert.True(completedTicks >= releasedAtTicks, $"Handler completion ({completedTicks}) must occur after release ({releasedAtTicks}).");
            Assert.NotNull(deliveredEntity);
            Assert.Equal("in-flight", deliveredEntity!.Name);
            Assert.Equal(TableDependencyStatus.StopDueToCancellation, tableDependency.Status);
        }
        finally
        {
            // Unblock the handler unconditionally so a failed assertion above doesn't leave DisposeAsync hanging on StopAsync.
            releaseHandler.TrySetResult();
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }
    }

    private async Task InsertAsync(string name, CancellationToken ct)
    {
        const int maxAttempts = 5;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var sqlConnection = new SqlConnection(ConnectionString);
                await sqlConnection.OpenAsync(ct);

                await using var sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES (@name);";
                sqlCommand.Parameters.AddWithValue("@name", name);
                await sqlCommand.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (SqlException ex) when (ex.Number is 1205 && attempt < maxAttempts)
            {
                // The trigger's SEND ON CONVERSATION can race against the listener's WAITFOR RECEIVE on Service Broker internals and be picked as a deadlock victim - that's a normal SQL Server outcome to retry, not a real test failure.
                await Task.Delay(TimeSpan.FromMilliseconds(100 * attempt), ct);
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate())
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct);
        }

        throw new TimeoutException($"Predicate did not become true within {timeout}.");
    }
}