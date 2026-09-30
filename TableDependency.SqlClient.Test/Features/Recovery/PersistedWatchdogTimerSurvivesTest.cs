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

namespace TableDependency.SqlClient.Test.Features.Recovery;

// A persisted listener keeps working after its initiator dialog is closed externally (e.g. by the _Sender activation procedure on a watchdog DialogTimer fire).
public class PersistedWatchdogTimerSurvivesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(PersistedWatchdogTimerSurvivesTest);

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
    public async Task PersistedListener_SurvivesWatchdogConversationKill()
    {
        SqlTableDependency<Model>? tableDependency = null;
        var inserted = new TaskCompletionSource<Model>(TaskCreationOptions.RunContinuationsAsynchronously);
        var statuses = new List<TableDependencyStatus>();
        Exception? listenerException = null;

        try
        {
            // ARRANGE
            var persistentId = $"watchdog_{Guid.NewGuid():N}";
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                DependencyConnectionString,
                tableName: TableName,
                persistentId: persistentId,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += e => inserted.TrySetResult(e.Entity);
            tableDependency.OnStatusChanged += e => statuses.Add(e.Status);
            tableDependency.OnExceptionAsync = e => { listenerException = e.Exception; return Task.CompletedTask; };

            var naming = tableDependency.NamingPrefix;

            // Minimum-allowed timeouts (watchdogTimeout >= timeout + 60).
            await tableDependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(TableDependencyStatus.WaitingForNotification, tableDependency.Status);

            // ACT
            // Mimics the DialogTimer branch of the _Sender activation procedure.
            await EndPersistedInitiatorConversationAsync(naming, TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            // ASSERT
            // Listener should remain alive even though its initiator handle was just closed.
            Assert.Equal(TableDependencyStatus.WaitingForNotification, tableDependency.Status);
            Assert.DoesNotContain(TableDependencyStatus.StopDueToError, statuses);
            Assert.Null(listenerException);

            // INSERT a row; the trigger creates a fresh initiator dialog and the listener reads from the new one.
            await using var sqlConnection = new SqlConnection(ConnectionString);
            await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('after-watchdog');";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            var delivered = await Task.WhenAny(
                inserted.Task,
                Task.Delay(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken)) == inserted.Task;

            Assert.True(
                delivered,
                $"Persisted listener should deliver INSERT after its initiator dialog is terminated. " +
                $"Status sequence: [{string.Join(", ", statuses)}]; final={tableDependency.Status}; " +
                $"ex={listenerException?.GetType().Name}: {listenerException?.Message}");

            var deliveredEntity = await inserted.Task;
            Assert.Equal("after-watchdog", deliveredEntity.Name);

            // The next receive iteration must arm the replacement dialog, not the retired handle.
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(TableDependencyStatus.WaitingForNotification, tableDependency.Status);
            Assert.DoesNotContain(TableDependencyStatus.StopDueToError, statuses);
            Assert.Null(listenerException);
        }
        finally
        {
            if (tableDependency is not null)
            {
                await tableDependency.DisposeAsync();
                await tableDependency.DropDatabaseObjectsAsync();
            }
        }
    }

    [Fact]
    public async Task PersistedListener_SurvivesConversationClosedBetweenLookupAndTimerArm()
    {
        SqlTableDependency<Model>? tableDependency = null;
        var inserted = new TaskCompletionSource<Model>(TaskCreationOptions.RunContinuationsAsynchronously);
        var statuses = new List<TableDependencyStatus>();
        Exception? listenerException = null;

        try
        {
            // ARRANGE
            var persistentId = $"timer_race_{Guid.NewGuid():N}";
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                DependencyConnectionString,
                tableName: TableName,
                persistentId: persistentId,
                ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += e => inserted.TrySetResult(e.Entity);
            tableDependency.OnStatusChanged += e => statuses.Add(e.Status);
            tableDependency.OnExceptionAsync = e => { listenerException = e.Exception; return Task.CompletedTask; };

            await tableDependency.StartAsync(timeout: 60, watchdogTimeout: 120, ct: TestContext.Current.CancellationToken);
            var naming = tableDependency.NamingPrefix;

            // Hold the persisted dialog's lock so the listener's next timer arm blocks after its handle lookup.
            await using var blocker = new SqlConnection(ConnectionString);
            await blocker.OpenAsync(TestContext.Current.CancellationToken);
            await using var transaction = (SqlTransaction)await blocker.BeginTransactionAsync(TestContext.Current.CancellationToken);
            var conversationHandle = await LockPersistedConversationAsync(blocker, transaction, naming, TestContext.Current.CancellationToken);

            // ACT
            await WaitForListenerTimerArmBlockedAsync(blocker, transaction, TestContext.Current.CancellationToken);
            await EndConversationAsync(blocker, transaction, conversationHandle, TestContext.Current.CancellationToken);
            await transaction.CommitAsync(TestContext.Current.CancellationToken);

            // The failed arm is statement-level, so the batch still enters WAITFOR; an INSERT ends it and surfaces any error.
            await using var sqlConnection = new SqlConnection(ConnectionString);
            await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('after-timer-race');";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            var delivered = await Task.WhenAny(inserted.Task, Task.Delay(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken)) == inserted.Task;
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            // ASSERT
            Assert.True(delivered, $"Listener should keep delivering after the timer-arm race. Statuses: [{string.Join(", ", statuses)}]; ex={listenerException?.Message}");
            Assert.Equal("after-timer-race", (await inserted.Task).Name);
            Assert.Null(listenerException);
            Assert.DoesNotContain(TableDependencyStatus.StopDueToError, statuses);
            Assert.Equal(TableDependencyStatus.WaitingForNotification, tableDependency.Status);
        }
        finally
        {
            if (tableDependency is not null)
            {
                await tableDependency.DisposeAsync();
                await tableDependency.DropDatabaseObjectsAsync();
            }
        }
    }

    private static async Task<Guid> LockPersistedConversationAsync(SqlConnection sqlConnection, SqlTransaction transaction, string naming, CancellationToken ct)
    {
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText =
            "DECLARE @conversationHandle UNIQUEIDENTIFIER;" +
            " SELECT TOP(1) @conversationHandle = conversation_handle FROM sys.conversation_endpoints WITH (NOLOCK)" +
            " WHERE far_service = @farService AND is_initiator = 1 AND state_desc NOT IN ('CLOSED', 'ERROR');" +
            " BEGIN CONVERSATION TIMER (@conversationHandle) TIMEOUT = 120;" +
            " SELECT @conversationHandle;";
        sqlCommand.Parameters.AddWithValue("@farService", $"{naming}_Receiver");

        return (Guid)(await sqlCommand.ExecuteScalarAsync(ct))!;
    }

    private static async Task WaitForListenerTimerArmBlockedAsync(SqlConnection sqlConnection, SqlTransaction transaction, CancellationToken ct)
    {
        // The listener re-arms at the start of each iteration, so the first blocked arm can take up to one WAITFOR timeout.
        var deadline = DateTime.UtcNow.AddSeconds(90);
        while (DateTime.UtcNow < deadline)
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.Transaction = transaction;
            sqlCommand.CommandText =
                "SELECT COUNT(*) FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t" +
                " WHERE r.blocking_session_id = @@SPID AND t.text LIKE N'%BEGIN CONVERSATION TIMER%WAITFOR (RECEIVE%';";

            if ((int)(await sqlCommand.ExecuteScalarAsync(ct))! > 0)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(250), ct);
        }

        throw new TimeoutException("The listener's timer arm never blocked on the locked persisted conversation.");
    }

    private static async Task EndConversationAsync(SqlConnection sqlConnection, SqlTransaction transaction, Guid conversationHandle, CancellationToken ct)
    {
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.Transaction = transaction;
        sqlCommand.CommandText = "END CONVERSATION @conversationHandle;";
        sqlCommand.Parameters.AddWithValue("@conversationHandle", conversationHandle);
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }

    private async Task EndPersistedInitiatorConversationAsync(string naming, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText =
            "DECLARE @h UNIQUEIDENTIFIER;" +
            " SELECT TOP(1) @h = conversation_handle FROM sys.conversation_endpoints WITH (NOLOCK)" +
            "  WHERE far_service = @farService AND is_initiator = 1 AND state_desc NOT IN ('CLOSED', 'ERROR');" +
            " IF @h IS NOT NULL END CONVERSATION @h;";
        sqlCommand.Parameters.AddWithValue("@farService", $"{naming}_Receiver");
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }
}