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
                ConnectionString,
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