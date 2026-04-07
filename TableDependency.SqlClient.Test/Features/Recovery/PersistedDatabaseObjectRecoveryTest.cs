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

namespace TableDependency.SqlClient.Test.Features.Recovery;

public class PersistedDatabaseObjectRecoveryTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(PersistedDatabaseObjectRecoveryTest);

    private sealed class Model
    {
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

    [Theory]
    [InlineData(true)] // Trigger
    [InlineData(false, true)] // Procedure
    [InlineData(false, false, true)] // SenderService
    [InlineData(false, false, false, true)] // ReceiverService
    [InlineData(false, false, true, false, true)] // SenderService and SenderQueue
    [InlineData(false, false, false, true, false, true)] // ReceiverService and ReceiverQueue
    [InlineData(false, false, false, true, false, false, true)] // ReceiverService and Contract
    [InlineData(false, false, false, true, false, false, true, true)] // ReceiverService and Contract and MessageTypes
    [InlineData(true, true, true, true, true, true, true, true)] // All
    public async Task Test(
        bool dropTrigger = false,
        bool dropProcedure = false,
        bool dropSenderService = false,
        bool dropReceiverService = false,
        bool dropSenderQueue = false,
        bool dropReceiverQueue = false,
        bool dropContract = false,
        bool dropMessageTypes = false)
    {
        SqlTableDependency<Model>? tableDependency = null;
        int changeCounter = 0;

        try
        {
            var persistentId = $"recovery_{Guid.NewGuid():N}";
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                persistentId: persistentId,
                ct: TestContext.Current.CancellationToken);

            var naming = tableDependency.NamingPrefix;
            var schemaName = tableDependency.SchemaName;

            tableDependency.OnChanged += _ => changeCounter++;

            // Create Persistent Objects
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await using var sqlConnection = new SqlConnection(ConnectionString);
            await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
            await using var sqlCommand = sqlConnection.CreateCommand();

            // Test insert
            await ExecuteNonQueryAsync(sqlCommand, $"INSERT INTO {TableName} (Name) VALUES ('test')");
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(1, changeCounter);

            await tableDependency.StopAsync();

            // Ensure all objects have been created
            var baselineCounts = await GetDatabaseObjectCountsAsync(sqlCommand, naming, schemaName);
            Assert.Equal(1, baselineCounts.Trigger);
            Assert.Equal(1, baselineCounts.Procedure);
            Assert.Equal(1, baselineCounts.SenderService);
            Assert.Equal(1, baselineCounts.ReceiverService);
            Assert.Equal(1, baselineCounts.SenderQueue);
            Assert.Equal(1, baselineCounts.ReceiverQueue);
            Assert.Equal(1, baselineCounts.Contract);
            Assert.True(baselineCounts.MessageTypes > 0);

            // Drop Resources
            if (dropTrigger)
                await DropTriggerAsync(sqlCommand, naming, schemaName);
            if (dropProcedure)
                await DropProcedureAsync(sqlCommand, naming, schemaName);
            if (dropSenderService)
                await DropSenderServiceAsync(sqlCommand, naming);
            if (dropReceiverService)
                await DropReceiverServiceAsync(sqlCommand, naming);

            if (dropSenderQueue || dropReceiverQueue || dropContract || dropMessageTypes)
                await EndConversationsAsync(sqlCommand, naming);

            if (dropSenderQueue)
            {
                await DeactivateSenderQueueAsync(sqlCommand, naming, schemaName);
                await DropSenderQueueAsync(sqlCommand, naming, schemaName);
            }
            if (dropReceiverQueue)
                await DropReceiverQueueAsync(sqlCommand, naming, schemaName);
            if (dropContract)
                await DropContractAsync(sqlCommand, naming);
            if (dropMessageTypes)
                await DropMessageTypesAsync(sqlCommand, naming);

            // Check that only what we dropped is dropped
            var missingCounts = await GetDatabaseObjectCountsAsync(sqlCommand, naming, schemaName);
            Assert.Equal(dropTrigger ? 0 : baselineCounts.Trigger, missingCounts.Trigger);
            Assert.Equal(dropProcedure ? 0 : baselineCounts.Procedure, missingCounts.Procedure);
            Assert.Equal(dropSenderService ? 0 : baselineCounts.SenderService, missingCounts.SenderService);
            Assert.Equal(dropReceiverService ? 0 : baselineCounts.ReceiverService, missingCounts.ReceiverService);
            Assert.Equal(dropSenderQueue ? 0 : baselineCounts.SenderQueue, missingCounts.SenderQueue);
            Assert.Equal(dropReceiverQueue ? 0 : baselineCounts.ReceiverQueue, missingCounts.ReceiverQueue);
            Assert.Equal(dropContract ? 0 : baselineCounts.Contract, missingCounts.Contract);
            Assert.Equal(dropMessageTypes ? 0 : baselineCounts.MessageTypes, missingCounts.MessageTypes);

            // Restart should recreate missing objects
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            var restoredCounts = await GetDatabaseObjectCountsAsync(sqlCommand, naming, schemaName);
            Assert.Equal(baselineCounts, restoredCounts);

            await ExecuteNonQueryAsync(sqlCommand, $"INSERT INTO {TableName} (Name) VALUES ('test')");
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(2, changeCounter);
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

    private sealed record DatabaseObjectCounts(int Trigger, int Procedure, int SenderService, int ReceiverService, int SenderQueue, int ReceiverQueue, int Contract, int MessageTypes);

    private static async Task<DatabaseObjectCounts> GetDatabaseObjectCountsAsync(SqlCommand sqlCommand, string naming, string schemaName)
    {
        static async Task<int> ExecuteCountAsync(SqlCommand sqlCommand, string commandText)
        {
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = commandText;
            return Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }

        var trigger = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.triggers WITH (NOLOCK) WHERE object_id = OBJECT_ID(N'[{schemaName}].[tr_{naming}_Sender]');");
        var procedure = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.objects WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_QueueActivationSender';");
        var senderService = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.services WITH (NOLOCK) WHERE name = N'{naming}_Sender';");
        var receiverService = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.services WITH (NOLOCK) WHERE name = N'{naming}_Receiver';");
        var senderQueue = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_Sender';");
        var receiverQueue = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_Receiver';");
        var contract = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.service_contracts WITH (NOLOCK) WHERE name = N'{naming}';");
        var messageTypes = await ExecuteCountAsync(sqlCommand, $"SELECT COUNT(*) FROM sys.service_message_types WITH (NOLOCK) WHERE name LIKE N'{naming}/%';");

        return new DatabaseObjectCounts(trigger, procedure, senderService, receiverService, senderQueue, receiverQueue, contract, messageTypes);
    }

    private static Task DropTriggerAsync(SqlCommand sqlCommand, string naming, string schemaName)
        => ExecuteNonQueryAsync(sqlCommand, $"IF OBJECT_ID(N'[{schemaName}].[tr_{naming}_Sender]', 'TR') IS NOT NULL DROP TRIGGER [{schemaName}].[tr_{naming}_Sender];");

    private static Task DropProcedureAsync(SqlCommand sqlCommand, string naming, string schemaName)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.objects WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_QueueActivationSender') DROP PROCEDURE [{schemaName}].[{naming}_QueueActivationSender];");

    private static Task DropSenderServiceAsync(SqlCommand sqlCommand, string naming)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.services WITH (NOLOCK) WHERE name = N'{naming}_Sender') DROP SERVICE [{naming}_Sender];");

    private static Task DropReceiverServiceAsync(SqlCommand sqlCommand, string naming)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.services WITH (NOLOCK) WHERE name = N'{naming}_Receiver') DROP SERVICE [{naming}_Receiver];");

    private static Task DeactivateSenderQueueAsync(SqlCommand sqlCommand, string naming, string schemaName)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_Sender') ALTER QUEUE [{schemaName}].[{naming}_Sender] WITH ACTIVATION (STATUS = OFF);");

    private static Task DropSenderQueueAsync(SqlCommand sqlCommand, string naming, string schemaName)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_Sender') DROP QUEUE [{schemaName}].[{naming}_Sender];");

    private static Task DropReceiverQueueAsync(SqlCommand sqlCommand, string naming, string schemaName)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.service_queues WITH (NOLOCK) WHERE schema_id = SCHEMA_ID(N'{schemaName}') AND name = N'{naming}_Receiver') DROP QUEUE [{schemaName}].[{naming}_Receiver];");

    private static Task DropContractAsync(SqlCommand sqlCommand, string naming)
        => ExecuteNonQueryAsync(sqlCommand, $"IF EXISTS (SELECT 1 FROM sys.service_contracts WITH (NOLOCK) WHERE name = N'{naming}') DROP CONTRACT [{naming}];");

    private static async Task DropMessageTypesAsync(SqlCommand sqlCommand, string naming)
    {
        var messageTypes = await GetMessageTypesAsync(sqlCommand, naming);
        foreach (var messageType in messageTypes)
            await ExecuteNonQueryAsync(sqlCommand, $"DROP MESSAGE TYPE [{messageType}];");
    }

    private static Task ExecuteNonQueryAsync(SqlCommand sqlCommand, string commandText)
    {
        sqlCommand.Parameters.Clear();
        sqlCommand.CommandText = commandText;
        return sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task EndConversationsAsync(SqlCommand sqlCommand, string naming)
    {
        sqlCommand.Parameters.Clear();
        sqlCommand.CommandText = "SELECT conversation_handle FROM sys.conversation_endpoints WITH (NOLOCK) WHERE far_service LIKE @farService;";
        sqlCommand.Parameters.AddWithValue("@farService", $"{naming}_%");

        var handles = new List<Guid>();
        await using (var reader = await sqlCommand.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                handles.Add(reader.GetGuid(0));
        }

        foreach (var handle in handles)
        {
            sqlCommand.Parameters.Clear();
            sqlCommand.CommandText = "END CONVERSATION @handle WITH CLEANUP;";
            sqlCommand.Parameters.AddWithValue("@handle", handle);
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        sqlCommand.Parameters.Clear();
    }

    private static async Task<List<string>> GetMessageTypesAsync(SqlCommand sqlCommand, string naming)
    {
        sqlCommand.Parameters.Clear();
        sqlCommand.CommandText = "SELECT name FROM sys.service_message_types WITH (NOLOCK) WHERE name LIKE @messagePrefix;";
        sqlCommand.Parameters.AddWithValue("@messagePrefix", $"{naming}/%");

        var messageTypes = new List<string>();
        await using var reader = await sqlCommand.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            messageTypes.Add(reader.GetString(0));

        return messageTypes;
    }
}