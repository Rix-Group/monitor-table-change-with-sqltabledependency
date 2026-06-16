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

global using Xunit;
global using TableDependency.SqlClient.Test.Base;
global using TableDependency.SqlClient.Test.Fixtures;
using Microsoft.Data.SqlClient;

namespace TableDependency.SqlClient.Test.Base;

public abstract class SqlTableDependencyBaseTest(DatabaseFixture databaseFixture) : IAsyncLifetime
{
    private readonly DatabaseFixture _databaseFixture = databaseFixture;

    // Admin connection for test scaffolding.
    protected string ConnectionString => _databaseFixture.MsSqlContainerConnectionString;

    // Non-admin baseline connection handed to SqlTableDependency under test (every grant plus db-wide CONTROL).
    protected string DependencyConnectionString => _databaseFixture.BaselineConnectionString;

    // Permission probes for the effective-permission guard.
    protected string NoControlConnectionString => _databaseFixture.NoControlConnectionString;
    protected string SchemaControlConnectionString => _databaseFixture.SchemaControlConnectionString;
    protected string OwnsBrokerConnectionString => _databaseFixture.OwnsBrokerConnectionString;
    protected string UnrelatedControlConnectionString => _databaseFixture.UnrelatedControlConnectionString;
    protected string ConnectOnlyConnectionString => _databaseFixture.ConnectOnlyConnectionString;
    protected static string BrokerSchemaName => DatabaseFixture.BrokerSchemaName;
    protected static string OwnsBrokerLogin => DatabaseFixture.OwnsBrokerLogin;
    protected static string UnrelatedControlLogin => DatabaseFixture.UnrelatedControlLogin;

    // Grant object-level permissions to a login once the monitored table exists (broker-schema ownership covers the rest).
    protected async Task GrantTableObjectPermissionsAsync(string login, string schemaName, string tableName, CancellationToken ct = default)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var permission in (string[])["ALTER", "SELECT"])
        {
            sqlCommand.CommandText = $"GRANT {permission} ON OBJECT::[{schemaName}].[{tableName}] TO [{login}];";
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }
    }

    protected async Task<bool> AreAllDbObjectDisposedAsync(string naming, CancellationToken ct = default)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WITH (NOLOCK) WHERE name = N'tr_{naming}_Sender'";
        var triggerExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_contracts WITH (NOLOCK) WHERE name = N'{naming}'";
        var contractExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_message_types WITH (NOLOCK) WHERE name = N'{naming}_Updated'";
        var messageExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_queues WHERE name = N'{naming}_Receiver'";
        var receiverQueueExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_queues WHERE name = N'{naming}_Sender'";
        var senderQueueExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.services WHERE name = N'{naming}_Receiver'";
        var serviceExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.objects WITH (NOLOCK) WHERE name = N'{naming}_QueueActivationSender'";
        var procedureExists = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));

        return serviceExists is 0 && senderQueueExists is 0 && receiverQueueExists is 0 && triggerExists is 0 && messageExists is 0 && procedureExists is 0 && contractExists is 0;
    }

    // Schema that owns a created object (queue/procedure/trigger), or null if it does not exist.
    protected async Task<string?> GetObjectSchemaAsync(string objectName, CancellationToken ct = default)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "SELECT SCHEMA_NAME(schema_id) FROM sys.objects WITH (NOLOCK) WHERE name = @name";
        sqlCommand.Parameters.AddWithValue("@name", objectName);
        return await sqlCommand.ExecuteScalarAsync(ct) as string;
    }

    protected async Task<int> CountConversationEndpointsAsync(string? naming = null, CancellationToken ct = default)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "select COUNT(*) from sys.conversation_endpoints WITH (NOLOCK)" + (string.IsNullOrWhiteSpace(naming) ? ";" : $" WHERE [far_service] = '{naming}_Receiver';");
        return (int)await sqlCommand.ExecuteScalarAsync(ct);
    }

    protected static byte[] GetBytes(string str, int? length = null)
    {
        if (str is null)
            return null!;

        byte[] bytes = length.HasValue
            ? new byte[length.Value]
            : new byte[str.Length * sizeof(char)];

        Buffer.BlockCopy(str.ToCharArray(), 0, bytes, 0, str.Length * sizeof(char));
        return bytes;
    }

    protected static string GetString(byte[] bytes)
    {
        if (bytes is null)
            return null!;

        char[] chars = new char[bytes.Length / sizeof(char)];
        Buffer.BlockCopy(bytes, 0, chars, 0, bytes.Length);

        return new string(chars);
    }

    public virtual ValueTask InitializeAsync()
        => ValueTask.CompletedTask;

    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}