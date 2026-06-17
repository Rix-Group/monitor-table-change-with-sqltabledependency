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
using TableDependency.SqlClient.Test.Support;

namespace TableDependency.SqlClient.Test.Features.Misc;

// Broker objects land in the dedicated broker schema while the trigger stays on the monitored table's schema.
public class BrokerSchemaRoutingTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Model
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "BrokerSchemaRoutingModel";
    private const string OtherSchema = "routing_schema";
    private const string OwnsBrokerLogin = "td_owns_broker";
    private string _ownsBrokerConnectionString = string.Empty;

    public override async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        _ownsBrokerConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            OwnsBrokerLogin,
            SqlServerPrincipalFactory.BrokerCreatePermissions,
            extraStatements: [SqlServerPrincipalFactory.CreateSchemaStatement(BrokerSchemaName, OwnsBrokerLogin)],
            ct);

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{OtherSchema}].[{TableName}]', 'U') IS NOT NULL DROP TABLE [{OtherSchema}].[{TableName}];"
            + $" IF OBJECT_ID('[dbo].[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}];"
            + $" IF SCHEMA_ID('{OtherSchema}') IS NULL EXEC('CREATE SCHEMA [{OtherSchema}]');"
            + $" CREATE TABLE [dbo].[{TableName}] ([Name] NVARCHAR(100));"
            + $" CREATE TABLE [{OtherSchema}].[{TableName}] ([Name] NVARCHAR(100));";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(ConnectionString, OwnsBrokerLogin, "dbo", TableName, ct);
        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(ConnectionString, OwnsBrokerLogin, OtherSchema, TableName, ct);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{OtherSchema}].[{TableName}]', 'U') IS NOT NULL DROP TABLE [{OtherSchema}].[{TableName}];"
            + $" IF OBJECT_ID('[dbo].[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}];"
            + $" IF SCHEMA_ID('{OtherSchema}') IS NOT NULL DROP SCHEMA [{OtherSchema}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    // Trigger lands on the monitored table's schema (dbo or non-dbo), broker objects always on the broker schema.
    [Theory]
    [InlineData("dbo")]
    [InlineData(OtherSchema)]
    public async Task BrokerObjectsLiveInBrokerSchema_TriggerLivesOnTableSchema(string tableSchema)
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        var tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            _ownsBrokerConnectionString, tableName: TableName, schemaName: tableSchema, ct: ct);
        var naming = tableDependency.NamingPrefix;
        tableDependency.OnChanged += _ => { };
        tableDependency.OnException += e => Assert.Fail($"OnException: {e.Message}; {e.Exception?.Message}");

        try
        {
            // ACT
            await tableDependency.StartAsync(ct: ct);

            // ASSERT - broker objects in the broker schema, trigger on the table schema
            Assert.Equal(BrokerSchemaName, await GetObjectSchemaAsync($"{naming}_Receiver", ct));
            Assert.Equal(BrokerSchemaName, await GetObjectSchemaAsync($"{naming}_Sender", ct));
            Assert.Equal(BrokerSchemaName, await GetObjectSchemaAsync($"{naming}_QueueActivationSender", ct));
            Assert.Equal(tableSchema, await GetObjectSchemaAsync($"tr_{naming}_Sender", ct));
        }
        finally
        {
            await tableDependency.StopAsync();
            await tableDependency.DisposeAsync();
        }

        // ASSERT - non-persistent stop drops everything across both schemas
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        Assert.True(await AreAllDbObjectDisposedAsync(naming, ct));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, ct));
    }

    // Schema that owns a created object (queue/procedure/trigger), or null if it does not exist.
    private async Task<string?> GetObjectSchemaAsync(string objectName, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "SELECT SCHEMA_NAME(schema_id) FROM sys.objects WITH (NOLOCK) WHERE name = @name";
        sqlCommand.Parameters.AddWithValue("@name", objectName);
        return await sqlCommand.ExecuteScalarAsync(ct) as string;
    }
}