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

namespace TableDependency.SqlClient.Test.Features.Misc;

// Broker objects land in the broker schema (auto-detected default, or explicit override) while the trigger
// stays on the table schema - i.e. the ScriptDropAll {2} (broker) vs {5} (table) split targets the right schema.
public class BrokerSchemaRoutingTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Model
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "BrokerSchemaRoutingModel";

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Name] NVARCHAR(100))";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await GrantTableObjectPermissionsAsync(OwnsBrokerLogin, "dbo", TableName, TestContext.Current.CancellationToken);
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
    public async Task BrokerObjectsLiveInBrokerSchema_TriggerLivesOnTableSchema()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        var tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
            OwnsBrokerConnectionString, tableName: TableName, ct: ct);
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
            Assert.Equal("dbo", await GetObjectSchemaAsync($"tr_{naming}_Sender", ct));
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
}