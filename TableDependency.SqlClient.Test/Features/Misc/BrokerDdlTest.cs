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
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.Resources;

namespace TableDependency.SqlClient.Test.Features.Misc;

// The parameterised broker DDL must still create exactly the objects the listener needs.
public class BrokerDdlTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class BrokerDdlModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = nameof(BrokerDdlModel);
    private const string BaselineUser = "td_baseline";
    private const int OwnerPrincipalId = -2;

    public override async ValueTask InitializeAsync()
    {
        await ExecuteAsync(
            $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];"
            + $"CREATE TABLE [{TableName}] ([Id] INT NOT NULL PRIMARY KEY, [Name] NVARCHAR(50) NOT NULL);",
            TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await ExecuteAsync($"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];", CancellationToken.None);
    }

    [Fact]
    public async Task Contract_ListsEveryMessageTypeSentByInitiator()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: true, ct);
        var naming = tableDependency.NamingPrefix;
        string[] expected =
        [
            $"{naming}/StartMessage/Insert", $"{naming}/StartMessage/Update", $"{naming}/StartMessage/Delete",
            $"{naming}/Id", $"{naming}/Id/old", $"{naming}/Name", $"{naming}/Name/old", $"{naming}/EndMessage"
        ];

        // ACT
        await tableDependency.StartAsync(ct: ct);

        // ASSERT
        var usages = await QueryAsync(
            "SELECT mt.name + N'|' + CAST(u.is_sent_by_initiator AS NVARCHAR(1)) + CAST(u.is_sent_by_target AS NVARCHAR(1))"
            + " FROM sys.service_contract_message_usages u"
            + " JOIN sys.service_contracts c ON c.service_contract_id = u.service_contract_id"
            + " JOIN sys.service_message_types mt ON mt.message_type_id = u.message_type_id"
            + " WHERE c.name = @naming;",
            naming, ct);
        Assert.Equal(expected.Select(m => $"{m}|10").Order(), usages.Order());
    }

    [Fact]
    public async Task Services_WithoutAuthorization_BindQueuesAndContract()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        var naming = tableDependency.NamingPrefix;

        // ACT
        await tableDependency.StartAsync(ct: ct);

        // ASSERT
        var services = await QueryAsync(
            "SELECT s.name COLLATE DATABASE_DEFAULT + N'|' + SCHEMA_NAME(q.schema_id) + N'.' + q.name + N'|' + ISNULL(c.name COLLATE DATABASE_DEFAULT, N'-')"
            + " FROM sys.services s"
            + " JOIN sys.service_queues q ON q.object_id = s.service_queue_id"
            + " LEFT JOIN sys.service_contract_usages cu ON cu.service_id = s.service_id"
            + " LEFT JOIN sys.service_contracts c ON c.service_contract_id = cu.service_contract_id"
            + " WHERE s.name IN (@naming + N'_Sender', @naming + N'_Receiver');",
            naming, ct);
        Assert.Equal(
            [$"{naming}_Receiver|{BrokerSchemaName}.{naming}_Receiver|{naming}", $"{naming}_Sender|{BrokerSchemaName}.{naming}_Sender|-"],
            services.Order());
    }

    [Fact]
    public async Task ServiceAuthorization_OwnsBothServicesAndNotificationsFlow()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        tableDependency.ServiceAuthorization = "dbo";
        var received = new TaskCompletionSource<ChangeType>(TaskCreationOptions.RunContinuationsAsynchronously);
        tableDependency.OnChanged += e => received.TrySetResult(e.ChangeType);
        await tableDependency.StartAsync(ct: ct);

        // ACT
        await ExecuteAsync($"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, N'a');", ct);

        // ASSERT
        Assert.Equal(ChangeType.Insert, await received.Task.WaitAsync(TimeSpan.FromSeconds(30), ct));
        var owners = await QueryAsync(
            "SELECT USER_NAME(principal_id) FROM sys.services WHERE name IN (@naming + N'_Sender', @naming + N'_Receiver');",
            tableDependency.NamingPrefix, ct);
        Assert.Equal(["dbo", "dbo"], owners);
    }

    [Fact]
    public async Task ServiceAuthorization_UnknownPrincipal_ThrowsAndRollsBack()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        tableDependency.ServiceAuthorization = "td_no_such_principal";

        // ACT
        var exception = await Record.ExceptionAsync(() => tableDependency.StartAsync(ct: ct));

        // ASSERT
        Assert.IsType<SqlException>(exception);
        Assert.True(await AreAllDbObjectDisposedAsync(tableDependency.NamingPrefix, ct));
    }

    [Fact]
    public async Task ServiceAuthorization_OverSysnameLength_ThrowsInsteadOfDroppingTheClause()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        tableDependency.ServiceAuthorization = new string('a', 129);

        // ACT
        var exception = await Record.ExceptionAsync(() => tableDependency.StartAsync(ct: ct));

        // ASSERT
        var sqlException = Assert.IsType<SqlException>(exception);
        Assert.Contains("exceeds 128 characters", sqlException.Message);
        Assert.True(await AreAllDbObjectDisposedAsync(tableDependency.NamingPrefix, ct));
    }

    [Fact]
    public async Task BeginConversation_OverSysnameLength_RaisesAnError()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = SqlScripts.BeginConversation;
        sqlCommand.Parameters.AddWithValue("@sender", new string('a', 129));
        sqlCommand.Parameters.AddWithValue("@receiver", "unused");
        sqlCommand.Parameters.AddWithValue("@contract", "unused");

        // ACT
        var exception = await Record.ExceptionAsync(() => sqlCommand.ExecuteScalarAsync(ct));

        // ASSERT
        var sqlException = Assert.IsType<SqlException>(exception);
        Assert.Equal(50000, sqlException.Number);
        Assert.Contains("exceeds 128 characters", sqlException.Message);
    }

    [Theory]
    [InlineData("SELF")]
    [InlineData("self")]
    [InlineData("OWNER")]
    [InlineData(BaselineUser)]
    [InlineData($"'{BaselineUser}'")]
    public async Task QueueExecuteAs_SetsActivationPrincipal(string executeAs)
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        tableDependency.QueueExecuteAs = executeAs;
        var expected = executeAs is "OWNER"
            ? OwnerPrincipalId
            : await ScalarAsync($"SELECT DATABASE_PRINCIPAL_ID(N'{BaselineUser}');", ct);

        // ACT
        await tableDependency.StartAsync(ct: ct);

        // ASSERT
        var actual = await ScalarAsync($"SELECT execute_as_principal_id FROM sys.service_queues WHERE name = N'{tableDependency.NamingPrefix}_Sender';", ct);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task CheckIfDatabaseObjectsExist_MatchesLivePrefixOnly()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await CreateAsync(includeOldEntity: false, ct);
        await tableDependency.StartAsync(ct: ct);

        // ACT
        var live = await ConnectionString.CheckIfDatabaseObjectsExistAsync(tableDependency.NamingPrefix, ct);
        var unknown = await ConnectionString.CheckIfDatabaseObjectsExistAsync(Guid.NewGuid().ToString(), ct);
        var quoted = await ConnectionString.CheckIfDatabaseObjectsExistAsync("td_it's_absent", ct);

        // ASSERT
        Assert.True(live);
        Assert.False(unknown);
        Assert.False(quoted);
    }

    private async Task<SqlTableDependency<BrokerDdlModel>> CreateAsync(bool includeOldEntity, CancellationToken ct)
    {
        var tableDependency = await SqlTableDependency<BrokerDdlModel>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, includeOldEntity: includeOldEntity, ct: ct);
        tableDependency.OnChanged += _ => { };
        return tableDependency;
    }

    private async Task<List<string>> QueryAsync(string commandText, string naming, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = commandText;
        sqlCommand.Parameters.AddWithValue("@naming", naming);

        var rows = new List<string>();
        await using var reader = await sqlCommand.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(reader.GetString(0));

        return rows;
    }

    private async Task<int> ScalarAsync(string commandText, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = commandText;
        return Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(ct));
    }

    private async Task ExecuteAsync(string commandText, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = commandText;
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }
}