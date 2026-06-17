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
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Test.Support;

namespace TableDependency.SqlClient.Test.Features.Misc;

// Permission model verification for successful and rejected grant layouts.
public class ControlPermissionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public enum GrantLayout
    {
        DatabaseControl,
        SchemaControl,
        OwnsBroker,
        NoControl,
        UnrelatedControl,
        BrokerSchemaGrantsOnly,
    }

    private class Model
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "ControlPermissionModel";
    private const string NoControlLogin = "td_no_control";
    private const string OwnsBrokerLogin = "td_owns_broker";
    private const string SchemaControlLogin = "td_schema_control";
    private const string UnrelatedControlLogin = "td_unrelated_control";
    private const string BrokerSchemaGrantsOnlyLogin = "td_broker_grants_only";
    private const string UnrelatedSchemaName = "unrelated";
    private readonly Dictionary<ChangeType, int> _changes = Enum.GetValues<ChangeType>().ToDictionary(e => e, _ => 0);
    private string _noControlConnectionString = string.Empty;
    private string _ownsBrokerConnectionString = string.Empty;
    private string _schemaControlConnectionString = string.Empty;
    private string _unrelatedControlConnectionString = string.Empty;
    private string _brokerSchemaGrantsOnlyConnectionString = string.Empty;

    public override async ValueTask InitializeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        _noControlConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            NoControlLogin,
            SqlServerPrincipalFactory.LegacyDatabasePermissions,
            extraStatements: [],
            ct);
        _schemaControlConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            SchemaControlLogin,
            SqlServerPrincipalFactory.LegacyDatabasePermissions,
            extraStatements: [SqlServerPrincipalFactory.GrantSchemaControlStatement("dbo", SchemaControlLogin)],
            ct);
        _ownsBrokerConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            OwnsBrokerLogin,
            SqlServerPrincipalFactory.BrokerCreatePermissions,
            extraStatements: [SqlServerPrincipalFactory.CreateSchemaStatement(BrokerSchemaName, OwnsBrokerLogin)],
            ct);
        _unrelatedControlConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            UnrelatedControlLogin,
            SqlServerPrincipalFactory.BrokerCreatePermissions,
            extraStatements:
            [
                SqlServerPrincipalFactory.CreateSchemaStatement(UnrelatedSchemaName),
                SqlServerPrincipalFactory.GrantSchemaControlStatement(UnrelatedSchemaName, UnrelatedControlLogin),
            ],
            ct);
        _brokerSchemaGrantsOnlyConnectionString = await SqlServerPrincipalFactory.CreateLoginAsync(
            ConnectionString,
            BrokerSchemaGrantsOnlyLogin,
            SqlServerPrincipalFactory.BrokerCreatePermissions,
            extraStatements:
            [
                SqlServerPrincipalFactory.CreateSchemaStatement(BrokerSchemaName),
                SqlServerPrincipalFactory.GrantSchemaPermissionsStatement(BrokerSchemaName, BrokerSchemaGrantsOnlyLogin, "ALTER", "REFERENCES"),
            ],
            ct);

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Name] NVARCHAR(100))";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        // The CONTROL-less probes get only table-object rights; everything else comes from broker-schema ownership.
        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(ConnectionString, OwnsBrokerLogin, "dbo", TableName, ct);
        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(ConnectionString, UnrelatedControlLogin, "dbo", TableName, ct);
        await SqlServerPrincipalFactory.GrantTableObjectPermissionsAsync(ConnectionString, BrokerSchemaGrantsOnlyLogin, "dbo", TableName, ct);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    // Database CONTROL and broker-schema ownership both work in the dedicated schema - owns-broker holds no CONTROL.
    [Theory]
    [InlineData(GrantLayout.DatabaseControl, false)]
    [InlineData(GrantLayout.DatabaseControl, true)]
    [InlineData(GrantLayout.OwnsBroker, false)]
    [InlineData(GrantLayout.OwnsBroker, true)]
    public async Task GrantLayout_WithSufficientRights_ListensAndDrops(GrantLayout layout, bool persistent)
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        var connectionString = ConnectionStringFor(layout);
        SqlTableDependency<Model>? tableDependency = null;
        var naming = string.Empty;

        try
        {
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                connectionString, tableName: TableName, persistentId: persistent ? "control_probe" : null, ct: ct);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += e => _changes[e.ChangeType]++;
            tableDependency.OnException += e => Assert.Fail($"OnException: {e.Message}; {e.Exception?.Message}");

            // ACT - listen
            await tableDependency.StartAsync(ct: ct);
            await ModifyTableContent(ct);
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            await tableDependency.StopAsync();

            // ASSERT - listen
            Assert.Equal(1, _changes[ChangeType.Insert]);
            Assert.Equal(1, _changes[ChangeType.Update]);
            Assert.Equal(1, _changes[ChangeType.Delete]);
        }
        finally
        {
            if (tableDependency is not null)
            {
                // ACT - drop
                await tableDependency.DisposeAsync();
                await tableDependency.DropDatabaseObjectsAsync();
            }
        }

        // ASSERT - drop
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        Assert.True(await AreAllDbObjectDisposedAsync(naming, ct));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, ct));
    }

    // These layouts cannot write/RECEIVE on the dedicated broker schema, even if an old guard would have passed them.
    // BrokerSchemaGrantsOnly holds ALTER+REFERENCES but not CONTROL/ownership, so it lacks RECEIVE on the queues;
    // the effective CONTROL probe rejects it up-front instead of letting it fail later at StartAsync.
    [Theory]
    [InlineData(GrantLayout.NoControl, false)]
    [InlineData(GrantLayout.NoControl, true)]
    [InlineData(GrantLayout.SchemaControl, false)]
    [InlineData(GrantLayout.SchemaControl, true)]
    [InlineData(GrantLayout.UnrelatedControl, false)]
    [InlineData(GrantLayout.UnrelatedControl, true)]
    [InlineData(GrantLayout.BrokerSchemaGrantsOnly, false)]
    [InlineData(GrantLayout.BrokerSchemaGrantsOnly, true)]
    public async Task GrantLayout_WithoutBrokerRights_RejectedByEffectiveGuard(GrantLayout layout, bool persistent)
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        var connectionString = ConnectionStringFor(layout);

        // ACT
        var ex = await Assert.ThrowsAsync<UserWithMissingPermissionException>(
            () => SqlTableDependency<Model>.CreateSqlTableDependencyAsync(
                connectionString, tableName: TableName, persistentId: persistent ? "control_probe" : null, ct: ct));

        // ASSERT
        Assert.Contains("BROKER SCHEMA", ex.Message);
    }

    private string ConnectionStringFor(GrantLayout layout) => layout switch
    {
        GrantLayout.DatabaseControl => DependencyConnectionString,
        GrantLayout.SchemaControl => _schemaControlConnectionString,
        GrantLayout.OwnsBroker => _ownsBrokerConnectionString,
        GrantLayout.NoControl => _noControlConnectionString,
        GrantLayout.UnrelatedControl => _unrelatedControlConnectionString,
        GrantLayout.BrokerSchemaGrantsOnly => _brokerSchemaGrantsOnlyConnectionString,
        _ => throw new ArgumentOutOfRangeException(nameof(layout)),
    };

    private async Task ModifyTableContent(CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('Test Inserted')";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'Test Update'";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(ct);
    }
}