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

namespace TableDependency.SqlClient.Test.Features.Misc;

// Permission model verification (plans/control-change.md): which grant layouts let a full listen + drop
// lifecycle run, and that the effective-permission guard rejects principals lacking the actual broker rights.
public class ControlPermissionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public enum GrantLayout
    {
        DatabaseControl,
        SchemaControl,
        OwnsBroker,
        NoControl,
        UnrelatedControl,
    }

    private class Model
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "ControlPermissionModel";
    private readonly Dictionary<ChangeType, int> _changes = Enum.GetValues<ChangeType>().ToDictionary(e => e, _ => 0);

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Name] NVARCHAR(100))";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // The CONTROL-less probes get only table-object rights; everything else comes from broker-schema ownership.
        await GrantTableObjectPermissionsAsync(OwnsBrokerLogin, "dbo", TableName, TestContext.Current.CancellationToken);
        await GrantTableObjectPermissionsAsync(UnrelatedControlLogin, "dbo", TableName, TestContext.Current.CancellationToken);
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

    // None of these can write the dedicated broker schema (NoControl: db-wide ALTER only; SchemaControl: CONTROL on dbo
    // only; UnrelatedControl: CONTROL on an unrelated schema), so all are rejected naming it - UnrelatedControl would
    // have passed the old name-scan guard.
    [Theory]
    [InlineData(GrantLayout.NoControl, false)]
    [InlineData(GrantLayout.NoControl, true)]
    [InlineData(GrantLayout.SchemaControl, false)]
    [InlineData(GrantLayout.SchemaControl, true)]
    [InlineData(GrantLayout.UnrelatedControl, false)]
    [InlineData(GrantLayout.UnrelatedControl, true)]
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
        GrantLayout.SchemaControl => SchemaControlConnectionString,
        GrantLayout.OwnsBroker => OwnsBrokerConnectionString,
        GrantLayout.NoControl => NoControlConnectionString,
        GrantLayout.UnrelatedControl => UnrelatedControlConnectionString,
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