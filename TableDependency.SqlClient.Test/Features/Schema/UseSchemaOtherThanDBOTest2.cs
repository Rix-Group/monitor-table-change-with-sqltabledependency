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
using System.ComponentModel.DataAnnotations.Schema;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Schema;

public class UseSchemaOtherThanDboTestSqlServer2(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Table("Item", Schema = "Transaction")]
    private class UseSchemaOtherThanDboTestSqlServer2Model
    {
        public Guid TransactionItemId { get; set; }
        public string Description { get; set; } = string.Empty;
    }

    private const string TableName = "Item";
    private const string SchemaName = "Transaction";
    private int _counter;
    private readonly Dictionary<ChangeType, (UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model)> _checkValues = [];
    private readonly Dictionary<ChangeType, (UseSchemaOtherThanDboTestSqlServer2Model, UseSchemaOtherThanDboTestSqlServer2Model)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF NOT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{SchemaName}') BEGIN EXEC sp_executesql N'CREATE SCHEMA [{SchemaName}];'; END;";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}' AND TABLE_SCHEMA = '{SchemaName}'";
        var exists = await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        if (exists is > 0)
        {
            sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}]";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] (TransactionItemId uniqueidentifier NULL, Description nvarchar(50) NOT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { Description = "Christian" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Description = "Velia" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Description = "Velia" }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Description = "Christian" }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Description = "Velia" }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Description = "Velia" }, new()));
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}' AND TABLE_SCHEMA = '{SchemaName}'";
        var exists = await sqlCommand.ExecuteScalarAsync(CancellationToken.None);
        if (exists is > 0)
        {
            sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}]";
            await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

            sqlCommand.CommandText = $"DROP SCHEMA [{SchemaName}];";
            await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Description, _checkValues[ChangeType.Insert].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Description, _checkValues[ChangeType.Update].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Description, _checkValues[ChangeType.Delete].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<UseSchemaOtherThanDboTestSqlServer2Model>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Description, _checkValues[ChangeType.Insert].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Description, _checkValues[ChangeType.Update].Item2.Description);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Description, _checkValues[ChangeType.Insert].Item2.Description);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Description, _checkValues[ChangeType.Delete].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<UseSchemaOtherThanDboTestSqlServer2Model> e)
    {
        _counter++;

        _checkValues[e.ChangeType].Item2.Description = e.Entity.Description;

        if (e.OldEntity is not null)
            _checkValuesOld[e.ChangeType].Item2.Description = e.OldEntity.Description;
        else
            _checkValuesOld.Remove(e.ChangeType);
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{SchemaName}].[{TableName}] ([Description]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Description}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{SchemaName}].[{TableName}] SET [Description] = '{_checkValues[ChangeType.Update].Item1.Description}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{SchemaName}].[{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}