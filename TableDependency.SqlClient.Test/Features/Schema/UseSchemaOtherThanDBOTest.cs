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

public class UseSchemaOtherThanDboTestSqlServer(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Table("Customers", Schema = "test_schema")]
    private class UseSchemaOtherThanDboTestSqlServerModel
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "Customers";
    private const string SchemaName = "test_schema";
    private int _counter;
    private readonly Dictionary<ChangeType, (UseSchemaOtherThanDboTestSqlServerModel, UseSchemaOtherThanDboTestSqlServerModel)> _checkValues = [];

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

        sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] ([Name] [nvarchar](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
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
            sqlCommand.CommandText = $"DROP TABLE [{SchemaName}].[{TableName}];";
            await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

            sqlCommand.CommandText = $"DROP SCHEMA [{SchemaName}];";
            await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<UseSchemaOtherThanDboTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<UseSchemaOtherThanDboTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
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
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<UseSchemaOtherThanDboTestSqlServerModel> e)
    {
        _counter++;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
    }

    private async Task ModifyTableContent()
    {
        _checkValues.Add(ChangeType.Insert, (new() { Name = "Christian" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Name = "Velia" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Name = "Velia" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO {SchemaName}.{TableName} ([Name]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Name}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE {SchemaName}.{TableName} SET [Name] = '{_checkValues[ChangeType.Update].Item1.Name}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM {SchemaName}.{TableName}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}