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

// Caller-supplied names reach DDL; they must be quoted as identifiers, never spliced in as SQL.
public class SqlInjectionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class SqlInjectionModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = nameof(SqlInjectionModel);
    private const string CanaryTableName = "SqlInjectionCanary";

    public override async ValueTask InitializeAsync()
    {
        await ExecuteAsync(
            $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];"
            + $"CREATE TABLE [{TableName}] ([Id] INT NOT NULL PRIMARY KEY, [Name] NVARCHAR(50) NOT NULL);"
            + $"IF OBJECT_ID('{CanaryTableName}', 'U') IS NOT NULL DROP TABLE [{CanaryTableName}];"
            + $"CREATE TABLE [{CanaryTableName}] ([Id] INT NOT NULL);",
            TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await ExecuteAsync(
            $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];"
            + $"IF OBJECT_ID('{CanaryTableName}', 'U') IS NOT NULL DROP TABLE [{CanaryTableName}];",
            CancellationToken.None);
    }

    [Fact]
    public async Task QueueExecuteAs_IsNotExecutedAsSql()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await SqlTableDependency<SqlInjectionModel>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, ct: ct);
        tableDependency.OnChanged += _ => { };
        tableDependency.QueueExecuteAs = $"SELF, STATUS = ON); DROP TABLE [{CanaryTableName}]; --";

        // ACT
        var exception = await Record.ExceptionAsync(() => tableDependency.StartAsync(ct: ct));

        // ASSERT
        Assert.True(await CanaryExistsAsync(ct));
        Assert.IsType<SqlException>(exception);
    }

    [Fact]
    public async Task ServiceAuthorization_IsNotExecutedAsSql()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        await using var tableDependency = await SqlTableDependency<SqlInjectionModel>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, ct: ct);
        tableDependency.OnChanged += _ => { };
        tableDependency.ServiceAuthorization = $"dbo] ON QUEUE [{BrokerSchemaName}].[{tableDependency.NamingPrefix}_Sender]; DROP TABLE IF EXISTS [{CanaryTableName}]; --";

        // ACT
        var exception = await Record.ExceptionAsync(() => tableDependency.StartAsync(ct: ct));

        // ASSERT
        Assert.True(await CanaryExistsAsync(ct));
        Assert.IsType<SqlException>(exception);
    }

    private async Task<bool> CanaryExistsAsync(CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"SELECT OBJECT_ID('{CanaryTableName}', 'U');";
        return await sqlCommand.ExecuteScalarAsync(ct) is not DBNull;
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