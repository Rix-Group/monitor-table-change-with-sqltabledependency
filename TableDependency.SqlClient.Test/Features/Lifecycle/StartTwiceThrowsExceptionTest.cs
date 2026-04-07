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
using TableDependency.SqlClient.Base.Exceptions;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class StartTwiceThrowsExceptionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private sealed class StartTwiceThrowsExceptionTestModel
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = nameof(StartTwiceThrowsExceptionTestModel);

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL, [Name] NVARCHAR(50) NOT NULL);";
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

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<StartTwiceThrowsExceptionTestModel>? tableDependency = null;
        string naming = string.Empty;

        try
        {
            tableDependency = await SqlTableDependency<StartTwiceThrowsExceptionTestModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += _ => { };

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await Assert.ThrowsAsync<AlreadyListeningException>(()
                => tableDependency.StartAsync(ct: TestContext.Current.CancellationToken));
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.False(string.IsNullOrWhiteSpace(naming));
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }
}