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
using System.Linq.Expressions;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Features.Where;

public class WhereMultipleConditionsTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class ProdottiSqlServerModel
    {
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public int ItemsInStock { get; set; }
    }

    private static readonly string TableName = typeof(ProdottiSqlServerModel).Name;
    private readonly Dictionary<ChangeType, int> _ids = [];
    private int _counter;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NOT NULL, [CategoryId] [int] NOT NULL, [Quantity] [int] NOT NULL)";
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
        SqlTableDependency<ProdottiSqlServerModel>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<ProdottiSqlServerModel>();
        mapper.AddMapping(c => c.ItemsInStock, "Quantity");

        ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<ProdottiSqlServerModel>(p => (p.CategoryId == 1 || p.CategoryId == 2) && p.ItemsInStock <= 10, mapper);

        try
        {
            tableDependency = await SqlTableDependency<ProdottiSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, mapper: mapper, filter: whereCondition, ct: TestContext.Current.CancellationToken);
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
        Assert.Equal(1, _ids[ChangeType.Insert]);
        Assert.Equal(2, _ids[ChangeType.Update]);
        Assert.Equal(2, _ids[ChangeType.Delete]);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<ProdottiSqlServerModel> e)
    {
        _counter++;
        _ids[e.ChangeType] = e.Entity.Id;
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();

        // Notification: YES
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (1, 1, 9)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (2, 2, 11)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [CategoryId], [Quantity]) VALUES (3, 3, 3)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 19 WHERE [CategoryId] = 1";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: YES
        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 1 WHERE [CategoryId] = 2";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Quantity] = 1 WHERE [CategoryId] = 3";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 1";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: YES
        sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 2";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // Notification: NO
        sqlCommand.CommandText = $"DELETE from [{TableName}] WHERE [CategoryId] = 3";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}