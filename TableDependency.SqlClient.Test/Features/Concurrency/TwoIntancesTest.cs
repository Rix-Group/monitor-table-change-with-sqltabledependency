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
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Concurrency;

public class TwoIntancesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class TwoIntancesModel
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName1 = "TwoIntancesModel1";
    private const string TableName2 = "TwoIntancesModel2";
    private readonly Dictionary<ChangeType, IList<TwoIntancesModel>> _checkValues1 = [];
    private readonly Dictionary<ChangeType, IList<TwoIntancesModel>> _checkValues2 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues1.Add(ChangeType.Insert, []);
        _checkValues1.Add(ChangeType.Update, []);
        _checkValues1.Add(ChangeType.Delete, []);

        _checkValues2.Add(ChangeType.Insert, []);
        _checkValues2.Add(ChangeType.Update, []);
        _checkValues2.Add(ChangeType.Delete, []);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<TwoIntancesModel>? tableDependency1 = null;
        SqlTableDependency<TwoIntancesModel>? tableDependency2 = null;
        string naming1;
        string naming2;

        try
        {
            tableDependency1 = await SqlTableDependency<TwoIntancesModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName1, ct: TestContext.Current.CancellationToken);
            tableDependency1.OnChanged += TableDependency_Changed1;
            naming1 = tableDependency1.NamingPrefix;
            tableDependency2 = await SqlTableDependency<TwoIntancesModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName2, ct: TestContext.Current.CancellationToken);
            tableDependency2.OnChanged += TableDependency_Changed2;
            naming2 = tableDependency2.NamingPrefix;

            await tableDependency1.StartAsync(ct: TestContext.Current.CancellationToken);
            await tableDependency2.StartAsync(ct: TestContext.Current.CancellationToken);

            var t1 = ModifyTableContent1();
            var t2 = ModifyTableContent2();

            await Task.WhenAll(t1, t2);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency1 is not null)
                await tableDependency1.DisposeAsync();

            if (tableDependency2 is not null)
                await tableDependency2.DisposeAsync();
        }

        Assert.True(_checkValues1[ChangeType.Insert].All(m => m is { Id: 1, Name: "Luciano Bruschi" }));
        Assert.Equal(50, _checkValues1[ChangeType.Insert].Count);
        Assert.True(_checkValues1[ChangeType.Update].All(m => m is { Id: 2, Name: "Ceccarelli Velia" }));
        Assert.Equal(50, _checkValues1[ChangeType.Update].Count);
        Assert.True(_checkValues1[ChangeType.Delete].All(m => m is { Id: 2, Name: "Ceccarelli Velia" }));
        Assert.Equal(50, _checkValues1[ChangeType.Delete].Count);

        Assert.True(_checkValues2[ChangeType.Insert].All(m => m is { Id: 1, Name: "Christian Del Bianco" }));
        Assert.Equal(50, _checkValues2[ChangeType.Insert].Count);
        Assert.True(_checkValues2[ChangeType.Update].All(m => m is { Id: 2, Name: "Dina Bruschi" }));
        Assert.Equal(50, _checkValues2[ChangeType.Update].Count);
        Assert.True(_checkValues2[ChangeType.Delete].All(m => m is { Id: 2, Name: "Dina Bruschi" }));
        Assert.Equal(50, _checkValues2[ChangeType.Delete].Count);

        Assert.True(await AreAllDbObjectDisposedAsync(naming1, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming1, TestContext.Current.CancellationToken));

        Assert.True(await AreAllDbObjectDisposedAsync(naming2, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming2, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<TwoIntancesModel> e)
        => _checkValues1[e.ChangeType].Add(new() { Name = e.Entity.Name, Id = e.Entity.Id });

    private void TableDependency_Changed2(RecordChangedEventArgs<TwoIntancesModel> e)
        => _checkValues2[e.ChangeType].Add(new() { Name = e.Entity.Name, Id = e.Entity.Id });

    private async Task ModifyTableContent1()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        for (int i = 0; i < 50; i++)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName1}] ([Id], [Name]) VALUES (1, 'Luciano Bruschi')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"UPDATE [{TableName1}] SET [Id] = 2, [Name] = 'Ceccarelli Velia'";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"DELETE FROM [{TableName1}]";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task ModifyTableContent2()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        for (int i = 0; i < 50; i++)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName2}] ([Id], [Name]) VALUES (1, 'Christian Del Bianco')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"UPDATE [{TableName2}] SET [Id] = 2, [Name] = 'Dina Bruschi'";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"DELETE FROM [{TableName2}]";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }
}