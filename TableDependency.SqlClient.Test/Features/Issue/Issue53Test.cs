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

namespace TableDependency.SqlClient.Test.Features.Issue;

public enum Sex
{
    Male,
    Female
}

internal class Issue53Model1
{
    public int Id { get; set; }
    public Sex? Sex { get; set; }
}

internal class Issue53Model2
{
    public int Id { get; set; }
    public Sex Sex { get; set; }
}

internal class Issue53Model3
{
    public int Id { get; set; }
    public Sex? Sex { get; set; }
}

internal class Issue53Model4
{
    public int Id { get; set; }
    public Sex Sex { get; set; }
}

public class Issue53Test(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName1 = typeof(Issue53Model1).Name;
    private static readonly string TableName2 = typeof(Issue53Model2).Name;
    private static readonly string TableName3 = typeof(Issue53Model3).Name;
    private static readonly string TableName4 = typeof(Issue53Model4).Name;
    private readonly Dictionary<ChangeType, (Issue53Model1, Issue53Model1)> _checkValues1 = [];
    private readonly Dictionary<ChangeType, (Issue53Model2, Issue53Model2)> _checkValues2 = [];
    private readonly Dictionary<ChangeType, (Issue53Model3, Issue53Model3)> _checkValues3 = [];
    private readonly Dictionary<ChangeType, (Issue53Model4, Issue53Model4)> _checkValues4 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName1}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName1}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Id] [int] NULL, [Sex] [int])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName2}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName2}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Id] [int] NULL, [Sex] [int])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName3}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName3}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName3}]([Id] [int] NULL, [Sex] [int] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName4}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName4}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName4}]([Id] [int] NULL, [Sex] [int] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
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

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName3}', 'U') IS NOT NULL DROP TABLE [{TableName3}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName4}', 'U') IS NOT NULL DROP TABLE [{TableName4}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test1()
    {
        SqlTableDependency<Issue53Model1>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue53Model1>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName1, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed1;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent1();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(_checkValues1[ChangeType.Insert].Item1.Id, _checkValues1[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues1[ChangeType.Insert].Item1.Sex, _checkValues1[ChangeType.Insert].Item2.Sex);
        Assert.Equal(_checkValues1[ChangeType.Update].Item1.Id, _checkValues1[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues1[ChangeType.Update].Item1.Sex, _checkValues1[ChangeType.Update].Item2.Sex);
        Assert.Equal(_checkValues1[ChangeType.Delete].Item1.Id, _checkValues1[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues1[ChangeType.Delete].Item1.Sex, _checkValues1[ChangeType.Delete].Item2.Sex);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test2()
    {
        SqlTableDependency<Issue53Model2>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue53Model2>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName2, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed2;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent2();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(_checkValues2[ChangeType.Insert].Item1.Id, _checkValues2[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues2[ChangeType.Insert].Item1.Sex, _checkValues2[ChangeType.Insert].Item2.Sex);
        Assert.Equal(_checkValues2[ChangeType.Update].Item1.Id, _checkValues2[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues2[ChangeType.Update].Item1.Sex, _checkValues2[ChangeType.Update].Item2.Sex);
        Assert.Equal(_checkValues2[ChangeType.Delete].Item1.Id, _checkValues2[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues2[ChangeType.Delete].Item1.Sex, _checkValues2[ChangeType.Delete].Item2.Sex);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test3()
    {
        SqlTableDependency<Issue53Model3>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue53Model3>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName3, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed3;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent3();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(_checkValues3[ChangeType.Insert].Item1.Id, _checkValues3[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues3[ChangeType.Insert].Item1.Sex, _checkValues3[ChangeType.Insert].Item2.Sex);
        Assert.Equal(_checkValues3[ChangeType.Update].Item1.Id, _checkValues3[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues3[ChangeType.Update].Item1.Sex, _checkValues3[ChangeType.Update].Item2.Sex);
        Assert.Equal(_checkValues3[ChangeType.Delete].Item1.Id, _checkValues3[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues3[ChangeType.Delete].Item1.Sex, _checkValues3[ChangeType.Delete].Item2.Sex);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test4()
    {
        SqlTableDependency<Issue53Model4>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue53Model4>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName4, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed4;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent4();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(_checkValues4[ChangeType.Insert].Item1.Id, _checkValues4[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues4[ChangeType.Insert].Item1.Sex, _checkValues4[ChangeType.Insert].Item2.Sex);
        Assert.Equal(_checkValues4[ChangeType.Update].Item1.Id, _checkValues4[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues4[ChangeType.Update].Item1.Sex, _checkValues4[ChangeType.Update].Item2.Sex);
        Assert.Equal(_checkValues4[ChangeType.Delete].Item1.Id, _checkValues4[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues4[ChangeType.Delete].Item1.Sex, _checkValues4[ChangeType.Delete].Item2.Sex);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<Issue53Model1> e)
    {
        _checkValues1[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValues1[e.ChangeType].Item2.Sex = e.Entity.Sex;
    }

    private void TableDependency_Changed2(RecordChangedEventArgs<Issue53Model2> e)
    {
        _checkValues2[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValues2[e.ChangeType].Item2.Sex = e.Entity.Sex;
    }

    private void TableDependency_Changed3(RecordChangedEventArgs<Issue53Model3> e)
    {
        _checkValues3[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValues3[e.ChangeType].Item2.Sex = e.Entity.Sex;
    }

    private void TableDependency_Changed4(RecordChangedEventArgs<Issue53Model4> e)
    {
        _checkValues4[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValues4[e.ChangeType].Item2.Sex = e.Entity.Sex;
    }

    private async Task ModifyTableContent1()
    {
        _checkValues1.Add(ChangeType.Insert, (new() { Id = 23, Sex = Sex.Female }, new()));
        _checkValues1.Add(ChangeType.Update, (new() { Id = 4, Sex = Sex.Male }, new()));
        _checkValues1.Add(ChangeType.Delete, (new() { Id = 4, Sex = Sex.Male }, new()));

        await ModifyTableContent(
            TableName1,
            _checkValues1[ChangeType.Insert].Item1.Id,
            _checkValues1[ChangeType.Insert].Item1.Sex.GetHashCode(),
            _checkValues1[ChangeType.Update].Item1.Id,
            _checkValues1[ChangeType.Update].Item1.Sex.GetHashCode());
    }

    private async Task ModifyTableContent2()
    {
        _checkValues2.Add(ChangeType.Insert, (new() { Id = 9, Sex = Sex.Female }, new()));
        _checkValues2.Add(ChangeType.Update, (new() { Id = 4, Sex = Sex.Male }, new()));
        _checkValues2.Add(ChangeType.Delete, (new() { Id = 4, Sex = Sex.Male }, new()));

        await ModifyTableContent(
            TableName2,
            _checkValues2[ChangeType.Insert].Item1.Id,
            _checkValues2[ChangeType.Insert].Item1.Sex.GetHashCode(),
            _checkValues2[ChangeType.Update].Item1.Id,
            _checkValues2[ChangeType.Update].Item1.Sex.GetHashCode());
    }

    private async Task ModifyTableContent3()
    {
        _checkValues3.Add(ChangeType.Insert, (new() { Id = 7, Sex = Sex.Female }, new()));
        _checkValues3.Add(ChangeType.Update, (new() { Id = 4, Sex = Sex.Male }, new()));
        _checkValues3.Add(ChangeType.Delete, (new() { Id = 4, Sex = Sex.Male }, new()));

        await ModifyTableContent(
            TableName3,
            _checkValues3[ChangeType.Insert].Item1.Id,
            _checkValues3[ChangeType.Insert].Item1.Sex.GetHashCode(),
            _checkValues3[ChangeType.Update].Item1.Id,
            _checkValues3[ChangeType.Update].Item1.Sex.GetHashCode());
    }

    private async Task ModifyTableContent4()
    {
        _checkValues4.Add(ChangeType.Insert, (new() { Id = 57, Sex = Sex.Female }, new()));
        _checkValues4.Add(ChangeType.Update, (new() { Id = 4, Sex = Sex.Male }, new()));
        _checkValues4.Add(ChangeType.Delete, (new() { Id = 4, Sex = Sex.Male }, new()));

        await ModifyTableContent(
            TableName4,
            _checkValues4[ChangeType.Insert].Item1.Id,
            _checkValues4[ChangeType.Insert].Item1.Sex.GetHashCode(),
            _checkValues4[ChangeType.Update].Item1.Id,
            _checkValues4[ChangeType.Update].Item1.Sex.GetHashCode());
    }

    private async Task ModifyTableContent(string tableName, int idInsert, int SexInsert, int idUpdate, int SexUpdate)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{tableName}] ([Id], [Sex]) VALUES ({idInsert}, {SexInsert})";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{tableName}] SET [Id] = {idUpdate}, [Sex] = {SexUpdate}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{tableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}