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

namespace TableDependency.SqlClient.Test.Features.Issue;

public class Issue66Test(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Issue66Model1
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
    }

    private class Issue66Model2 : Issue66Model1
    {
        public string Surname { get; set; } = string.Empty;
    }

    private const string TableName = "Issue66Model";
    private readonly Dictionary<ChangeType, List<Issue66Model1>> _checkValues1 = [];
    private readonly Dictionary<ChangeType, List<Issue66Model1>> _checkValuesOld1 = [];
    private readonly Dictionary<ChangeType, List<Issue66Model2>> _checkValues2 = [];
    private readonly Dictionary<ChangeType, List<Issue66Model2>> _checkValuesOld2 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}1]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}1]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}1] ([Id] [INT] NULL, [Name] [NVARCHAR](50) NULL, [City] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}2]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}2]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}2] ([Id] [INT] NULL, [Name] [NVARCHAR](50) NULL, [Second Name] [NVARCHAR](50) NULL, [City] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues1.Add(ChangeType.Insert, []);
        _checkValues1.Add(ChangeType.Update, []);
        _checkValues1.Add(ChangeType.Delete, []);
        _checkValuesOld1.Add(ChangeType.Insert, []);
        _checkValuesOld1.Add(ChangeType.Update, []);
        _checkValuesOld1.Add(ChangeType.Delete, []);

        _checkValues2.Clear();
        _checkValuesOld2.Clear();

        _checkValues2.Add(ChangeType.Insert, []);
        _checkValues2.Add(ChangeType.Update, []);
        _checkValues2.Add(ChangeType.Delete, []);
        _checkValuesOld2.Add(ChangeType.Insert, []);
        _checkValuesOld2.Add(ChangeType.Update, []);
        _checkValuesOld2.Add(ChangeType.Delete, []);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}1', 'U') IS NOT NULL DROP TABLE [{TableName}1];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}2', 'U') IS NOT NULL DROP TABLE [{TableName}2];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test1()
    {
        SqlTableDependency<Issue66Model1>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model1>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName + "1", includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(1, _checkValues1[ChangeType.Insert][0].Id);
        Assert.Equal("CHRISTIAN", _checkValues1[ChangeType.Insert][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues1[ChangeType.Insert][0].City);
        Assert.Equal(2, _checkValues1[ChangeType.Insert][1].Id);
        Assert.Equal("VALENTINA", _checkValues1[ChangeType.Insert][1].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues1[ChangeType.Insert][1].City);

        Assert.Equal("BAAR", _checkValues1[ChangeType.Update][0].City);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Update][1].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld1[ChangeType.Update][0].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld1[ChangeType.Update][1].City);

        Assert.Equal("christian", _checkValues1[ChangeType.Delete][0].Name);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Delete][0].City);
        Assert.Equal("valentina", _checkValues1[ChangeType.Delete][1].Name);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Delete][1].City);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test2()
    {
        SqlTableDependency<Issue66Model2>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<Issue66Model2>();
        mapper.AddMapping(c => c.Surname, "Second Name");

        var updateOf = new UpdateOfModel<Issue66Model2>();
        updateOf.Add(i => i.Surname);
        updateOf.Add(i => i.City);

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model2>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName + "2", mapper: mapper, updateOf: updateOf, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(1, _checkValues2[ChangeType.Insert][0].Id);
        Assert.Equal("CHRISTIAN", _checkValues2[ChangeType.Insert][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues2[ChangeType.Insert][0].City);
        Assert.Equal(2, _checkValues2[ChangeType.Insert][1].Id);
        Assert.Equal("VALENTINA", _checkValues2[ChangeType.Insert][1].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues2[ChangeType.Insert][1].City);

        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][0].City);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][1].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][0].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][1].City);

        Assert.Equal("del bianco", _checkValues2[ChangeType.Delete][0].Surname);
        Assert.Equal("christian", _checkValues2[ChangeType.Delete][0].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Delete][0].City);
        Assert.Equal("del bianco", _checkValues2[ChangeType.Delete][1].Surname);
        Assert.Equal("valentina", _checkValues2[ChangeType.Delete][1].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Delete][1].City);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test3()
    {
        SqlTableDependency<Issue66Model2>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<Issue66Model2>();
        mapper.AddMapping(c => c.Surname, "Second Name");

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model2>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName + "2", mapper: mapper, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(1, _checkValues2[ChangeType.Insert][0].Id);
        Assert.Equal("CHRISTIAN", _checkValues2[ChangeType.Insert][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues2[ChangeType.Insert][0].City);
        Assert.Equal(2, _checkValues2[ChangeType.Insert][1].Id);
        Assert.Equal("VALENTINA", _checkValues2[ChangeType.Insert][1].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues2[ChangeType.Insert][1].City);

        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][0].City);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][1].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][0].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][1].City);

        Assert.Equal("del bianco", _checkValues2[ChangeType.Delete][0].Surname);
        Assert.Equal("christian", _checkValues2[ChangeType.Delete][0].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Delete][0].City);
        Assert.Equal("del bianco", _checkValues2[ChangeType.Delete][1].Surname);
        Assert.Equal("valentina", _checkValues2[ChangeType.Delete][1].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Delete][1].City);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test4()
    {
        SqlTableDependency<Issue66Model1>? tableDependency = null;
        string naming;

        var updateOf = new UpdateOfModel<Issue66Model1>();
        updateOf.Add(i => i.Name);
        updateOf.Add(i => i.City);

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model1>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName + "1", updateOf: updateOf, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(1, _checkValues1[ChangeType.Insert][0].Id);
        Assert.Equal("CHRISTIAN", _checkValues1[ChangeType.Insert][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues1[ChangeType.Insert][0].City);
        Assert.Equal(2, _checkValues1[ChangeType.Insert][1].Id);
        Assert.Equal("VALENTINA", _checkValues1[ChangeType.Insert][1].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValues1[ChangeType.Insert][1].City);

        Assert.Equal("BAAR", _checkValues1[ChangeType.Update][0].City);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Update][1].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld1[ChangeType.Update][0].City);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld1[ChangeType.Update][1].City);

        Assert.Equal("christian", _checkValues1[ChangeType.Delete][0].Name);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Delete][0].City);
        Assert.Equal("valentina", _checkValues1[ChangeType.Delete][1].Name);
        Assert.Equal("BAAR", _checkValues1[ChangeType.Delete][1].City);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test5()
    {
        SqlTableDependency<Issue66Model2>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<Issue66Model2>();
        mapper.AddMapping(c => c.Surname, "Second Name");

        ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Issue66Model2>(p => p.Id == 1 && p.Surname == "DEL BIANCO", mapper);

        var updateOf = new UpdateOfModel<Issue66Model2>();
        updateOf.Add(i => i.Surname);
        updateOf.Add(i => i.City);

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model2>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName + "2",
                mapper: mapper,
                updateOf: updateOf,
                filter: whereCondition,
                includeOldEntity: true,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += TableDependency_Changed2;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent5();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Empty(_checkValues2[ChangeType.Insert]);

        Assert.Single(_checkValues2[ChangeType.Update]);

        Assert.Equal(1, _checkValues2[ChangeType.Update][0].Id);
        Assert.Equal("DEL BIANCO", _checkValues2[ChangeType.Update][0].Surname);
        Assert.Equal("CHRISTIAN", _checkValues2[ChangeType.Update][0].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][0].City);
        Assert.Equal(1, _checkValuesOld2[ChangeType.Update][0].Id);
        Assert.Equal("DELBIANCO", _checkValuesOld2[ChangeType.Update][0].Surname);
        Assert.Equal("CHRISTIAN", _checkValuesOld2[ChangeType.Update][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][0].City);

        Assert.Single(_checkValues2[ChangeType.Delete]);

        Assert.Equal("DEL BIANCO", _checkValues2[ChangeType.Delete][0].Surname);
        Assert.Equal("christian", _checkValues2[ChangeType.Delete][0].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Delete][0].City);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test6()
    {
        SqlTableDependency<Issue66Model2>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<Issue66Model2>();
        mapper.AddMapping(c => c.Surname, "Second Name");

        ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Issue66Model2>(p => p.Id == 1 && p.Surname == "DEL BIANCO", mapper);

        var updateOf = new UpdateOfModel<Issue66Model2>();
        updateOf.Add(i => i.Surname);
        updateOf.Add(i => i.City);

        try
        {
            tableDependency = await SqlTableDependency<Issue66Model2>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName + "2",
                mapper: mapper,
                updateOf: updateOf,
                filter: whereCondition,
                notifyOn: NotifyOn.Update,
                includeOldEntity: true,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += TableDependency_Changed2;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent5();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Empty(_checkValues2[ChangeType.Insert]);

        Assert.Single(_checkValues2[ChangeType.Update]);

        Assert.Equal(1, _checkValues2[ChangeType.Update][0].Id);
        Assert.Equal("DEL BIANCO", _checkValues2[ChangeType.Update][0].Surname);
        Assert.Equal("CHRISTIAN", _checkValues2[ChangeType.Update][0].Name);
        Assert.Equal("BAAR", _checkValues2[ChangeType.Update][0].City);
        Assert.Equal(1, _checkValuesOld2[ChangeType.Update][0].Id);
        Assert.Equal("DELBIANCO", _checkValuesOld2[ChangeType.Update][0].Surname);
        Assert.Equal("CHRISTIAN", _checkValuesOld2[ChangeType.Update][0].Name);
        Assert.Equal("LAVENA PONTE TRESA", _checkValuesOld2[ChangeType.Update][0].City);

        Assert.Empty(_checkValues2[ChangeType.Delete]);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<Issue66Model1> e)
    {
        _checkValues1[e.ChangeType].Add(e.Entity);

        if (e.ChangeType is ChangeType.Update && e.OldEntity is not null)
            _checkValuesOld1[ChangeType.Update].Add(e.OldEntity);
    }

    private void TableDependency_Changed2(RecordChangedEventArgs<Issue66Model2> e)
    {
        _checkValues2[e.ChangeType].Add(e.Entity);

        if (e.ChangeType is ChangeType.Update && e.OldEntity is not null)
            _checkValuesOld2[ChangeType.Update].Add(e.OldEntity);
    }

    private async Task ModifyTableContent1()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}1] ([Id], [Name], [City]) VALUES(1, 'CHRISTIAN', 'LAVENA PONTE TRESA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"INSERT INTO [{TableName}1] ([Id], [Name], [City]) VALUES(2, 'VALENTINA', 'LAVENA PONTE TRESA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}1] SET [City] = 'BAAR', [Name] = LOWER([Name])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}1] WHERE [Id] = 1";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}1] WHERE [Id] = 2";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task ModifyTableContent2()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(1, 'CHRISTIAN', 'DEL BIANCO', 'LAVENA PONTE TRESA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(2, 'VALENTINA', 'DEL BIANCO', 'LAVENA PONTE TRESA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [City] = 'BAAR', [Second Name] = LOWER([Second Name])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [Name] = LOWER([Name])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}2] WHERE [Id] = 1";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}2] WHERE [Id] = 2";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task ModifyTableContent5()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(1, 'CHRISTIAN', 'DELBIANCO', 'LAVENA PONTE TRESA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"INSERT INTO [{TableName}2] ([Id], [Name], [Second Name], [City]) VALUES(2, 'LEONARDO', 'DA VINCI', 'ROMA')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [City] = 'BAAR', [Second Name] = 'DEL BIANCO'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}2] SET [Name] = LOWER([Name])";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}2]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}