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

namespace TableDependency.SqlClient.Test.Features.Eventing;

public class NotificationOnlyWhenEntityIsDifferentFromOldEntityTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class NotificationOnlyWhenEntityIsDifferentFromOldEntityModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
    }

    private static readonly string TableName = typeof(NotificationOnlyWhenEntityIsDifferentFromOldEntityModel).Name;
    private int _counter;
    private readonly List<(NotificationOnlyWhenEntityIsDifferentFromOldEntityModel, NotificationOnlyWhenEntityIsDifferentFromOldEntityModel)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NULL, [Second Name] [NVARCHAR](50) NULL, [NickName] [NVARCHAR](50) NULL)";
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

    #region UpdateOneInterestedColumn

    [Fact]
    public async Task UpdateOneInterestedColumn()
    {
        SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

            tableDependency = await SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(2, _counter);
        Assert.Equal(_checkValues[0].Item1.Name, _checkValues[0].Item2.Name);
        Assert.Equal(_checkValues[0].Item1.Surname, _checkValues[0].Item2.Surname);
        Assert.Equal(_checkValues[1].Item1.Name, _checkValues[1].Item2.Name);
        Assert.Equal(_checkValues[1].Item1.Surname, _checkValues[1].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel> e)
    {
        if (e.ChangeType is ChangeType.Update)
        {
            _checkValues[_counter].Item2.Name = e.Entity.Name;
            _checkValues[_counter].Item2.Surname = e.Entity.Surname;
            _counter++;
        }
    }

    private async Task ModifyTableContent1()
    {
        _checkValues.Add((new() { Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValues.Add((new() { Name = "Velia", Surname = "Del Bianco" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('xx', 'cc', 'xxxx')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[0].Item1.Name}', [Second Name] = '{_checkValues[0].Item1.Surname}', [NickName] = 'xxsds'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[1].Item1.Name}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    #endregion UpdateOneInterestedColumn

    #region UpdateTwoInterestedColumn

    [Fact]
    public async Task UpdateTwoInterestedColumn()
    {
        SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

            tableDependency = await SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(2, _counter);
        Assert.Equal(_checkValues[0].Item1.Name, _checkValues[0].Item2.Name);
        Assert.Equal(_checkValues[0].Item1.Surname, _checkValues[0].Item2.Surname);
        Assert.Equal(_checkValues[1].Item1.Name, _checkValues[1].Item2.Name);
        Assert.Equal(_checkValues[1].Item1.Surname, _checkValues[1].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed2(RecordChangedEventArgs<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel> e)
    {
        if (e.ChangeType is ChangeType.Update)
        {
            _checkValues[_counter].Item2.Name = e.Entity.Name;
            _checkValues[_counter].Item2.Surname = e.Entity.Surname;
            _counter++;
        }
    }

    private async Task ModifyTableContent2()
    {
        _checkValues.Add((new() { Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValues.Add((new() { Name = "Velia", Surname = "Ceccarelli" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'sasasa')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[0].Item1.Name}', [Second Name] = '{_checkValues[0].Item1.Surname}', [NickName] = 'wswsw'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[1].Item1.Name}', [Second Name] = '{_checkValues[1].Item1.Surname}', [NickName] = 'xxxx'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    #endregion UpdateTwoInterestedColumn

    #region UpdateNoInterestedColumn

    [Fact]
    public async Task UpdateNoInterestedColumn()
    {
        SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second NAME");

            tableDependency = await SqlTableDependency<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(0, _counter);
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed3(RecordChangedEventArgs<NotificationOnlyWhenEntityIsDifferentFromOldEntityModel> e)
    {
        if (e.ChangeType is ChangeType.Update)
            _counter++;
    }

    private async Task ModifyTableContent3()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name], [NickName]) VALUES ('Name', 'Surname', 'baba')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [NickName] = 'xxxxx'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    #endregion UpdateNoInterestedColumn
}