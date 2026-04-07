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

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class NoChangesDuringFirstThreeMinutesTest2(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class NoChangesDuringFirstThreeMinutesTestSqlServer2Model
    {
        public int MenuId { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
    }

    private static readonly string TableName = typeof(NoChangesDuringFirstThreeMinutesTestSqlServer2Model).Name;
    private readonly Dictionary<ChangeType, (NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model)> _checkValues1 = [];
    private readonly Dictionary<ChangeType, (NoChangesDuringFirstThreeMinutesTestSqlServer2Model, NoChangesDuringFirstThreeMinutesTestSqlServer2Model)> _checkValues2 = [];
    private int _counter;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([MenuId] [INT] NULL, [Name] [NVARCHAR](30) NULL, [Surname] [NVARCHAR](30) NULL)";
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
        SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<NoChangesDuringFirstThreeMinutesTestSqlServer2Model>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed1;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent1();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

            Assert.Equal(3, _counter);
            Assert.Equal(_checkValues1[ChangeType.Insert].Item1.Name, _checkValues1[ChangeType.Insert].Item2.Name);
            Assert.Equal(_checkValues1[ChangeType.Insert].Item1.Surname, _checkValues1[ChangeType.Insert].Item2.Surname);
            Assert.Equal(_checkValues1[ChangeType.Update].Item1.Name, _checkValues1[ChangeType.Update].Item2.Name);
            Assert.Equal(_checkValues1[ChangeType.Update].Item1.Surname, _checkValues1[ChangeType.Update].Item2.Surname);
            Assert.Equal(_checkValues1[ChangeType.Delete].Item1.Name, _checkValues1[ChangeType.Delete].Item2.Name);
            Assert.Equal(_checkValues1[ChangeType.Delete].Item1.Surname, _checkValues1[ChangeType.Delete].Item2.Surname);

            await ModifyTableContent2();
            await Task.Delay(TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);

            Assert.Equal(6, _counter);
            Assert.Equal(_checkValues2[ChangeType.Insert].Item1.Name, _checkValues2[ChangeType.Insert].Item2.Name);
            Assert.Equal(_checkValues2[ChangeType.Insert].Item1.Surname, _checkValues2[ChangeType.Insert].Item2.Surname);
            Assert.Equal(_checkValues2[ChangeType.Update].Item1.Name, _checkValues2[ChangeType.Update].Item2.Name);
            Assert.Equal(_checkValues2[ChangeType.Update].Item1.Surname, _checkValues2[ChangeType.Update].Item2.Surname);
            Assert.Equal(_checkValues2[ChangeType.Delete].Item1.Name, _checkValues2[ChangeType.Delete].Item2.Name);
            Assert.Equal(_checkValues2[ChangeType.Delete].Item1.Surname, _checkValues2[ChangeType.Delete].Item2.Surname);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<NoChangesDuringFirstThreeMinutesTestSqlServer2Model> e)
    {
        _counter++;
        if (e.Entity.MenuId is 1)
        {
            _checkValues1[e.ChangeType].Item2.Name = e.Entity.Name;
            _checkValues1[e.ChangeType].Item2.Surname = e.Entity.Surname;
        }
        else
        {
            _checkValues2[e.ChangeType].Item2.Name = e.Entity.Name;
            _checkValues2[e.ChangeType].Item2.Surname = e.Entity.Surname;
        }
    }

    private async Task ModifyTableContent1()
    {
        _checkValues1.Add(ChangeType.Insert, (new() { MenuId = 1, Name = "Pizza Prosciutto", Surname = "Pizza Prosciutto" }, new()));
        _checkValues1.Add(ChangeType.Update, (new() { MenuId = 1, Name = "Pizza Napoletana", Surname = "Pizza Prosciutto" }, new()));
        _checkValues1.Add(ChangeType.Delete, (new() { MenuId = 1, Name = "Pizza Napoletana", Surname = "Pizza Prosciutto" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([MenuId], [Name], [Surname]) VALUES ({_checkValues1[ChangeType.Insert].Item1.MenuId}, '{_checkValues1[ChangeType.Insert].Item1.Name}', '{_checkValues1[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues1[ChangeType.Update].Item1.Name}' WHERE [MenuId] = " + _checkValues1[ChangeType.Update].Item1.MenuId;
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [MenuId] = " + _checkValues1[ChangeType.Delete].Item1.MenuId;
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task ModifyTableContent2()
    {
        _checkValues2.Add(ChangeType.Insert, (new() { MenuId = 2, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new()));
        _checkValues2.Add(ChangeType.Update, (new() { MenuId = 2, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new()));
        _checkValues2.Add(ChangeType.Delete, (new() { MenuId = 2, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([MenuId], [Name], [Surname]) VALUES ({_checkValues2[ChangeType.Insert].Item1.MenuId}, '{_checkValues2[ChangeType.Insert].Item1.Name}', '{_checkValues2[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues2[ChangeType.Update].Item1.Name}' WHERE [MenuId] = " + _checkValues2[ChangeType.Update].Item1.MenuId;
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [MenuId] = " + _checkValues2[ChangeType.Delete].Item1.MenuId;
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}