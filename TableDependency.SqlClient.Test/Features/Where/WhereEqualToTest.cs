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
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Features.Where;

public class WhereEqualToTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class EqualToTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private const int _id = 2;

    private static readonly string TableName = typeof(EqualToTestSqlServerModel).Name;
    private int _counter;
    private readonly Dictionary<ChangeType, (EqualToTestSqlServerModel, EqualToTestSqlServerModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (EqualToTestSqlServerModel, EqualToTestSqlServerModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( "
            + "[Id] [int] NOT NULL, "
            + "[Name] [nvarchar](50) NOT NULL, "
            + "[Second Name] [nvarchar](50) NULL, "
            + "[Born] [datetime] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { Id = _id, Name = "Christian" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Id = _id, Name = "Velia" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Id = _id, Name = "Velia" }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Id = _id, Name = "Christian" }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Id = _id, Name = "Velia" }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Id = _id, Name = "Velia" }, new()));
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID(N'{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<EqualToTestSqlServerModel>? tableDependency = null;
        string naming;

        var filterExpression = new SqlTableDependencyFilter<EqualToTestSqlServerModel>(p => p.Id == _id);

        try
        {
            tableDependency = await SqlTableDependency<EqualToTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                filter: filterExpression,
                includeOldEntity: true,
                ct: TestContext.Current.CancellationToken);

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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Id, _checkValues[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Id, _checkValues[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Id, _checkValues[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<EqualToTestSqlServerModel>? tableDependency = null;
        string naming;

        var filterExpression = new SqlTableDependencyFilter<EqualToTestSqlServerModel>(p => p.Id == _id);

        try
        {
            tableDependency = await SqlTableDependency<EqualToTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                filter: filterExpression,
                includeOldEntity: true,
                ct: TestContext.Current.CancellationToken);

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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Id, _checkValues[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Id, _checkValues[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Id, _checkValues[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Name, _checkValues[ChangeType.Insert].Item2.Name);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Id, _checkValues[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<EqualToTestSqlServerModel> e)
    {
        _counter++;

        _checkValues[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.Id = e.OldEntity.Id;
            _checkValuesOld[e.ChangeType].Item2.Name = e.OldEntity.Name;
        }
        else
        {
            _checkValuesOld.Remove(e.ChangeType);
        }
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (999, N'Iron Man')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES ({_checkValues[ChangeType.Insert].Item1.Id}, N'{_checkValues[ChangeType.Insert].Item1.Name}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'Spider Man' WHERE [Id] = 999";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{_checkValues[ChangeType.Update].Item1.Name}' WHERE [Id] = {_checkValues[ChangeType.Update].Item1.Id}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id]= 999";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = {_checkValues[ChangeType.Delete].Item1.Id}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}