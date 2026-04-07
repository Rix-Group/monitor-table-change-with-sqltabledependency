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

public class WhereWithMapperTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class WithMapperTestModel
    {
        public int Identificator { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
    }

    private const int _id = 2;

    private const string TableName = "WithMapperTestModelTable";
    private int _counter;
    private readonly Dictionary<ChangeType, (WithMapperTestModel, WithMapperTestModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (WithMapperTestModel, WithMapperTestModel)> _checkValuesOld = [];

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

        _checkValues.Add(ChangeType.Insert, (new() { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Identificator = _id, Surname = "Del Bianco", Name = "Christian" }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Identificator = _id, Surname = "Nonna", Name = "Velia" }, new()));
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
        SqlTableDependency<WithMapperTestModel>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<WithMapperTestModel>();
        mapper.AddMapping(c => c.Surname, "Second Name");
        mapper.AddMapping(c => c.Identificator, "Id");

        ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithMapperTestModel>(p => p.Identificator == _id, mapper);

        try
        {
            tableDependency = await SqlTableDependency<WithMapperTestModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                filter: filterExpression,
                includeOldEntity: false,
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Identificator, _checkValues[ChangeType.Insert].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Identificator, _checkValues[ChangeType.Update].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Surname, _checkValues[ChangeType.Update].Item2.Surname);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Identificator, _checkValues[ChangeType.Delete].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<WithMapperTestModel>? tableDependency = null;
        string naming;

        var mapper = new ModelToTableMapper<WithMapperTestModel>();
        mapper.AddMapping(c => c.Surname, "Second Name");
        mapper.AddMapping(c => c.Identificator, "Id");

        ITableDependencyFilter filterExpression = new SqlTableDependencyFilter<WithMapperTestModel>(p => p.Identificator == _id, mapper);

        try
        {
            tableDependency = await SqlTableDependency<WithMapperTestModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Identificator, _checkValues[ChangeType.Insert].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Identificator, _checkValues[ChangeType.Update].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Surname, _checkValues[ChangeType.Update].Item2.Surname);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Identificator, _checkValues[ChangeType.Insert].Item2.Identificator);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Surname, _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Identificator, _checkValues[ChangeType.Delete].Item2.Identificator);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<WithMapperTestModel> e)
    {
        _counter++;

        _checkValues[e.ChangeType].Item2.Identificator = e.Entity.Identificator;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Surname = e.Entity.Surname;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.Identificator = e.OldEntity.Identificator;
            _checkValuesOld[e.ChangeType].Item2.Name = e.OldEntity.Name;
            _checkValuesOld[e.ChangeType].Item2.Surname = e.OldEntity.Surname;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Second Name]) VALUES (999, N'Iron', N'Man')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name], [Second Name]) VALUES ({_checkValues[ChangeType.Insert].Item1.Identificator}, N'{_checkValues[ChangeType.Insert].Item1.Name}', N'{_checkValues[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'Spider', [Second Name] = 'Man' WHERE [Id] = 999";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{_checkValues[ChangeType.Update].Item1.Name}', [Second Name] =  N'{_checkValues[ChangeType.Update].Item1.Surname}' WHERE [Id] = {_checkValues[ChangeType.Update].Item1.Identificator}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = 999";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = {_checkValues[ChangeType.Delete].Item1.Identificator}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}