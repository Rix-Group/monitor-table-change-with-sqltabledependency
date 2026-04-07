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
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Operations;

public class MultiDmlOperationsTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class MultiDmlOperationsTestSqlServerModel : IEquatable<MultiDmlOperationsTestSqlServerModel>
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;

        public bool Equals(MultiDmlOperationsTestSqlServerModel? other)
        {
            if (other is null)
                return false;
            if (Name != other.Name)
                return false;
            if (Surname != other.Surname)
                return false;
            return true;
        }
    }

    private static readonly string TableName = typeof(MultiDmlOperationsTestSqlServerModel).Name;
    private readonly List<MultiDmlOperationsTestSqlServerModel> _modifiedValues = [];
    private readonly List<MultiDmlOperationsTestSqlServerModel> _initialValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NOT NULL, [Second Name] [NVARCHAR](50) NOT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _initialValues.Add(new() { Name = "CHRISTIAN", Surname = "DEL BIANCO" });
        _initialValues.Add(new() { Name = "VELIA", Surname = "CECCARELLI" });
        _initialValues.Add(new() { Name = "ALFREDINA", Surname = "BRUSCHI" });
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
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var item in _initialValues)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        SqlTableDependency<MultiDmlOperationsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name");
            mapper.AddMapping(c => c.Surname, "Second Name");

            tableDependency = await SqlTableDependency<MultiDmlOperationsTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.OnException += TableDependency_OnException;
            naming = tableDependency.NamingPrefix;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await MultiDeleteOperation();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _modifiedValues.Count);
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[0]));
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[1]));
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[2]));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TwoUpdateTest()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var item in _initialValues)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        SqlTableDependency<MultiDmlOperationsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name");
            mapper.AddMapping(c => c.Surname, "Second Name");

            tableDependency = await SqlTableDependency<MultiDmlOperationsTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.OnException += TableDependency_OnException;
            naming = tableDependency.NamingPrefix;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await MultiUpdateOperation("VELIA");
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(2, _modifiedValues.Count);
        Assert.Equal("VELIA", _modifiedValues[0].Name);
        Assert.Equal("VELIA", _modifiedValues[1].Name);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ThreeUpdateTest()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var item in _initialValues)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        SqlTableDependency<MultiDmlOperationsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name");
            mapper.AddMapping(c => c.Surname, "Second Name");

            tableDependency = await SqlTableDependency<MultiDmlOperationsTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.OnException += TableDependency_OnException;
            naming = tableDependency.NamingPrefix;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await MultiUpdateOperation("xxx");
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _modifiedValues.Count);
        Assert.Equal("xxx", _modifiedValues[0].Name);
        Assert.Equal("xxx", _modifiedValues[1].Name);
        Assert.Equal("xxx", _modifiedValues[2].Name);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task MultiInsertTest()
    {
        SqlTableDependency<MultiDmlOperationsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<MultiDmlOperationsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            tableDependency = await SqlTableDependency<MultiDmlOperationsTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.OnException += TableDependency_OnException;
            naming = tableDependency.NamingPrefix;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await MultiInsertOperation();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _modifiedValues.Count);
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[0]));
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[1]));
        Assert.Contains(_initialValues, i => i.Equals(_modifiedValues[2]));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private static void TableDependency_OnException(ExceptionEventArgs e)
        => Assert.Fail(e.Exception?.Message);

    private void TableDependency_Changed(RecordChangedEventArgs<MultiDmlOperationsTestSqlServerModel> e)
        => _modifiedValues.Add(e.Entity);

    private async Task MultiInsertOperation()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var item in _initialValues)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{item.Name}', '{item.Surname}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task MultiDeleteOperation()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task MultiUpdateOperation(string value)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{value}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}