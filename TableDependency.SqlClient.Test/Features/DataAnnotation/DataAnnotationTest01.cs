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
using System.ComponentModel.DataAnnotations.Schema;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.DataAnnotation;

public class DataAnnotationTest01(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Table("XXXX")]
    private class DataAnnotationTestSqlServer1Model
    {
        public long Id { get; set; }

        public string Name { get; set; } = string.Empty;

        [Column("YYYY")]
        public string Description { get; set; } = string.Empty;
    }

    private const string TableName = "ANItemsTableSQL";
    private int _counter;
    private readonly Dictionary<ChangeType, (DataAnnotationTestSqlServer1Model, DataAnnotationTestSqlServer1Model)> _checkValues = [];
    private readonly Dictionary<ChangeType, (DataAnnotationTestSqlServer1Model, DataAnnotationTestSqlServer1Model)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] IDENTITY(1, 1) NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { Name = "Christian", Description = "Del Bianco" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Name = "Velia", Description = "Ceccarelli" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Name = "Velia", Description = "Ceccarelli" }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Name = "Christian", Description = "Del Bianco" }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Name = "Velia", Description = "Ceccarelli" }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Name = "Velia", Description = "Ceccarelli" }, new()));
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
        SqlTableDependency<DataAnnotationTestSqlServer1Model>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<DataAnnotationTestSqlServer1Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            tableDependency = await SqlTableDependency<DataAnnotationTestSqlServer1Model>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Description, _checkValues[ChangeType.Insert].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Description, _checkValues[ChangeType.Update].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Description, _checkValues[ChangeType.Delete].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<DataAnnotationTestSqlServer1Model>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<DataAnnotationTestSqlServer1Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            tableDependency = await SqlTableDependency<DataAnnotationTestSqlServer1Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Description, _checkValues[ChangeType.Insert].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Description, _checkValues[ChangeType.Update].Item2.Description);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Description, _checkValues[ChangeType.Insert].Item2.Description);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Description, _checkValues[ChangeType.Delete].Item2.Description);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<DataAnnotationTestSqlServer1Model> e)
    {
        _counter++;

        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Description = e.Entity.Description;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.Name = e.OldEntity.Name;
            _checkValuesOld[e.ChangeType].Item2.Description = e.OldEntity.Description;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Long Description]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Name}', '{_checkValues[ChangeType.Insert].Item1.Description}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update].Item1.Name}', [Long Description] = '{_checkValues[ChangeType.Update].Item1.Description}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}