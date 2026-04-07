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

public class EventForSpecificColumnsTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class EventForSpecificColumnsTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    private static readonly string TableName = typeof(EventForSpecificColumnsTestSqlServerModel).Name;
    private int _counter;
    private readonly Dictionary<ChangeType, (EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (EventForSpecificColumnsTestSqlServerModel, EventForSpecificColumnsTestSqlServerModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( " +
            "[Id][int] IDENTITY(1, 1) NOT NULL, " +
            "[First Name] [nvarchar](50) NOT NULL, " +
            "[Second Name] [nvarchar](50) NOT NULL, " +
            "[Born] [datetime] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Name = "Velia" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Name = "Velia", Surname = "Del Bianco" }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Name = "Velia" }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Name = "Velia", Surname = "Del Bianco" }, new()));
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
        SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<EventForSpecificColumnsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var updateOf = new UpdateOfModel<EventForSpecificColumnsTestSqlServerModel>();
            updateOf.Add(i => i.Surname);

            tableDependency = await SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                updateOf: updateOf,
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

        Assert.Equal(2, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<EventForSpecificColumnsTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

            var updateOf = new UpdateOfModel<EventForSpecificColumnsTestSqlServerModel>();
            updateOf.Add(i => i.Surname);

            tableDependency = await SqlTableDependency<EventForSpecificColumnsTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                updateOf: updateOf,
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

        Assert.Equal(2, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<EventForSpecificColumnsTestSqlServerModel> e)
    {
        _counter++;

        if (e.ChangeType is ChangeType.Update)
            Assert.Fail("Not expected event!");

        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Surname = e.Entity.Surname;

        if (e.OldEntity is not null)
        {
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Name}', '{_checkValues[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = '{_checkValues[ChangeType.Update].Item1.Name}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}