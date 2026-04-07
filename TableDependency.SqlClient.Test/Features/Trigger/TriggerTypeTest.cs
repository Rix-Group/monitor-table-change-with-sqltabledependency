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
using TableDependency.SqlClient.Base.Exceptions;

namespace TableDependency.SqlClient.Test.Features.Trigger;

public class TriggerTypeTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class TriggerTypeTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    private static readonly string TableName = typeof(TriggerTypeTestSqlServerModel).Name;
    private int _counter;
    private readonly Dictionary<ChangeType, (TriggerTypeTestSqlServerModel, TriggerTypeTestSqlServerModel)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( "
            + "[Id] [int] IDENTITY(1, 1) NOT NULL, "
            + "[Name] [NVARCHAR](50) NOT NULL, "
            + "[Surname] [NVARCHAR](50) NOT NULL)";
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
    public async Task RaiseException1()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;

        var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
        updateOf.Add(i => i.Surname);

        bool exceptionThrown = false;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                updateOf: updateOf,
                notifyOn: NotifyOn.Insert,
                ct: TestContext.Current.CancellationToken);
        }
        catch (NotifyOnException)
        {
            exceptionThrown = true;
        }

        if (tableDependency is not null)
            await tableDependency.DisposeAsync();

        Assert.True(exceptionThrown);
    }

    [Fact]
    public async Task RaiseException2()
    {
        var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
        updateOf.Add(t => t.Surname);

        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;

        bool exceptionThrown = false;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                updateOf: updateOf,
                notifyOn: NotifyOn.Delete,
                ct: TestContext.Current.CancellationToken);
        }
        catch (NotifyOnException)
        {
            exceptionThrown = true;
        }

        if (tableDependency is not null)
            await tableDependency.DisposeAsync();

        Assert.True(exceptionThrown);
    }

    [Fact]
    public async Task RaiseException3()
    {
        var updateOf = new UpdateOfModel<TriggerTypeTestSqlServerModel>();
        updateOf.Add(t => t.Surname);

        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;

        bool exceptionThrown = false;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                updateOf: updateOf,
                notifyOn: NotifyOn.Delete | NotifyOn.Insert,
                ct: TestContext.Current.CancellationToken);
        }
        catch (NotifyOnException)
        {
            exceptionThrown = true;
        }

        if (tableDependency is not null)
            await tableDependency.DisposeAsync();

        Assert.True(exceptionThrown);
    }

    [Fact]
    public async Task OnlyInsert()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Insert,
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

        Assert.Equal(1, _counter);

        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnlyDelete()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Delete,
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

        Assert.Equal(1, _counter);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnlyUdate()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Update,
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

        Assert.Equal(1, _counter);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Update].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnlyInsertDelete()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Insert | NotifyOn.Delete,
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

        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnlyInsertUpdate()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Insert | NotifyOn.Update,
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

        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Update].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DeleteInsertUpdate()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.Delete | NotifyOn.Insert | NotifyOn.Update,
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

        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Update].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task All()
    {
        SqlTableDependency<TriggerTypeTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TriggerTypeTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                notifyOn: NotifyOn.All,
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

        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Insert].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Update].Item2.Surname);

        Assert.Equal("Pizza Funghi", _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal("Pizza Mergherita", _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<TriggerTypeTestSqlServerModel> e)
    {
        _counter++;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Surname = e.Entity.Surname;
    }

    private async Task ModifyTableContent()
    {
        _checkValues.Add(ChangeType.Insert, (new() { Id = 23, Name = "Pizza Mergherita", Surname = "Pizza Mergherita" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Mergherita" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Id = 23, Name = "Pizza Funghi", Surname = "Pizza Funghi" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Surname]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Name}', '{_checkValues[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update].Item1.Name}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}