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
using System.Dynamic;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Mapping;

public class ExpandoObjectTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(ExpandoObjectTest);
    private readonly Dictionary<ChangeType, ExpandoObject> _received = [];
    private readonly Dictionary<ChangeType, ExpandoObject?> _receivedOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] (" +
            "[Id] INT IDENTITY(1,1) NOT NULL," +
            "[Name] NVARCHAR(50) NOT NULL," +
            "[Notes] NVARCHAR(50) NULL);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _received.Clear();
        _receivedOld.Clear();
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
        SqlTableDependency<ExpandoObject>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<ExpandoObject>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
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

        Assert.True(_received.ContainsKey(ChangeType.Insert));
        Assert.True(_received.ContainsKey(ChangeType.Update));
        Assert.True(_received.ContainsKey(ChangeType.Delete));

        var insert = _received[ChangeType.Insert];
        Assert.Equal("1", insert.GetValue("Id"));
        Assert.Equal("Alice", insert.GetValue("Name"));
        Assert.Null(insert.GetValue("Notes"));

        var update = _received[ChangeType.Update];
        Assert.Equal("1", update.GetValue("Id"));
        Assert.Equal("Bob", update.GetValue("Name"));
        Assert.Equal("Note", update.GetValue("Notes"));

        var delete = _received[ChangeType.Delete];
        Assert.Equal("1", delete.GetValue("Id"));
        Assert.Equal("Bob", delete.GetValue("Name"));
        Assert.Equal("Note", delete.GetValue("Notes"));

        Assert.Empty(_receivedOld[ChangeType.Insert] ?? []);
        Assert.Empty(_receivedOld[ChangeType.Delete] ?? []);

        var updateOld = _receivedOld[ChangeType.Update];
        Assert.NotNull(updateOld);
        Assert.Equal("1", updateOld.GetValue("Id"));
        Assert.Equal("Alice", updateOld.GetValue("Name"));
        Assert.Null(updateOld.GetValue("Notes"));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<ExpandoObject> e)
    {
        Assert.IsType<ExpandoRecordChangedEventArgs>(e);
        _received[e.ChangeType] = e.Entity;
        _receivedOld[e.ChangeType] = e.OldEntity;
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var insertCommand = sqlConnection.CreateCommand();
        insertCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Notes]) VALUES ('Alice', NULL);";
        await insertCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var updateCommand = sqlConnection.CreateCommand();
        updateCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'Bob', [Notes] = 'Note' WHERE [Id] = 1;";
        await updateCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var deleteCommand = sqlConnection.CreateCommand();
        deleteCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [Id] = 1;";
        await deleteCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

public static class ExpandoObjectExtensions
{
    extension(ExpandoObject obj)
    {
        public object? GetValue(string key)
            => obj.FirstOrDefault(o => o.Key == key).Value;
    }
}