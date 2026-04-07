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

namespace TableDependency.SqlClient.Test.Features.Pr;

public class Pr232(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Model
    {
        public string Name { get; set; } = string.Empty;
    }

    private const string TableName = "Model";
    private readonly Dictionary<ChangeType, int> _changes = Enum.GetValues<ChangeType>().ToDictionary(e => e, _ => 0);

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName} ([Name] NVARCHAR(100))";
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
        SqlTableDependency<Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Model>.CreateSqlTableDependencyAsync(ConnectionString, persistentId: "persistent", ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += OnChanged;
            tableDependency.OnException += e => Assert.Fail($"EventArg Message: {e.Message}; Exception Message: {e.Exception?.Message}; Exception Stack Trace: {e.Exception?.StackTrace}");

            // Check there's no pre-existing objects or conversations
            Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

            // Check stop doesn't drop objects and conversation
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(1, _changes[ChangeType.Insert]);
            Assert.Equal(1, _changes[ChangeType.Update]);
            Assert.Equal(1, _changes[ChangeType.Delete]);
            await tableDependency.StopAsync();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.False(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            Assert.Equal(1, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

            // Modify data, restart conversation
            await ModifyTableContent();
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(2, _changes[ChangeType.Insert]);
            Assert.Equal(2, _changes[ChangeType.Update]);
            Assert.Equal(2, _changes[ChangeType.Delete]);
            await tableDependency.StopAsync();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.False(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            Assert.Equal(1, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

            // Wait for conversation to close
            while (await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken) > 0)
                await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            // Modify data, create conversation
            Assert.False(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            await ModifyTableContent();
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
            Assert.Equal(3, _changes[ChangeType.Insert]);
            Assert.Equal(3, _changes[ChangeType.Update]);
            Assert.Equal(3, _changes[ChangeType.Delete]);
            await tableDependency.StopAsync();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.False(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            Assert.Equal(1, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
        }
        finally
        {
            if (tableDependency is not null)
            {
                await tableDependency.DisposeAsync();
                await tableDependency.DropDatabaseObjectsAsync();
            }
        }

        await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void OnChanged(RecordChangedEventArgs<Model> e)
        => _changes[e.ChangeType]++;

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name]) VALUES ('Test Inserted')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = 'Test Update'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}