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
using System.Diagnostics;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class CancellationTokenTestModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Surname { get; set; } = string.Empty;
    public DateTime Born { get; set; }
    public int Quantity { get; set; }
}

public class CancellationTokenTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(CancellationTokenTestModel).Name;

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
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = cts.Token;

        await using var listenerSlq = await ListenerSlq.CreateListenerSlqAsync(ConnectionString);
        var objectNaming = listenerSlq.ObjectNaming;

        var t = ListenerSlq.RunAsync(token);

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        while (!token.IsCancellationRequested)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{DateTime.Now.Ticks}', '{DateTime.Now.Ticks}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        }

        await listenerSlq.DisposeAsync();

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));

        await t; // Ensure task has finished
    }
}

public sealed class ListenerSlq : IAsyncDisposable
{
    private readonly SqlTableDependency<CancellationTokenTestModel> _tableDependency;
    public string ObjectNaming { get; }

    public static async Task<ListenerSlq> CreateListenerSlqAsync(string connectionString)
    {
        var mapper = new ModelToTableMapper<CancellationTokenTestModel>();
        mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

        var tableDependency = await SqlTableDependency<CancellationTokenTestModel>.CreateSqlTableDependencyAsync(connectionString, mapper: mapper);
        tableDependency.OnChanged += args => Debug.WriteLine("Received:" + args.Entity.Name);
        await tableDependency.StartAsync(60, 120);
        return new(tableDependency);
    }

    private ListenerSlq(SqlTableDependency<CancellationTokenTestModel> tableDependency)
    {
        _tableDependency = tableDependency;
        ObjectNaming = tableDependency.NamingPrefix;
    }

    public static async Task RunAsync(CancellationToken token)
    {
        while (!token.IsCancellationRequested)
            await Task.Delay(TimeSpan.FromSeconds(2), CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        await _tableDependency.DisposeAsync();
    }
}