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
using TableDependency.SqlClient.Enums;

namespace TableDependency.SqlClient.Test.Features.Status;

public class EndpointsStatusTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class EndpointsStatusModel
    {
        public long Id { get; set; }
    }

    private static readonly string TableName = typeof(EndpointsStatusModel).Name;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName} ([Id] BIGINT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID ('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        bool startReceivingMessages = false;

        var tableDependency = await SqlTableDependency<EndpointsStatusModel>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
        tableDependency.OnChanged += _ => startReceivingMessages = true;
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        var naming = tableDependency.NamingPrefix;

        Assert.True(await IsSenderEndpointInStatus(naming, ConversationEndpointState.SO));
        Assert.True(await IsReceiverEndpointInStatus(naming, null));

        var t = InsertRecord();

        while (!startReceivingMessages)
            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        Assert.True(await IsSenderEndpointInStatus(naming, ConversationEndpointState.CO));
        Assert.True(await IsReceiverEndpointInStatus(naming, ConversationEndpointState.CO));

        await tableDependency.StopAsync();

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

        await t; // Ensure task has finished
    }

    private async Task InsertRecord()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id]) VALUES ({DateTime.Now.Ticks})"; await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<bool> IsSenderEndpointInStatus(string objectNaming, ConversationEndpointState? status)
        => status == await RetrieveEndpointStatus($"{objectNaming}_Receiver");

    private async Task<bool> IsReceiverEndpointInStatus(string objectNaming, ConversationEndpointState? status)
        => status == await RetrieveEndpointStatus($"{objectNaming}_Sender");

    private async Task<ConversationEndpointState?> RetrieveEndpointStatus(string farService)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"select [state] from sys.conversation_endpoints WITH (NOLOCK) where [far_service] = '{farService}';";
        var state = (string)await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken);

        return string.IsNullOrWhiteSpace(state)
            ? null
            : Enum.Parse<ConversationEndpointState>(state);
    }
}