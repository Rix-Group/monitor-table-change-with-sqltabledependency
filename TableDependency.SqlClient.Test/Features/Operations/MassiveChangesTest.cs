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

namespace TableDependency.SqlClient.Test.Features.Operations;

public class MassiveChangesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class MassiveChangesModel
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public const int ChangesNumber = 1000;
    private static readonly string TableName = typeof(MassiveChangesModel).Name;
    private readonly Dictionary<ChangeType, IList<MassiveChangesModel>> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id] [int] NULL, [Name] [NVARCHAR](50) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, []);
        _checkValues.Add(ChangeType.Update, []);
        _checkValues.Add(ChangeType.Delete, []);
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
        SqlTableDependency<MassiveChangesModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<MassiveChangesModel>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.True(_checkValues[ChangeType.Insert].All(m => m is { Id: 1, Name: "Luciano Bruschi" }));
        Assert.Equal(ChangesNumber, _checkValues[ChangeType.Insert].Count);
        Assert.True(_checkValues[ChangeType.Update].All(m => m is { Id: 2, Name: "Ceccarelli Velia" }));
        Assert.Equal(ChangesNumber, _checkValues[ChangeType.Update].Count);
        Assert.True(_checkValues[ChangeType.Delete].All(m => m is { Id: 2, Name: "Ceccarelli Velia" }));
        Assert.Equal(ChangesNumber, _checkValues[ChangeType.Delete].Count);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<MassiveChangesModel> e)
        => _checkValues[e.ChangeType].Add(new() { Name = e.Entity.Name, Id = e.Entity.Id });

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        for (int i = 0; i < ChangesNumber; i++)
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Id], [Name]) VALUES (1, 'Luciano Bruschi')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Id] = 2, [Name] = 'Ceccarelli Velia'";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }
}