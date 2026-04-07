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

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class VectorTypeTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class VectorTypeModel
    {
        public string Embedding { get; set; } = string.Empty;
    }

    private static readonly string TableName = typeof(VectorTypeModel).Name;
    private readonly Dictionary<ChangeType, VectorTypeModel> _checkValues = [];
    private readonly Dictionary<ChangeType, VectorTypeModel?> _checkOldValues = [];

    public override async ValueTask InitializeAsync()
    {
        if (!await IsVectorAvailableAsync())
            return;

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName} (Embedding VECTOR(3) NULL);";
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
        if (!await IsVectorAvailableAsync())
            return;

        SqlTableDependency<VectorTypeModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<VectorTypeModel>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            naming = tableDependency.NamingPrefix;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal("[1.0000000e+000,2.0000000e+000,3.0000000e+000]", _checkValues[ChangeType.Insert].Embedding);
        Assert.Equal("[4.0000000e+000,5.0000000e+000,6.0000000e+000]", _checkValues[ChangeType.Update].Embedding);
        Assert.Equal("[1.0000000e+000,2.0000000e+000,3.0000000e+000]", _checkOldValues[ChangeType.Update]?.Embedding);
        Assert.Equal("[4.0000000e+000,5.0000000e+000,6.0000000e+000]", _checkValues[ChangeType.Delete].Embedding);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<VectorTypeModel> e)
    {
        _checkValues[e.ChangeType] = e.Entity;
        _checkOldValues[e.ChangeType] = e.OldEntity;
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Embedding]) VALUES (CAST('[1,2,3]' AS VECTOR(3)))";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Embedding] = CAST('[4,5,6]' AS VECTOR(3))";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<bool> IsVectorAvailableAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "SELECT COUNT(*) FROM sys.types WHERE name = 'vector'";
        var count = Convert.ToInt32(await sqlCommand.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        return count > 0;
    }
}