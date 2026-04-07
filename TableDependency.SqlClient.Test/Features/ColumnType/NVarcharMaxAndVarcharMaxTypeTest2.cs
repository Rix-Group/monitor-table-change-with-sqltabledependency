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

public class NVarcharMaxAndVarcharMaxType2Model
{
    // *****************************************************
    // SQL Server Data Type Mappings:
    // https://msdn.microsoft.com/en-us/library/cc716729%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
    // *****************************************************
    public string VarcharMaxColumn { get; set; } = string.Empty;

    public string NvarcharMaxColumn { get; set; } = string.Empty;
}

public class NVarcharMaxAndVarcharMaxTypeTest2(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(NVarcharMaxAndVarcharMaxType2Model).Name;
    private readonly Dictionary<ChangeType, (NVarcharMaxAndVarcharMaxType2Model, NVarcharMaxAndVarcharMaxType2Model)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName}(varcharMAXColumn VARCHAR(MAX) NULL, NvarcharMAXColumn NVARCHAR(MAX) NULL)";
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
        SqlTableDependency<NVarcharMaxAndVarcharMaxType2Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<NVarcharMaxAndVarcharMaxType2Model>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent1();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.VarcharMaxColumn, _checkValues[ChangeType.Insert].Item2.VarcharMaxColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.NvarcharMaxColumn, _checkValues[ChangeType.Insert].Item2.NvarcharMaxColumn);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<NVarcharMaxAndVarcharMaxType2Model> e)
    {
        _checkValues[e.ChangeType].Item2.VarcharMaxColumn = e.Entity.VarcharMaxColumn;
        _checkValues[e.ChangeType].Item2.NvarcharMaxColumn = e.Entity.NvarcharMaxColumn;
    }

    private async Task ModifyTableContent1()
    {
        _checkValues.Add(ChangeType.Insert, (new() { VarcharMaxColumn = new string('¢', 6000), NvarcharMaxColumn = "мы фантастические" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([varcharMAXColumn], [nvarcharMAXColumn]) VALUES(@varcharMAXColumn, @nvarcharMAXColumn)";
        sqlCommand.Parameters.AddWithValue("@varcharMAXColumn", _checkValues[ChangeType.Insert].Item1.VarcharMaxColumn);
        sqlCommand.Parameters.AddWithValue("@nvarcharMAXColumn", _checkValues[ChangeType.Insert].Item1.NvarcharMaxColumn);
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}