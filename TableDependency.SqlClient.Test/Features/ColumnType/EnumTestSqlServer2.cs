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

public class EnumTestSqlServer2(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public enum TestType
    {
        None = 0,
        UnitTest,
        IntegrationTest
    }

    private enum TestStatus : byte
    {
        None,
        Pass,
        Fail
    }

    public enum TesterName
    {
        DonalDuck,
        MickeyMouse
    }

    private class EnumTestSqlServerModel2
    {
        public string? ErrorMessage { get; set; }
        public TesterName TesterName { get; set; }
        public TestType TestType { get; set; }
        public TestStatus TestStatus { get; set; }
    }

    private static readonly string TableName = typeof(EnumTestSqlServerModel2).Name.ToUpper();
    private int _counter;
    private readonly Dictionary<ChangeType, (EnumTestSqlServerModel2, EnumTestSqlServerModel2)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([TesterName] nvarchar(30), [TestType] int, [TestStatus] AS (CASE WHEN LTRIM(RTRIM(ISNULL([ErrorMessage],'')))='' THEN 'Pass' ELSE 'Fail' END) PERSISTED NOT NULL, [ErrorMessage] [NVARCHAR](512) NULL)";
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
        SqlTableDependency<EnumTestSqlServerModel2>? tableDependency = null;

        try
        {
            tableDependency = await SqlTableDependency<EnumTestSqlServerModel2>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.TesterName, _checkValues[ChangeType.Insert].Item2.TesterName);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.TestStatus, _checkValues[ChangeType.Insert].Item2.TestStatus);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.TestType, _checkValues[ChangeType.Insert].Item2.TestType);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.ErrorMessage, _checkValues[ChangeType.Insert].Item2.ErrorMessage);

        Assert.Equal(_checkValues[ChangeType.Update].Item1.TesterName, _checkValues[ChangeType.Update].Item2.TesterName);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.TestType, _checkValues[ChangeType.Update].Item2.TestType);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.TestStatus, _checkValues[ChangeType.Update].Item2.TestStatus);
        Assert.Null(_checkValues[ChangeType.Update].Item2.ErrorMessage);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.TesterName, _checkValues[ChangeType.Delete].Item2.TesterName);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.TestType, _checkValues[ChangeType.Delete].Item2.TestType);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.TestStatus, _checkValues[ChangeType.Delete].Item2.TestStatus);
        Assert.Null(_checkValues[ChangeType.Delete].Item2.ErrorMessage);

        Assert.True(await AreAllDbObjectDisposedAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<EnumTestSqlServerModel2> e)
    {
        _counter++;

        switch (e.ChangeType)
        {
            case ChangeType.Insert:
                _checkValues[ChangeType.Insert].Item2.TesterName = e.Entity.TesterName;
                _checkValues[ChangeType.Insert].Item2.TestType = e.Entity.TestType;
                _checkValues[ChangeType.Insert].Item2.TestStatus = e.Entity.TestStatus;
                _checkValues[ChangeType.Insert].Item2.ErrorMessage = e.Entity.ErrorMessage;
                break;

            case ChangeType.Update:
                _checkValues[ChangeType.Update].Item2.TesterName = e.Entity.TesterName;
                _checkValues[ChangeType.Update].Item2.TestType = e.Entity.TestType;
                _checkValues[ChangeType.Update].Item2.TestStatus = e.Entity.TestStatus;
                _checkValues[ChangeType.Update].Item2.ErrorMessage = e.Entity.ErrorMessage;
                break;

            case ChangeType.Delete:
                _checkValues[ChangeType.Delete].Item2.TesterName = e.Entity.TesterName;
                _checkValues[ChangeType.Delete].Item2.TestType = e.Entity.TestType;
                _checkValues[ChangeType.Delete].Item2.TestStatus = e.Entity.TestStatus;
                _checkValues[ChangeType.Delete].Item2.ErrorMessage = e.Entity.ErrorMessage;
                break;
        }
    }

    private async Task ModifyTableContent()
    {
        _checkValues.Add(ChangeType.Insert, (new() { TesterName = TesterName.DonalDuck, TestType = TestType.IntegrationTest, TestStatus = TestStatus.Fail, ErrorMessage = "Random error" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { TesterName = TesterName.MickeyMouse, TestType = TestType.UnitTest, TestStatus = TestStatus.Pass, ErrorMessage = null! }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { TesterName = TesterName.MickeyMouse, TestType = TestType.UnitTest, TestStatus = TestStatus.Pass, ErrorMessage = null! }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([TesterName], [TestType], [ErrorMessage]) VALUES (N'{_checkValues[ChangeType.Insert].Item1.TesterName}', {_checkValues[ChangeType.Insert].Item1.TestType.GetHashCode()}, N'{_checkValues[ChangeType.Insert].Item1.ErrorMessage}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [ErrorMessage] = null, [TesterName] = N'{_checkValues[ChangeType.Update].Item1.TesterName}', [TestType] = {_checkValues[ChangeType.Update].Item1.TestType.GetHashCode()}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}