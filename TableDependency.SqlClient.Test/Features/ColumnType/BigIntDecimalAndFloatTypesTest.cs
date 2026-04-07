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
using System.Data;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class BigIntDecimalAndFloatTypesTestSqlServerModel
{
    public long? BigintColumn { get; set; }
    public decimal? Decimal18Column { get; set; }
    public decimal? Decimal54Column { get; set; }
    public float? FloatColumn { get; set; }
}

public class BigIntDecimalAndFloatTypesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(BigIntDecimalAndFloatTypesTestSqlServerModel).Name;
    private readonly Dictionary<ChangeType, (BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (BigIntDecimalAndFloatTypesTestSqlServerModel, BigIntDecimalAndFloatTypesTestSqlServerModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName}(" +
            "BigintColumn BIGINT NULL," +
            "Decimal18Column DECIMAL(18, 0) NULL, " +
            "Decimal54Column DECIMAL(5, 4) NULL, " +
            "FloatColumn FLOAT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Clear();
        _checkValuesOld.Clear();

        _checkValues.Add(ChangeType.Insert, (new() { BigintColumn = 123, Decimal18Column = 987654321, Decimal54Column = null, FloatColumn = null }, new()));
        _checkValues.Add(ChangeType.Update, (new() { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { BigintColumn = 123, Decimal18Column = 987654321, Decimal54Column = null, FloatColumn = null }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { BigintColumn = null, Decimal18Column = null, Decimal54Column = 6.77M, FloatColumn = 7.55F }, new()));
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
        SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BigintColumn, _checkValues[ChangeType.Insert].Item2.BigintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Decimal18Column, _checkValues[ChangeType.Insert].Item2.Decimal18Column);
        Assert.Null(_checkValues[ChangeType.Insert].Item2.Decimal54Column);
        Assert.Null(_checkValues[ChangeType.Insert].Item2.FloatColumn);

        Assert.Null(_checkValues[ChangeType.Update].Item2.BigintColumn);
        Assert.Null(_checkValues[ChangeType.Update].Item2.Decimal18Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Decimal54Column, _checkValues[ChangeType.Update].Item2.Decimal54Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.FloatColumn, _checkValues[ChangeType.Update].Item2.FloatColumn);

        Assert.Null(_checkValues[ChangeType.Delete].Item2.BigintColumn);
        Assert.Null(_checkValues[ChangeType.Delete].Item2.Decimal18Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Decimal54Column, _checkValues[ChangeType.Delete].Item2.Decimal54Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.FloatColumn, _checkValues[ChangeType.Delete].Item2.FloatColumn);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<BigIntDecimalAndFloatTypesTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BigintColumn, _checkValues[ChangeType.Insert].Item2.BigintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Decimal18Column, _checkValues[ChangeType.Insert].Item2.Decimal18Column);
        Assert.Null(_checkValues[ChangeType.Insert].Item2.Decimal54Column);
        Assert.Null(_checkValues[ChangeType.Insert].Item2.FloatColumn);

        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Null(_checkValues[ChangeType.Update].Item2.BigintColumn);
        Assert.Null(_checkValues[ChangeType.Update].Item2.Decimal18Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Decimal54Column, _checkValues[ChangeType.Update].Item2.Decimal54Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.FloatColumn, _checkValues[ChangeType.Update].Item2.FloatColumn);

        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.BigintColumn, _checkValues[ChangeType.Insert].Item2.BigintColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Decimal18Column, _checkValues[ChangeType.Insert].Item2.Decimal18Column);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Decimal54Column, _checkValues[ChangeType.Insert].Item2.Decimal54Column);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.FloatColumn, _checkValues[ChangeType.Insert].Item2.FloatColumn);

        Assert.Null(_checkValues[ChangeType.Delete].Item2.BigintColumn);
        Assert.Null(_checkValues[ChangeType.Delete].Item2.Decimal18Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Decimal54Column, _checkValues[ChangeType.Delete].Item2.Decimal54Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.FloatColumn, _checkValues[ChangeType.Delete].Item2.FloatColumn);

        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<BigIntDecimalAndFloatTypesTestSqlServerModel> e)
    {
        _checkValues[e.ChangeType].Item2.BigintColumn = e.Entity.BigintColumn;
        _checkValues[e.ChangeType].Item2.Decimal18Column = e.Entity.Decimal18Column;
        _checkValues[e.ChangeType].Item2.Decimal54Column = e.Entity.Decimal54Column;
        _checkValues[e.ChangeType].Item2.FloatColumn = e.Entity.FloatColumn;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.BigintColumn = e.OldEntity.BigintColumn;
            _checkValuesOld[e.ChangeType].Item2.Decimal18Column = e.OldEntity.Decimal18Column;
            _checkValuesOld[e.ChangeType].Item2.Decimal54Column = e.OldEntity.Decimal54Column;
            _checkValuesOld[e.ChangeType].Item2.FloatColumn = e.OldEntity.FloatColumn;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([BigintColumn], [Decimal18Column], [Decimal54Column], [FloatColumn]) VALUES ({_checkValues[ChangeType.Insert].Item1.BigintColumn}, @decimal18Column, null, null)";
        sqlCommand.Parameters.Add(new SqlParameter("@decimal18Column", SqlDbType.Decimal) { Precision = 18, Scale = 0, Value = _checkValues[ChangeType.Insert].Item1.Decimal18Column });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET [BigintColumn] = null, [Decimal18Column] = null, [Decimal54Column] = @decimal54Column, [FloatColumn] = @floatColumn";
        sqlCommand2.Parameters.Add(new SqlParameter("@decimal54Column", SqlDbType.Decimal) { Value = _checkValues[ChangeType.Update].Item1.Decimal54Column });
        sqlCommand2.Parameters.Add(new SqlParameter("@floatColumn", SqlDbType.Float) { Value = _checkValues[ChangeType.Update].Item1.FloatColumn });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}