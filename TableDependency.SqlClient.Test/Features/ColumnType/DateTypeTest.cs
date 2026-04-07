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

public class DateTypeTestModel
{
    public DateTime? DateColumn { get; set; }
    public DateTime? DatetimeColumn { get; set; }
    public DateTime? Datetime2Column { get; set; }
    public DateTimeOffset? DatetimeoffsetColumn { get; set; }
}

public class DateTypeTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(DateTypeTestModel).Name;
    private readonly Dictionary<ChangeType, (DateTypeTestModel, DateTypeTestModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (DateTypeTestModel, DateTypeTestModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] (" +
            "dateColumn DATE NULL, " +
            "datetimeColumn DATETIME NULL, " +
            "datetime2Column datetime2(7) NULL, " +
            "datetimeoffsetColumn DATETIMEOFFSET(7) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { DateColumn = DateTime.Now.AddDays(-1).Date, DatetimeColumn = null, Datetime2Column = DateTime.Now.AddDays(-3), DatetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new()));
        _checkValues.Add(ChangeType.Update, (new() { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { DateColumn = DateTime.Now.AddDays(-1).Date, DatetimeColumn = null, Datetime2Column = DateTime.Now.AddDays(-3), DatetimeoffsetColumn = DateTimeOffset.Now.AddDays(-4) }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { DateColumn = null, DatetimeColumn = DateTime.Now, Datetime2Column = null, DatetimeoffsetColumn = DateTime.Now }, new()));
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
        SqlTableDependency<DateTypeTestModel>? tableDependency = null;

        try
        {
            tableDependency = await SqlTableDependency<DateTypeTestModel>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.DateColumn, _checkValues[ChangeType.Insert].Item2.DateColumn);
        Assert.Null(_checkValues[ChangeType.Insert].Item2.DatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Datetime2Column, _checkValues[ChangeType.Insert].Item2.Datetime2Column);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.DatetimeoffsetColumn, _checkValues[ChangeType.Insert].Item2.DatetimeoffsetColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Null(_checkValues[ChangeType.Update].Item2.DateColumn);
        var date1 = _checkValues[ChangeType.Update].Item1.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update].Item1.DatetimeColumn.GetValueOrDefault().Millisecond);
        var date2 = _checkValues[ChangeType.Update].Item2.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update].Item2.DatetimeColumn.GetValueOrDefault().Millisecond);
        Assert.Equal(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
        Assert.Null(_checkValues[ChangeType.Update].Item2.Datetime2Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.DatetimeoffsetColumn, _checkValues[ChangeType.Update].Item2.DatetimeoffsetColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Null(_checkValues[ChangeType.Delete].Item2.DateColumn);
        date1 = _checkValues[ChangeType.Update].Item1.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update].Item1.DatetimeColumn.GetValueOrDefault().Millisecond);
        date2 = _checkValues[ChangeType.Update].Item2.DatetimeColumn.GetValueOrDefault().AddMilliseconds(-_checkValues[ChangeType.Update].Item2.DatetimeColumn.GetValueOrDefault().Millisecond);
        Assert.Equal(date1.ToString("yyyyMMddhhmm"), date2.ToString("yyyyMMddhhmm"));
        Assert.Null(_checkValues[ChangeType.Delete].Item2.Datetime2Column);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.DatetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"), _checkValues[ChangeType.Delete].Item2.DatetimeoffsetColumn.GetValueOrDefault().ToString("yyyyMMddhhmm"));
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<DateTypeTestModel> e)
    {
        _checkValues[e.ChangeType].Item2.DateColumn = e.Entity.DateColumn;
        _checkValues[e.ChangeType].Item2.DatetimeColumn = e.Entity.DatetimeColumn;
        _checkValues[e.ChangeType].Item2.Datetime2Column = e.Entity.Datetime2Column;
        _checkValues[e.ChangeType].Item2.DatetimeoffsetColumn = e.Entity.DatetimeoffsetColumn;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.DateColumn = e.OldEntity.DateColumn;
            _checkValuesOld[e.ChangeType].Item2.DatetimeColumn = e.OldEntity.DatetimeColumn;
            _checkValuesOld[e.ChangeType].Item2.Datetime2Column = e.OldEntity.Datetime2Column;
            _checkValuesOld[e.ChangeType].Item2.DatetimeoffsetColumn = e.OldEntity.DatetimeoffsetColumn;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([dateColumn], [datetimeColumn], [datetime2Column], [datetimeoffsetColumn]) VALUES(@dateColumn, NULL, @datetime2Column, @datetimeoffsetColumn)";
        sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert].Item1.DateColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@datetime2Column", SqlDbType.DateTime2) { Value = _checkValues[ChangeType.Insert].Item1.Datetime2Column });
        sqlCommand.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Insert].Item1.DatetimeoffsetColumn });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET [dateColumn] = NULL, [datetimeColumn] = @datetimeColumn, [datetime2Column] = NULL, [datetimeoffsetColumn] = @datetimeoffsetColumn";
        sqlCommand2.Parameters.Add(new SqlParameter("@datetimeColumn", SqlDbType.DateTime) { Value = _checkValues[ChangeType.Update].Item1.DatetimeColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@datetimeoffsetColumn", SqlDbType.DateTimeOffset) { Value = _checkValues[ChangeType.Update].Item1.DatetimeoffsetColumn });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}