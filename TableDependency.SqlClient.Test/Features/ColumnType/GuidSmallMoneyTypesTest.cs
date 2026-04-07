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

public class GuidSmallMoneyTypesModel
{
    public Guid UniqueidentifierColumn { get; set; }
    public TimeSpan? Time7Column { get; set; }
    public byte TinyintColumn { get; set; }
    public DateTime SmalldatetimeColumn { get; set; }
    public short SmallintColumn { get; set; }
    public decimal MoneyColumn { get; set; }
    public decimal SmallmoneyColumn { get; set; }
}

public class GuidSmallMoneyTypesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(GuidSmallMoneyTypesModel).Name;
    private readonly Dictionary<ChangeType, (GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (GuidSmallMoneyTypesModel, GuidSmallMoneyTypesModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName}(" +
            "uniqueidentifierColumn uniqueidentifier, " +
            "time7Column time(7) NULL, " +
            "tinyintColumn tinyint NULL, " +
            "smalldatetimeColumn smalldatetime NULL, " +
            "smallintColumn smallint NULL, " +
            "moneyColumn money NULL," +
            "smallmoneyColumn smallmoney NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        // https://msdn.microsoft.com/en-us/library/bb675168%28v=vs.110%29.aspx
        _checkValues.Add(ChangeType.Insert, (new() { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = new TimeSpan(23, 59, 59), TinyintColumn = 1, SmalldatetimeColumn = DateTime.Today, SmallintColumn = 1, MoneyColumn = 123.77M, SmallmoneyColumn = 2.3M }, new()));
        _checkValues.Add(ChangeType.Update, (new() { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = new TimeSpan(13, 59, 59), TinyintColumn = 2, SmalldatetimeColumn = DateTime.Today.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { UniqueidentifierColumn = _checkValues[ChangeType.Update].Item2.UniqueidentifierColumn, Time7Column = new TimeSpan(13, 59, 59), TinyintColumn = 2, SmalldatetimeColumn = DateTime.Today.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = new TimeSpan(23, 59, 59), TinyintColumn = 1, SmalldatetimeColumn = DateTime.Today, SmallintColumn = 1, MoneyColumn = 123.77M, SmallmoneyColumn = 2.3M }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { UniqueidentifierColumn = Guid.NewGuid(), Time7Column = new TimeSpan(13, 59, 59), TinyintColumn = 2, SmalldatetimeColumn = DateTime.Today.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { UniqueidentifierColumn = _checkValues[ChangeType.Update].Item2.UniqueidentifierColumn, Time7Column = new TimeSpan(13, 59, 59), TinyintColumn = 2, SmalldatetimeColumn = DateTime.Today.AddDays(1), SmallintColumn = 1, MoneyColumn = 23.77M, SmallmoneyColumn = 1.3M }, new()));
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
        SqlTableDependency<GuidSmallMoneyTypesModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<GuidSmallMoneyTypesModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.UniqueidentifierColumn, _checkValues[ChangeType.Insert].Item2.UniqueidentifierColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Time7Column, _checkValues[ChangeType.Insert].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.TinyintColumn, _checkValues[ChangeType.Insert].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Insert].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmallintColumn, _checkValues[ChangeType.Insert].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.MoneyColumn, _checkValues[ChangeType.Insert].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmallmoneyColumn, _checkValues[ChangeType.Insert].Item2.SmallmoneyColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallintColumn, _checkValues[ChangeType.Update].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Time7Column, _checkValues[ChangeType.Update].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.TinyintColumn, _checkValues[ChangeType.Update].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Update].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallintColumn, _checkValues[ChangeType.Update].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.MoneyColumn, _checkValues[ChangeType.Update].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallmoneyColumn, _checkValues[ChangeType.Update].Item2.SmallmoneyColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallintColumn, _checkValues[ChangeType.Delete].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Time7Column, _checkValues[ChangeType.Delete].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.TinyintColumn, _checkValues[ChangeType.Delete].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Delete].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallintColumn, _checkValues[ChangeType.Delete].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.MoneyColumn, _checkValues[ChangeType.Delete].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallmoneyColumn, _checkValues[ChangeType.Delete].Item2.SmallmoneyColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<GuidSmallMoneyTypesModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<GuidSmallMoneyTypesModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.UniqueidentifierColumn, _checkValues[ChangeType.Insert].Item2.UniqueidentifierColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Time7Column, _checkValues[ChangeType.Insert].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.TinyintColumn, _checkValues[ChangeType.Insert].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Insert].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmallintColumn, _checkValues[ChangeType.Insert].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.MoneyColumn, _checkValues[ChangeType.Insert].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.SmallmoneyColumn, _checkValues[ChangeType.Insert].Item2.SmallmoneyColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallintColumn, _checkValues[ChangeType.Update].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Time7Column, _checkValues[ChangeType.Update].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.TinyintColumn, _checkValues[ChangeType.Update].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Update].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallintColumn, _checkValues[ChangeType.Update].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.MoneyColumn, _checkValues[ChangeType.Update].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.SmallmoneyColumn, _checkValues[ChangeType.Update].Item2.SmallmoneyColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.SmallintColumn, _checkValues[ChangeType.Insert].Item2.SmallintColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Time7Column, _checkValues[ChangeType.Insert].Item2.Time7Column);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.TinyintColumn, _checkValues[ChangeType.Insert].Item2.TinyintColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.SmalldatetimeColumn, _checkValues[ChangeType.Insert].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.SmallintColumn, _checkValues[ChangeType.Insert].Item2.SmallintColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.MoneyColumn, _checkValues[ChangeType.Insert].Item2.MoneyColumn);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.SmallmoneyColumn, _checkValues[ChangeType.Insert].Item2.SmallmoneyColumn);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallintColumn, _checkValues[ChangeType.Delete].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Time7Column, _checkValues[ChangeType.Delete].Item2.Time7Column);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.TinyintColumn, _checkValues[ChangeType.Delete].Item2.TinyintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmalldatetimeColumn, _checkValues[ChangeType.Delete].Item2.SmalldatetimeColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallintColumn, _checkValues[ChangeType.Delete].Item2.SmallintColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.MoneyColumn, _checkValues[ChangeType.Delete].Item2.MoneyColumn);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.SmallmoneyColumn, _checkValues[ChangeType.Delete].Item2.SmallmoneyColumn);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<GuidSmallMoneyTypesModel> e)
    {
        _checkValues[e.ChangeType].Item2.UniqueidentifierColumn = e.Entity.UniqueidentifierColumn;
        _checkValues[e.ChangeType].Item2.Time7Column = e.Entity.Time7Column;
        _checkValues[e.ChangeType].Item2.TinyintColumn = e.Entity.TinyintColumn;
        _checkValues[e.ChangeType].Item2.SmalldatetimeColumn = e.Entity.SmalldatetimeColumn;
        _checkValues[e.ChangeType].Item2.SmallintColumn = e.Entity.SmallintColumn;
        _checkValues[e.ChangeType].Item2.MoneyColumn = e.Entity.MoneyColumn;
        _checkValues[e.ChangeType].Item2.SmallmoneyColumn = e.Entity.SmallmoneyColumn;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.UniqueidentifierColumn = e.OldEntity.UniqueidentifierColumn;
            _checkValuesOld[e.ChangeType].Item2.Time7Column = e.OldEntity.Time7Column;
            _checkValuesOld[e.ChangeType].Item2.TinyintColumn = e.OldEntity.TinyintColumn;
            _checkValuesOld[e.ChangeType].Item2.SmalldatetimeColumn = e.OldEntity.SmalldatetimeColumn;
            _checkValuesOld[e.ChangeType].Item2.SmallintColumn = e.OldEntity.SmallintColumn;
            _checkValuesOld[e.ChangeType].Item2.MoneyColumn = e.OldEntity.MoneyColumn;
            _checkValuesOld[e.ChangeType].Item2.SmallmoneyColumn = e.OldEntity.SmallmoneyColumn;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] (uniqueidentifierColumn, time7Column, tinyintColumn, smalldatetimeColumn, smallintColumn, moneyColumn, smallmoneyColumn) " +
            "values (@uniqueidentifierColumn, @time7Column, @tinyintColumn, @smalldatetimeColumn, @smallintColumn, @moneyColumn, @smallmoneyColumn)";
        sqlCommand.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Insert].Item1.UniqueidentifierColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Insert].Item1.Time7Column });
        sqlCommand.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Insert].Item1.TinyintColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Insert].Item1.SmalldatetimeColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Insert].Item1.SmallintColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Insert].Item1.MoneyColumn });
        sqlCommand.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Insert].Item1.SmallmoneyColumn });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET uniqueidentifierColumn = @uniqueidentifierColumn, time7Column = @time7Column, tinyintColumn = @tinyintColumn, smalldatetimeColumn = @smalldatetimeColumn, smallintColumn = @smallintColumn, moneyColumn = @moneyColumn, smallmoneyColumn = @smallmoneyColumn";
        sqlCommand2.Parameters.Add(new SqlParameter("@uniqueidentifierColumn", SqlDbType.UniqueIdentifier) { Value = _checkValues[ChangeType.Update].Item1.UniqueidentifierColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@time7Column", SqlDbType.Time) { Value = _checkValues[ChangeType.Update].Item1.Time7Column });
        sqlCommand2.Parameters.Add(new SqlParameter("@tinyintColumn", SqlDbType.TinyInt) { Value = _checkValues[ChangeType.Update].Item1.TinyintColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@smalldatetimeColumn", SqlDbType.SmallDateTime) { Value = _checkValues[ChangeType.Update].Item1.SmalldatetimeColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@smallintColumn", SqlDbType.SmallInt) { Value = _checkValues[ChangeType.Update].Item1.SmallintColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@moneyColumn", SqlDbType.Money) { Value = _checkValues[ChangeType.Update].Item1.MoneyColumn });
        sqlCommand2.Parameters.Add(new SqlParameter("@smallmoneyColumn", SqlDbType.SmallMoney) { Value = _checkValues[ChangeType.Update].Item1.SmallmoneyColumn });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}