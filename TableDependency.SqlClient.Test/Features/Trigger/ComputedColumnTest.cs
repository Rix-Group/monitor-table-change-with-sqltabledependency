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
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Trigger;

public class ComputedColumnTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class ComputedColumnModel
    {
        public string Name { get; set; } = string.Empty;

        public DateTime BirthDate { get; set; }

        [Column("Age")]
        public int CalculatedAge { get; set; }
    }

    private static readonly string TableName = typeof(ComputedColumnModel).Name;
    private int _counter;
    private readonly Dictionary<ChangeType, (ComputedColumnModel, ComputedColumnModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (ComputedColumnModel, ComputedColumnModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"ALTER TABLE [{TableName}] ADD [Age] AS DATEDIFF(YEAR, [BirthDate], GETDATE())";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Clear();
        _checkValuesOld.Clear();

        _checkValues.Add(ChangeType.Insert, (new() { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Name = "Christian", BirthDate = DateTime.Now.AddYears(-46).Date, CalculatedAge = 46 }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Name = "Nonna Velia", BirthDate = DateTime.Now.AddYears(-95).Date, CalculatedAge = 95 }, new()));
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
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<ComputedColumnModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<ComputedColumnModel>.CreateSqlTableDependencyAsync(ConnectionString, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContentAsync();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BirthDate, _checkValues[ChangeType.Insert].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.CalculatedAge, _checkValues[ChangeType.Insert].Item2.CalculatedAge);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.BirthDate, _checkValues[ChangeType.Update].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.CalculatedAge, _checkValues[ChangeType.Update].Item2.CalculatedAge);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.BirthDate, _checkValues[ChangeType.Insert].Item2.BirthDate);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.CalculatedAge, _checkValues[ChangeType.Insert].Item2.CalculatedAge);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.BirthDate, _checkValues[ChangeType.Delete].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.CalculatedAge, _checkValues[ChangeType.Delete].Item2.CalculatedAge);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<ComputedColumnModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<ComputedColumnModel>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContentAsync();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BirthDate, _checkValues[ChangeType.Insert].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.CalculatedAge, _checkValues[ChangeType.Insert].Item2.CalculatedAge);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.BirthDate, _checkValues[ChangeType.Update].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.CalculatedAge, _checkValues[ChangeType.Update].Item2.CalculatedAge);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.BirthDate, _checkValues[ChangeType.Delete].Item2.BirthDate);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.CalculatedAge, _checkValues[ChangeType.Delete].Item2.CalculatedAge);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<ComputedColumnModel> e)
    {
        _counter++;

        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.BirthDate = e.Entity.BirthDate;
        _checkValues[e.ChangeType].Item2.CalculatedAge = e.Entity.CalculatedAge;

        if (e.OldEntity is null)
        {
            _checkValuesOld.Remove(e.ChangeType);
        }
        else
        {
            _checkValuesOld[e.ChangeType].Item2.Name = e.OldEntity.Name;
            _checkValuesOld[e.ChangeType].Item2.BirthDate = e.OldEntity.BirthDate;
            _checkValuesOld[e.ChangeType].Item2.CalculatedAge = e.OldEntity.CalculatedAge;
        }
    }

    private async Task ModifyTableContentAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [BirthDate]) VALUES (@name, @birth)";
        sqlCommand.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = _checkValues[ChangeType.Insert].Item1.Name });
        sqlCommand.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert].Item1.BirthDate });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET [Name] = @name, [BirthDate] = @birth";
        sqlCommand2.Parameters.Add(new SqlParameter("@name", SqlDbType.VarChar) { Value = _checkValues[ChangeType.Update].Item1.Name });
        sqlCommand2.Parameters.Add(new SqlParameter("@birth", SqlDbType.Date) { Value = _checkValues[ChangeType.Update].Item1.BirthDate });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}