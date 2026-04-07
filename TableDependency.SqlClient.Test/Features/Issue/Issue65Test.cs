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

namespace TableDependency.SqlClient.Test.Features.Issue;

public class Issue65Test(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Issue65Model
    {
        public string Id { get; set; } = string.Empty;

        [Column(TypeName = "Date")]
        public DateTime InvoiceDate { get; set; }
    }

    private static readonly string TableName = typeof(Issue65Model).Name;
    private readonly Dictionary<ChangeType, (Issue65Model, Issue65Model)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([InvoiceDate] [DATE] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { InvoiceDate = DateTime.Now.AddDays(-51).Date }, new()));
        _checkValues.Add(ChangeType.Update, (new() { InvoiceDate = DateTime.Today }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { InvoiceDate = DateTime.Today }, new()));
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
        SqlTableDependency<Issue65Model>? tableDependency = null;

        try
        {
            tableDependency = await SqlTableDependency<Issue65Model>.CreateSqlTableDependencyAsync(ConnectionString, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.InvoiceDate, _checkValues[ChangeType.Insert].Item2.InvoiceDate);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.InvoiceDate, _checkValues[ChangeType.Update].Item2.InvoiceDate);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.InvoiceDate, _checkValues[ChangeType.Delete].Item2.InvoiceDate);
    }

    private void TableDependency_Changed(RecordChangedEventArgs<Issue65Model> e)
        => _checkValues[e.ChangeType].Item2.InvoiceDate = e.Entity.InvoiceDate;

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([InvoiceDate]) VALUES(@dateColumn)";
        sqlCommand.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = _checkValues[ChangeType.Insert].Item1.InvoiceDate });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET [InvoiceDate] = @dateColumn";
        sqlCommand2.Parameters.Add(new SqlParameter("@dateColumn", SqlDbType.Date) { Value = _checkValues[ChangeType.Update].Item1.InvoiceDate });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}