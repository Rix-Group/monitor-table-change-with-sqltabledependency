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
using System.Globalization;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Misc;

public class CultureInfoTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class CultureInfoTestModel
    {
        public string Name { get; set; } = string.Empty;

        public DateTime BirthDate { get; set; }
    }

    private static readonly string TableName1 = typeof(CultureInfoTestModel).Name + "1";
    private static readonly string TableName2 = typeof(CultureInfoTestModel).Name + "2";
    private int _counter1;
    private int _counter2;
    private readonly Dictionary<ChangeType, CultureInfoTestModel> CheckValues1 = [];
    private readonly Dictionary<ChangeType, CultureInfoTestModel> CheckValues2 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName1}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName2}]([Name] [NVARCHAR](50) NULL, [BirthDate] [DATETIME] NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName1}', 'U') IS NOT NULL DROP TABLE [{TableName1}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName2}', 'U') IS NOT NULL DROP TABLE [{TableName2}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test1()
    {
        SqlTableDependency<CultureInfoTestModel>? tableDependency = null;
        string naming;

        CheckValues1.Add(ChangeType.Insert, new());
        CheckValues1.Add(ChangeType.Update, new());
        CheckValues1.Add(ChangeType.Delete, new());

        try
        {
            tableDependency = await SqlTableDependency<CultureInfoTestModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName1, ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += TableDependency_Changed1;
            tableDependency.CultureInfo = new CultureInfo("it-IT");

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContent1();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter1);

        Assert.Equal("Christian", CheckValues1[ChangeType.Insert].Name);
        Assert.Equal(DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Insert].BirthDate);

        Assert.Equal("Valentina", CheckValues1[ChangeType.Update].Name);
        Assert.Equal(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Update].BirthDate);

        Assert.Equal("Valentina", CheckValues1[ChangeType.Delete].Name);
        Assert.Equal(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("it-IT")), CheckValues1[ChangeType.Delete].BirthDate);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Test2()
    {
        SqlTableDependency<CultureInfoTestModel>? tableDependency = null;
        string naming;

        CheckValues2.Add(ChangeType.Insert, new());
        CheckValues2.Add(ChangeType.Update, new());
        CheckValues2.Add(ChangeType.Delete, new());

        try
        {
            tableDependency = await SqlTableDependency<CultureInfoTestModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName2, ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;
            tableDependency.OnChanged += TableDependency_Changed2;
            tableDependency.CultureInfo = new CultureInfo("en-US");

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await ModifyTableContent2();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter2);

        Assert.Equal("Christian", CheckValues2[ChangeType.Insert].Name);
        Assert.Equal(DateTime.ParseExact("2009-08-05", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Insert].BirthDate);

        Assert.Equal("Valentina", CheckValues2[ChangeType.Update].Name);
        Assert.Equal(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Update].BirthDate);

        Assert.Equal("Valentina", CheckValues2[ChangeType.Delete].Name);
        Assert.Equal(DateTime.ParseExact("2009-05-08", "yyyy-MM-dd", new CultureInfo("en-US")), CheckValues2[ChangeType.Delete].BirthDate);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<CultureInfoTestModel> e)
    {
        _counter1++;

        switch (e.ChangeType)
        {
            case ChangeType.Insert:
                CheckValues1[ChangeType.Insert].Name = e.Entity.Name;
                CheckValues1[ChangeType.Insert].BirthDate = e.Entity.BirthDate;
                break;

            case ChangeType.Update:
                CheckValues1[ChangeType.Update].Name = e.Entity.Name;
                CheckValues1[ChangeType.Update].BirthDate = e.Entity.BirthDate;
                break;

            case ChangeType.Delete:
                CheckValues1[ChangeType.Delete].Name = e.Entity.Name;
                CheckValues1[ChangeType.Delete].BirthDate = e.Entity.BirthDate;
                break;
        }
    }

    private void TableDependency_Changed2(RecordChangedEventArgs<CultureInfoTestModel> e)
    {
        _counter2++;

        switch (e.ChangeType)
        {
            case ChangeType.Insert:
                CheckValues2[ChangeType.Insert].Name = e.Entity.Name;
                CheckValues2[ChangeType.Insert].BirthDate = e.Entity.BirthDate;
                break;

            case ChangeType.Update:
                CheckValues2[ChangeType.Update].Name = e.Entity.Name;
                CheckValues2[ChangeType.Update].BirthDate = e.Entity.BirthDate;
                break;

            case ChangeType.Delete:
                CheckValues2[ChangeType.Delete].Name = e.Entity.Name;
                CheckValues2[ChangeType.Delete].BirthDate = e.Entity.BirthDate;
                break;
        }
    }

    private async Task ModifyTableContent1()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName1}] ([Name], [BirthDate]) VALUES ('Christian', '2009-08-05')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName1}] SET [Name] = 'Valentina', [BirthDate] = '2009-05-08'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName1}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task ModifyTableContent2()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName2}] ([Name], [BirthDate]) VALUES ('Christian', '2009-08-05')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName2}] SET [Name] = 'Valentina', [BirthDate] = '2009-05-08'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName2}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}