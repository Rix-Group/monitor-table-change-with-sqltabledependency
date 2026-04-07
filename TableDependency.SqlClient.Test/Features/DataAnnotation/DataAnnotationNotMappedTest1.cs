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
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.DataAnnotation;

public class DataAnnotationNotMappedTest1(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Table("DataAnnotationNotMappedTest1Model")]
    private class DataAnnotationNotMappedTest1Model
    {
        [NotMapped]
        public int Number { get => int.Parse(StringNumberInDatabase); set => StringNumberInDatabase = value.ToString(); }

        [Column("Number")]
        public string StringNumberInDatabase { get; set; } = string.Empty;
    }

    private const string SchemaName = "[dbo]";
    private int _counter;
    private readonly Dictionary<ChangeType, (DataAnnotationNotMappedTest1Model, DataAnnotationNotMappedTest1Model)> _checkValuesTest1 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest1Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest1Model];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = "CREATE TABLE [DataAnnotationNotMappedTest1Model]([Number] [NVARCHAR](50) NOT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValuesTest1.Add(ChangeType.Insert, (new() { StringNumberInDatabase = "100" }, new()));
        _checkValuesTest1.Add(ChangeType.Update, (new() { StringNumberInDatabase = "990" }, new()));
        _checkValuesTest1.Add(ChangeType.Delete, (new() { StringNumberInDatabase = "990" }, new()));
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest1Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest1Model];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test1()
    {
        SqlTableDependency<DataAnnotationNotMappedTest1Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<DataAnnotationNotMappedTest1Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                schemaName: SchemaName,
                includeOldEntity: false,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += TableDependency_Changed_Test1;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContentTest1Async();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValuesTest1[ChangeType.Insert].Item1.StringNumberInDatabase, _checkValuesTest1[ChangeType.Insert].Item2.StringNumberInDatabase);
        Assert.Equal(int.Parse(_checkValuesTest1[ChangeType.Insert].Item1.StringNumberInDatabase), _checkValuesTest1[ChangeType.Insert].Item2.Number);

        Assert.Equal(_checkValuesTest1[ChangeType.Update].Item1.StringNumberInDatabase, _checkValuesTest1[ChangeType.Update].Item2.StringNumberInDatabase);
        Assert.Equal(int.Parse(_checkValuesTest1[ChangeType.Update].Item1.StringNumberInDatabase), _checkValuesTest1[ChangeType.Update].Item2.Number);

        Assert.Equal(_checkValuesTest1[ChangeType.Delete].Item1.StringNumberInDatabase, _checkValuesTest1[ChangeType.Delete].Item2.StringNumberInDatabase);
        Assert.Equal(int.Parse(_checkValuesTest1[ChangeType.Update].Item1.StringNumberInDatabase), _checkValuesTest1[ChangeType.Delete].Item2.Number);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed_Test1(RecordChangedEventArgs<DataAnnotationNotMappedTest1Model> e)
    {
        _counter++;
        _checkValuesTest1[e.ChangeType].Item2.StringNumberInDatabase = e.Entity.StringNumberInDatabase;
    }

    private async Task ModifyTableContentTest1Async()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [DataAnnotationNotMappedTest1Model] ([Number]) VALUES ('{_checkValuesTest1[ChangeType.Insert].Item1.StringNumberInDatabase}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [DataAnnotationNotMappedTest1Model] SET [Number] = '{_checkValuesTest1[ChangeType.Update].Item1.StringNumberInDatabase}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest1Model]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}