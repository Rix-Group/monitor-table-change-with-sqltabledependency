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

public class DataAnnotationNotMappedTest2(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Table("DataAnnotationNotMappedTest2Model")]
    private class DataAnnotationNotMappedTest2Model
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;

        [NotMapped]
        public string ComposedName { get => Id + "-" + Name; set => Name = value; }
    }

    private const string SchemaName = "[dbo]";
    private int _counter;
    private readonly Dictionary<ChangeType, (DataAnnotationNotMappedTest2Model, DataAnnotationNotMappedTest2Model)> _checkValuesTest2 = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest2Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest2Model];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = "CREATE TABLE [DataAnnotationNotMappedTest2Model]([Id] [int] NOT NULL, [Name] [NVARCHAR](50) NULL, [Long Description] [NVARCHAR](255) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValuesTest2.Add(ChangeType.Insert, (new() { Id = 1, Name = "Christian", Description = "Del Bianco" }, new()));
        _checkValuesTest2.Add(ChangeType.Update, (new() { Id = 3, Name = "Velia", Description = "Ceccarelli" }, new()));
        _checkValuesTest2.Add(ChangeType.Delete, (new() { Id = 3, Name = "Velia", Description = "Ceccarelli" }, new()));
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "IF OBJECT_ID('DataAnnotationNotMappedTest2Model', 'U') IS NOT NULL DROP TABLE [DataAnnotationNotMappedTest2Model];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test2()
    {
        SqlTableDependency<DataAnnotationNotMappedTest2Model>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<DataAnnotationNotMappedTest2Model>();
            mapper.AddMapping(c => c.Description, "Long Description");

            tableDependency = await SqlTableDependency<DataAnnotationNotMappedTest2Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                schemaName: SchemaName,
                mapper: mapper,
                includeOldEntity: false,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += TableDependency_Changed_Test2;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContentTest2();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValuesTest2[ChangeType.Insert].Item1.Id, _checkValuesTest2[ChangeType.Insert].Item2.Id);
        Assert.Equal(_checkValuesTest2[ChangeType.Insert].Item1.Name, _checkValuesTest2[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValuesTest2[ChangeType.Insert].Item1.Description, _checkValuesTest2[ChangeType.Insert].Item2.Description);
        Assert.Equal(_checkValuesTest2[ChangeType.Insert].Item1.ComposedName, _checkValuesTest2[ChangeType.Insert].Item2.ComposedName);

        Assert.Equal(_checkValuesTest2[ChangeType.Update].Item1.Id, _checkValuesTest2[ChangeType.Update].Item2.Id);
        Assert.Equal(_checkValuesTest2[ChangeType.Update].Item1.Name, _checkValuesTest2[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValuesTest2[ChangeType.Update].Item1.Description, _checkValuesTest2[ChangeType.Update].Item2.Description);
        Assert.Equal(_checkValuesTest2[ChangeType.Update].Item1.ComposedName, _checkValuesTest2[ChangeType.Update].Item2.ComposedName);

        Assert.Equal(_checkValuesTest2[ChangeType.Delete].Item1.Id, _checkValuesTest2[ChangeType.Delete].Item2.Id);
        Assert.Equal(_checkValuesTest2[ChangeType.Delete].Item1.Name, _checkValuesTest2[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValuesTest2[ChangeType.Delete].Item1.Description, _checkValuesTest2[ChangeType.Delete].Item2.Description);
        Assert.Equal(_checkValuesTest2[ChangeType.Delete].Item1.ComposedName, _checkValuesTest2[ChangeType.Delete].Item2.ComposedName);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed_Test2(RecordChangedEventArgs<DataAnnotationNotMappedTest2Model> e)
    {
        _counter++;

        _checkValuesTest2[e.ChangeType].Item2.Id = e.Entity.Id;
        _checkValuesTest2[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValuesTest2[e.ChangeType].Item2.Description = e.Entity.Description;
    }

    private async Task ModifyTableContentTest2()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [DataAnnotationNotMappedTest2Model] ([Id], [Name], [Long Description]) VALUES ({_checkValuesTest2[ChangeType.Insert].Item1.Id}, '{_checkValuesTest2[ChangeType.Insert].Item1.Name}', '{_checkValuesTest2[ChangeType.Insert].Item1.Description}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [DataAnnotationNotMappedTest2Model] SET [Id] = {_checkValuesTest2[ChangeType.Update].Item1.Id}, [Name] = '{_checkValuesTest2[ChangeType.Update].Item1.Name}', [Long Description] = '{_checkValuesTest2[ChangeType.Update].Item1.Description}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = "DELETE FROM [DataAnnotationNotMappedTest2Model]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}