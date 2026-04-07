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
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Exceptions;

namespace TableDependency.SqlClient.Test.Features.Misc;

public class PreliminaryTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class PreliminaryTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    private static readonly string TableName = typeof(PreliminaryTestSqlServerModel).Name;
    private const string InvalidValidConnectionString = "data source=.;initial catalog=NotExistingDB;integrated security=True";
    private const string InvalidTableName = "NotExistingTable";

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( "
            + "[Id][int] IDENTITY(1, 1) NOT NULL, "
            + "[First Name] [nvarchar](50) NOT NULL, "
            + "[Second Name] [nvarchar](50) NOT NULL, "
            + "[Born] [datetime] NULL)";
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
    public async Task InvalidConnectionStringTest()
    {
        await Assert.ThrowsAsync<ImpossibleOpenSqlConnectionException>(
            () => SqlTableDependency<PreliminaryTestSqlServerModel>.CreateSqlTableDependencyAsync(InvalidValidConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InvalidTableNameTest()
    {
        await Assert.ThrowsAsync<NotExistingTableException>(
            () => SqlTableDependency<PreliminaryTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: InvalidTableName, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public void MapperWithNullTest()
    {
        var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();

        var ex = Assert.Throws<ModelToTableMapperException>(() => mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, null!));
        Assert.Equal("ModelToTableMapper cannot contains null or empty strings.", ex.Message);
    }

    [Fact]
    public void MappertWithEmptyTest()
    {
        var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();

        var ex = Assert.Throws<ModelToTableMapperException>(() => mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, string.Empty));
        Assert.Equal("ModelToTableMapper cannot contains null or empty strings.", ex.Message);
    }

    [Fact]
    public async Task InvalidMappertTest()
    {
        var mapper = new ModelToTableMapper<PreliminaryTestSqlServerModel>();
        mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Not Exist");

        await Assert.ThrowsAsync<ModelToTableMapperException>(
            () => SqlTableDependency<PreliminaryTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EmptyUpdateOfModelListTest()
    {
        await Assert.ThrowsAsync<UpdateOfException>(
            () => SqlTableDependency<PreliminaryTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, updateOf: new UpdateOfModel<PreliminaryTestSqlServerModel>(), ct: TestContext.Current.CancellationToken));
    }
}