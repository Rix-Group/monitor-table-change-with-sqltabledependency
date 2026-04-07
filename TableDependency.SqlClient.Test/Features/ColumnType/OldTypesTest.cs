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
using Microsoft.SqlServer.Types;
using System.Data;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class OldTypeModel<TType>(TType value)
{
    public TType Value { get; set; } = value;
}

public class StringModel() : OldTypeModel<string>(string.Empty);

public class BinaryModel() : OldTypeModel<byte[]>([]);

public class OldTypesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    [Fact]
    public Task TextTest()
        => SharedTest<StringModel, string>(
            columnType: "TEXT",
            insertValue: new string('*', 6000),
            updateValue: "111");

    [Fact]
    public Task NTextTest()
        => SharedTest<StringModel, string>(
            columnType: "NTEXT",
            insertValue: new string('*', 8000),
            updateValue: "1111");

    [Fact]
    public Task HierarchyIdTest()
        => SharedTest<StringModel, string>(
            columnType: "HIERARCHYID",
            insertValue: "/1/",
            updateValue: "/1/2/");

    [Fact]
    public Task XmlTest()
        => SharedTest<StringModel, string>(
            columnType: "XML",
            insertValue: "<root>0</root>",
            updateValue: "<root>1</root>",
            insertParam: new SqlParameter("@Value", "<root>0</root>") { SqlDbType = SqlDbType.Xml },
            updateParam: new SqlParameter("@Value", "<root>1</root>") { SqlDbType = SqlDbType.Xml });

    [Fact]
    public Task GeographyTest()
        => SharedTest<StringModel, string>(
            columnType: "GEOGRAPHY",
            insertValue: "POINT (1 1)",
            updateValue: "POINT (0 0)",
            insertParam: new SqlParameter("@Value", SqlGeography.Point(1, 1, 4326)) { SqlDbType = SqlDbType.Udt, UdtTypeName = "Geography" },
            updateParam: new SqlParameter("@Value", SqlGeography.Point(0, 0, 4326)) { SqlDbType = SqlDbType.Udt, UdtTypeName = "Geography" });

    [Fact]
    public Task GeometryTest()
        => SharedTest<StringModel, string>(
            columnType: "GEOMETRY",
            insertValue: "POINT (1 1)",
            updateValue: "POINT (0 0)",
            insertParam: new SqlParameter("@Value", SqlGeography.Point(1, 1, 4326)) { SqlDbType = SqlDbType.Udt, UdtTypeName = "Geometry" },
            updateParam: new SqlParameter("@Value", SqlGeography.Point(0, 0, 4326)) { SqlDbType = SqlDbType.Udt, UdtTypeName = "Geometry" });

    [Fact]
    public Task ImageTest()
        => SharedTest<BinaryModel, byte[]>(
            columnType: "IMAGE",
            insertValue: GetBytes(new string('*', 6000)),
            updateValue: GetBytes("111"),
            insertParam: new SqlParameter("@Value", GetBytes(new string('*', 6000))) { SqlDbType = SqlDbType.Image },
            updateParam: new SqlParameter("@Value", GetBytes(new string("111"))) { SqlDbType = SqlDbType.Image });

    private async Task SharedTest<TModel, TType>(string columnType, TType insertValue, TType updateValue, SqlParameter? insertParam = null, SqlParameter? updateParam = null) where TModel : OldTypeModel<TType>, new()
    {
        var tableName = typeof(TModel).Name;

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        try
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE [{tableName}];";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            sqlCommand.CommandText = $"CREATE TABLE {tableName}(Id INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, [Value] {columnType} NULL)";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            await InnerTest<TModel, TType>(tableName, insertValue, updateValue, insertParam, updateParam);
        }
        finally
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE [{tableName}];";
            await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    private async Task InnerTest<TModel, TType>(string tableName, TType insertValue, TType updateValue, SqlParameter? insertParam, SqlParameter? updateParam) where TModel : OldTypeModel<TType>, new()
    {
        // ARRANGE
        Dictionary<ChangeType, TModel> checkValues = new()
        {
            { ChangeType.Insert, new() },
            { ChangeType.Update, new() },
            { ChangeType.Delete, new() },
        };
        Dictionary<ChangeType, TModel> checkOldValues = new()
        {
            { ChangeType.Update, new() }
        };

        void TableDependency_Changed(RecordChangedEventArgs<TModel> e)
        {
            checkValues[e.ChangeType].Value = e.Entity.Value;
            if (e.OldEntity is not null)
                checkOldValues[e.ChangeType].Value = e.OldEntity.Value;
        }

        async Task ModifyTableContent()
        {
            try
            {
                await using var sqlConnection = new SqlConnection(ConnectionString);
                await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

                await using var sqlCommand = sqlConnection.CreateCommand();
                sqlCommand.CommandText = $"INSERT INTO [{tableName}] ([Value]) VALUES(@Value)";
                insertParam ??= new SqlParameter("@Value", insertValue);
                insertParam.ParameterName = "@Value";
                sqlCommand.Parameters.Add(insertParam);
                await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

                await using var sqlCommand2 = sqlConnection.CreateCommand();
                sqlCommand2.CommandText = $"UPDATE [{tableName}] SET [Value] = @Value";
                updateParam ??= new SqlParameter("@Value", updateValue);
                updateParam.ParameterName = "@Value";
                sqlCommand2.Parameters.Add(updateParam);
                await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

                await using var sqlCommand3 = sqlConnection.CreateCommand();
                sqlCommand3.CommandText = $"DELETE FROM [{tableName}]";
                await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
            catch (Exception ex)
            {
                Assert.Fail(ex.Message);
            }
        }

        SqlTableDependency<TModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<TModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: tableName, includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            // ACT
            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        // ASSERT
        Assert.Equal(insertValue, checkValues[ChangeType.Insert].Value);
        Assert.Equal(updateValue, checkValues[ChangeType.Update].Value);
        Assert.Equal(insertValue, checkOldValues[ChangeType.Update].Value);
        Assert.Equal(updateValue, checkValues[ChangeType.Delete].Value);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }
}