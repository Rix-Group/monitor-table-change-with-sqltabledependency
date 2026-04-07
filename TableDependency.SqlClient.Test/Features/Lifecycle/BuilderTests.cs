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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class SqlTableDependencyBuilderModel
{
    public string Name { get; set; } = string.Empty;
}

public class ModelBuilder : SqlTableDependencyBuilder<SqlTableDependencyBuilderModel>
{
    public ModelBuilder(string connectionString) : base(connectionString)
    {
        SchemaName = "dbo";
        TableName = nameof(SqlTableDependencyBuilderModel);

        Mapper = new ModelToTableMapper<SqlTableDependencyBuilderModel>();
        Mapper.AddMapping(c => c.Name, "Name");

        UpdateOf = new UpdateOfModel<SqlTableDependencyBuilderModel>();
        UpdateOf.Add(i => i.Name);

        Assert.Equal(NotifyOn.All, NotifyOn);
        Filter = new SqlTableDependencyFilter<SqlTableDependencyBuilderModel>(m => m.Name == "test");
        IncludeOldEntity = true;
        PersistentId = "123";
    }
}

public class BuilderTests(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string SchemaName = "dbo";
    private const string TableName = nameof(SqlTableDependencyBuilderModel);

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{SchemaName}.{TableName}', 'U') IS NOT NULL DROP TABLE [{SchemaName}].[{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{SchemaName}].[{TableName}] ([Name] [nvarchar](50) NOT NULL);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{SchemaName}.{TableName}', 'U') IS NOT NULL DROP TABLE [{SchemaName}].[{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(ConnectionString);
        services.AddSingleton<ITableDependencyBuilder<SqlTableDependencyBuilderModel>, ModelBuilder>();

        await using var provider = services.BuildServiceProvider();

        var builder = provider.GetRequiredService<ITableDependencyBuilder<SqlTableDependencyBuilderModel>>();
        var logger = provider.GetRequiredService<ILogger<BuilderTests>>();

        ITableDependency<SqlTableDependencyBuilderModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await builder.BuildAsync(logger, TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            Assert.NotNull(tableDependency);
            Assert.Equal(SchemaName, tableDependency.SchemaName);
            Assert.Equal(TableName, tableDependency.TableName);

            tableDependency.OnChanged += _ => { };
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
            {
                await tableDependency.DisposeAsync();
                await tableDependency.DropDatabaseObjectsAsync(); // Needed as persistent
            }
        }

        // Persistent
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }
}