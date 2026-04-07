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
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Operations;

public class MergeTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class MergeTestSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    private MergeTestSqlServerModel? _modifiedValues;
    private MergeTestSqlServerModel? _insertedValues;
    private MergeTestSqlServerModel? _deletedValues;

    private const string TargetTableName = "energydata";
    private const string SourceTableName = "temp_energydata";

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TargetTableName}', 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TargetTableName} (Id INT, Name NVARCHAR(100), quantity INT);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {SourceTableName} (Id INT, Name NVARCHAR(100), quantity INT);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = "IF EXISTS (SELECT * FROM sys.objects WHERE name = N'testMerge') DROP PROCEDURE[testMerge]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            "CREATE PROCEDURE dbo.testMerge AS " + Environment.NewLine +
            "BEGIN " + Environment.NewLine +
            "  SET NOCOUNT ON; " + Environment.NewLine +
            $"  MERGE INTO {TargetTableName} AS target " + Environment.NewLine +
            $"  USING {SourceTableName} AS source ON target.Id = source.Id " + Environment.NewLine +
            "  WHEN MATCHED THEN UPDATE SET target.quantity = source.quantity " + Environment.NewLine +
            "  WHEN NOT MATCHED BY TARGET THEN INSERT(Id, Name, quantity) VALUES(source.Id, source.Name, source.quantity) " + Environment.NewLine +
            "  WHEN NOT MATCHED BY SOURCE THEN DELETE; " + Environment.NewLine +
            "END;";

        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, quantity) values (0, 'DELETE', 0);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"insert into {TargetTableName} (id, name, quantity) values (1, 'UPDATE', 0);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, quantity) values (2, 'INSERT', 100);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"insert into {SourceTableName} (id, name, quantity) values (1, 'UPDATE', 200);";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "IF EXISTS (SELECT * FROM sys.objects WHERE name = N'testMerge') DROP PROCEDURE[testMerge]";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{TargetTableName}', 'U') IS NOT NULL DROP TABLE [{TargetTableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);

        sqlCommand.CommandText = $"IF OBJECT_ID('{SourceTableName}', 'U') IS NOT NULL DROP TABLE [{SourceTableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Test()
    {
        SqlTableDependency<MergeTestSqlServerModel>? tableDependency = null;

        try
        {
            tableDependency = await SqlTableDependency<MergeTestSqlServerModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TargetTableName, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            tableDependency.OnException += TableDependency_OnException;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await MergeOperation();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(100, _insertedValues?.Quantity);
        Assert.Equal(200, _modifiedValues?.Quantity);
        Assert.Equal(0, _deletedValues?.Quantity);
    }

    private static void TableDependency_OnException(ExceptionEventArgs e)
        => Assert.Fail(e.Exception?.Message);

    private void TableDependency_Changed(RecordChangedEventArgs<MergeTestSqlServerModel> e)
    {
        switch (e.ChangeType)
        {
            case ChangeType.Insert:
                _insertedValues = new() { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                break;

            case ChangeType.Update:
                _modifiedValues = new() { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                break;

            case ChangeType.Delete:
                _deletedValues = new() { Id = e.Entity.Id, Name = e.Entity.Name, Quantity = e.Entity.Quantity };
                break;
        }
    }

    private async Task MergeOperation()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandType = System.Data.CommandType.StoredProcedure;
        sqlCommand.CommandText = "testMerge";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}