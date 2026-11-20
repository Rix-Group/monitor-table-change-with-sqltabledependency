using Microsoft.Data.SqlClient;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class MoneyAndFloatPrecisionModel
{
    public int Id { get; set; }
    public decimal MoneyColumn { get; set; }
    public decimal SmallmoneyColumn { get; set; }
    public double FloatColumn { get; set; }
    public double TinyFloatColumn { get; set; }
    public float RealColumn { get; set; }
}

// money's default CONVERT style keeps two decimals and STR(float, 53, 16) fixed sixteen, so a change differed from the row it reported.
public class MoneyAndFloatPrecisionTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(MoneyAndFloatPrecisionModel).Name;

    private static readonly MoneyAndFloatPrecisionModel Written = new()
    {
        Id = 1,
        MoneyColumn = 1.2345M,
        SmallmoneyColumn = 2.3456M,
        FloatColumn = 1.2345678901234567,
        TinyFloatColumn = 1.25E-20,
        RealColumn = 1.2345678F
    };

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]; " +
            $"CREATE TABLE [{TableName}](Id int PRIMARY KEY, MoneyColumn money, SmallmoneyColumn smallmoney, FloatColumn float, TinyFloatColumn float, RealColumn real);";
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Change_MoneyAndFloatColumns_ArriveWithEveryDigit(bool includeOldEntity)
    {
        // ARRANGE
        var inserted = new TaskCompletionSource<MoneyAndFloatPrecisionModel>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deleted = new TaskCompletionSource<MoneyAndFloatPrecisionModel>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var tableDependency = await SqlTableDependency<MoneyAndFloatPrecisionModel>.CreateSqlTableDependencyAsync(DependencyConnectionString, tableName: TableName, includeOldEntity: includeOldEntity, ct: TestContext.Current.CancellationToken);
        tableDependency.OnChanged += (RecordChangedEventArgs<MoneyAndFloatPrecisionModel> e) =>
        {
            if (e.ChangeType is ChangeType.Insert)
                inserted.TrySetResult(e.Entity);
            else if (e.ChangeType is ChangeType.Delete)
                deleted.TrySetResult(e.Entity);
        };
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

        // ACT
        await using (var sqlConnection = new SqlConnection(ConnectionString))
        {
            await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] VALUES (@id, @money, @smallmoney, @float, @tiny, @real); DELETE FROM [{TableName}];";
            sqlCommand.Parameters.AddWithValue("@id", Written.Id);
            sqlCommand.Parameters.AddWithValue("@money", Written.MoneyColumn);
            sqlCommand.Parameters.AddWithValue("@smallmoney", Written.SmallmoneyColumn);
            sqlCommand.Parameters.AddWithValue("@float", Written.FloatColumn);
            sqlCommand.Parameters.AddWithValue("@tiny", Written.TinyFloatColumn);
            sqlCommand.Parameters.AddWithValue("@real", Written.RealColumn);
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var insert = await inserted.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        var delete = await deleted.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

        // ASSERT
        foreach (var received in new[] { insert, delete })
        {
            Assert.Equal(Written.MoneyColumn, received.MoneyColumn);
            Assert.Equal(Written.SmallmoneyColumn, received.SmallmoneyColumn);
            Assert.Equal(Written.FloatColumn, received.FloatColumn);
            Assert.Equal(Written.TinyFloatColumn, received.TinyFloatColumn);
            Assert.Equal(Written.RealColumn, received.RealColumn);
        }
    }
}