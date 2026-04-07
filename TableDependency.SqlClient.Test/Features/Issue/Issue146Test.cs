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

namespace TableDependency.SqlClient.Test.Features.Issue;

public class Issue146Test(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public class DomainObject
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
    }

    // Partial and inherited on purpose based on issue 146
    public partial class Bank : DomainObject
    {
        public string Description { get; set; } = null!;
    }

    private const string TableName = "Bank";
    private readonly Dictionary<ChangeType, List<Bank>> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [dbo].[{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([IdBank] [INT] NOT NULL PRIMARY KEY, [BankName] NVARCHAR(50), [BankDescription] NVARCHAR(100))";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, []);
        _checkValues.Add(ChangeType.Update, []);
        _checkValues.Add(ChangeType.Delete, []);
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
    public async Task Test1()
    {
        SqlTableDependency<Bank>? tableDependency = null;
        string naming;

        try
        {
            var mapper = new ModelToTableMapper<Bank>();
            mapper.AddMapping(c => c.Id, "IdBank"); // dbo,Bank.IdBank (PK, int, not null)
            mapper.AddMapping(c => c.Name, "BankName");
            mapper.AddMapping(c => c.Description, "BankDescription");

            tableDependency = await SqlTableDependency<Bank>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, mapper: mapper, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed1;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent1();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(1, _checkValues[ChangeType.Insert][0].Id);
        Assert.Equal("UBS bank", _checkValues[ChangeType.Insert][0].Name);
        Assert.Equal("THE UBS bank", _checkValues[ChangeType.Insert][0].Description);

        Assert.Equal(2, _checkValues[ChangeType.Update][0].Id);
        Assert.Equal("Credit Swiss Bank", _checkValues[ChangeType.Update][0].Name);
        Assert.Equal("THE Credit Swiss Bank", _checkValues[ChangeType.Update][0].Description);

        Assert.Equal(2, _checkValues[ChangeType.Delete][0].Id);
        Assert.Equal("Credit Swiss Bank", _checkValues[ChangeType.Delete][0].Name);
        Assert.Equal("THE Credit Swiss Bank", _checkValues[ChangeType.Delete][0].Description);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed1(RecordChangedEventArgs<Bank> e)
        => _checkValues[e.ChangeType].Add(e.Entity);

    private async Task ModifyTableContent1()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([IdBank], [BankName], [BankDescription]) VALUES(1, 'UBS bank', 'THE UBS bank')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [IdBank] = 2, [BankName] = 'Credit Swiss Bank', [BankDescription] = 'THE Credit Swiss Bank' WHERE [IdBank] = 1";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}] WHERE [IdBank] = 2";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}