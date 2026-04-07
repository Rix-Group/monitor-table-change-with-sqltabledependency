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

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class EnumTestSqlServer1(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private enum TypeEnum1 : byte
    {
        Genitore = 1,
        Figlio = 2
    }

    private class EnumTestSqlServerModel1
    {
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public TypeEnum1 Tipo { get; set; }
    }

    private static readonly string TableName = typeof(EnumTestSqlServerModel1).Name.ToUpper();
    private int _counter;
    private readonly Dictionary<ChangeType, (EnumTestSqlServerModel1, EnumTestSqlServerModel1)> _checkValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Tipo] [TINYINT] NULL, [Name] [NVARCHAR](50) NULL, [Surname] [NVARCHAR](50) NULL)";
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
    public async Task Test()
    {
        SqlTableDependency<EnumTestSqlServerModel1>? tableDependency = null;

        try
        {
            tableDependency = await SqlTableDependency<EnumTestSqlServerModel1>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Tipo, _checkValues[ChangeType.Insert].Item2.Tipo);

        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Surname, _checkValues[ChangeType.Update].Item2.Surname);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Tipo, _checkValues[ChangeType.Update].Item2.Tipo);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Tipo, _checkValues[ChangeType.Delete].Item2.Tipo);

        Assert.True(await AreAllDbObjectDisposedAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(tableDependency.NamingPrefix, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<EnumTestSqlServerModel1> e)
    {
        _counter++;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Surname = e.Entity.Surname;
        _checkValues[e.ChangeType].Item2.Tipo = e.Entity.Tipo;
    }

    private async Task ModifyTableContent()
    {
        _checkValues.Add(ChangeType.Insert, (new() { Tipo = TypeEnum1.Figlio, Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Tipo = TypeEnum1.Genitore, Name = "Velia", Surname = "Del Bianco" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Tipo = TypeEnum1.Genitore, Name = "Velia", Surname = "Del Bianco" }, new()));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Tipo], [Name], [Surname]) VALUES ({_checkValues[ChangeType.Insert].Item1.Tipo.GetHashCode()}, N'{_checkValues[ChangeType.Insert].Item1.Name}', N'{_checkValues[ChangeType.Insert].Item1.Surname}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = N'{_checkValues[ChangeType.Update].Item1.Name}', [Tipo] = {_checkValues[ChangeType.Update].Item1.Tipo.GetHashCode()}";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}