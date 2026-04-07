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

public class Issue55Test(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class Issue55Model
    {
        public decimal PaymentDiscount { get; set; }
        public int AllowQuantity { get; set; }
        public string DocNo { get; set; } = string.Empty;
    }

    private const string TableName = "BranchABCSales Invoice Header";
    private int _counter;
    private readonly Dictionary<ChangeType, (Issue55Model, Issue55Model)> _checkValues = [];
    private readonly Dictionary<ChangeType, (Issue55Model, Issue55Model)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('[{TableName}]', 'U') IS NOT NULL DROP TABLE [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Payment Discount %] decimal(18, 2), [Allow Quantity Disc_] int, [Applies-to Doc_ No_] [VARCHAR](100) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Add(ChangeType.Insert, (new() { DocNo = "Christian", AllowQuantity = 1, PaymentDiscount = 9 }, new()));
        _checkValues.Add(ChangeType.Update, (new() { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { DocNo = "Christian", AllowQuantity = 1, PaymentDiscount = 9 }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { DocNo = "Velia", AllowQuantity = 2, PaymentDiscount = 3 }, new()));
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
        var mapper = new ModelToTableMapper<Issue55Model>();
        mapper.AddMapping(c => c.PaymentDiscount, "Payment Discount %");
        mapper.AddMapping(c => c.AllowQuantity, "Allow Quantity Disc_");
        mapper.AddMapping(c => c.DocNo, "Applies-to Doc_ No_");

        SqlTableDependency<Issue55Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue55Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                includeOldEntity: false, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.DocNo, _checkValues[ChangeType.Insert].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.AllowQuantity, _checkValues[ChangeType.Insert].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.PaymentDiscount, _checkValues[ChangeType.Insert].Item2.PaymentDiscount);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.DocNo, _checkValues[ChangeType.Update].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.AllowQuantity, _checkValues[ChangeType.Update].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.PaymentDiscount, _checkValues[ChangeType.Update].Item2.PaymentDiscount);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Update));

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.DocNo, _checkValues[ChangeType.Delete].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.AllowQuantity, _checkValues[ChangeType.Delete].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.PaymentDiscount, _checkValues[ChangeType.Delete].Item2.PaymentDiscount);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        var mapper = new ModelToTableMapper<Issue55Model>();
        mapper.AddMapping(c => c.PaymentDiscount, "Payment Discount %");
        mapper.AddMapping(c => c.AllowQuantity, "Allow Quantity Disc_");
        mapper.AddMapping(c => c.DocNo, "Applies-to Doc_ No_");

        SqlTableDependency<Issue55Model>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<Issue55Model>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                includeOldEntity: true, ct: TestContext.Current.CancellationToken);
            tableDependency.OnChanged += TableDependency_Changed;
            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            naming = tableDependency.NamingPrefix;

            await ModifyTableContent();
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _counter);

        Assert.Equal(_checkValues[ChangeType.Insert].Item1.DocNo, _checkValues[ChangeType.Insert].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.AllowQuantity, _checkValues[ChangeType.Insert].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.PaymentDiscount, _checkValues[ChangeType.Insert].Item2.PaymentDiscount);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(_checkValues[ChangeType.Update].Item1.DocNo, _checkValues[ChangeType.Update].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.AllowQuantity, _checkValues[ChangeType.Update].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.PaymentDiscount, _checkValues[ChangeType.Update].Item2.PaymentDiscount);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.DocNo, _checkValues[ChangeType.Insert].Item2.DocNo);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.AllowQuantity, _checkValues[ChangeType.Insert].Item2.AllowQuantity);
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.PaymentDiscount, _checkValues[ChangeType.Insert].Item2.PaymentDiscount);

        Assert.Equal(_checkValues[ChangeType.Delete].Item1.DocNo, _checkValues[ChangeType.Delete].Item2.DocNo);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.AllowQuantity, _checkValues[ChangeType.Delete].Item2.AllowQuantity);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.PaymentDiscount, _checkValues[ChangeType.Delete].Item2.PaymentDiscount);
        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<Issue55Model> e)
    {
        _counter++;
        _checkValues[e.ChangeType].Item2.DocNo = e.Entity.DocNo;
        _checkValues[e.ChangeType].Item2.AllowQuantity = e.Entity.AllowQuantity;
        _checkValues[e.ChangeType].Item2.PaymentDiscount = e.Entity.PaymentDiscount;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.DocNo = e.OldEntity.DocNo;
            _checkValuesOld[e.ChangeType].Item2.AllowQuantity = e.OldEntity.AllowQuantity;
            _checkValuesOld[e.ChangeType].Item2.PaymentDiscount = e.OldEntity.PaymentDiscount;
        }
        else
        {
            _checkValuesOld.Remove(e.ChangeType);
        }
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Payment Discount %], [Allow Quantity Disc_], [Applies-to Doc_ No_]) VALUES ({_checkValues[ChangeType.Insert].Item1.PaymentDiscount}, {_checkValues[ChangeType.Insert].Item1.AllowQuantity}, '{_checkValues[ChangeType.Insert].Item1.DocNo}')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Payment Discount %] = {_checkValues[ChangeType.Update].Item1.PaymentDiscount}, [Allow Quantity Disc_] = {_checkValues[ChangeType.Update].Item1.AllowQuantity}, [Applies-to Doc_ No_] = '{_checkValues[ChangeType.Update].Item1.DocNo}'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}