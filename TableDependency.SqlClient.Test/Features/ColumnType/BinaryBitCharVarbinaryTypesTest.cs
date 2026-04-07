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
using System.Data;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class BinaryBitCharVarbinaryTypesModel
{
    public byte[] Binary50Column { get; set; } = null!;
    public bool? BitColumn { get; set; }
    public bool Bit2Column { get; set; }
    public bool Bit3Column { get; set; }
    public char[] Char10Column { get; set; } = null!;
    public byte[] Varbinary50Column { get; set; } = null!;
    public byte[] VarbinaryMaxColumn { get; set; } = null!;
}

public class BinaryBitCharVarbinaryTypesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = "Test";
    private readonly Dictionary<ChangeType, (BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (BinaryBitCharVarbinaryTypesModel, BinaryBitCharVarbinaryTypesModel)> _checkValuesOld = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE {TableName} (" +
            "binary50Column binary(50) NULL, " +
            "bitColumn bit NULL, bit2Column BIT, bit3Column BIT," +
            "char10Column char(10) NULL, " +
            "varbinary50Column varbinary(50) NULL, " +
            "varbinaryMAXColumn varbinary(MAX) NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        _checkValues.Clear();
        _checkValuesOld.Clear();

        _checkValues.Add(ChangeType.Insert, (new() { Binary50Column = GetBytes("Aurelia", 50), Bit2Column = false, Bit3Column = false, BitColumn = false, Char10Column = null!, Varbinary50Column = GetBytes("Nonna"), VarbinaryMaxColumn = null! }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = ['A'], Varbinary50Column = null!, VarbinaryMaxColumn = GetBytes("Velia") }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = ['A'], Varbinary50Column = null!, VarbinaryMaxColumn = GetBytes("Velia") }, new()));

        _checkValuesOld.Add(ChangeType.Insert, (new() { Binary50Column = GetBytes("Aurelia", 50), Bit2Column = false, Bit3Column = false, BitColumn = false, Char10Column = null!, Varbinary50Column = GetBytes("Nonna"), VarbinaryMaxColumn = null! }, new()));
        _checkValuesOld.Add(ChangeType.Update, (new() { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = ['A'], Varbinary50Column = null!, VarbinaryMaxColumn = GetBytes("Velia") }, new()));
        _checkValuesOld.Add(ChangeType.Delete, (new() { Binary50Column = GetBytes("Valentina", 50), Bit2Column = true, Bit3Column = true, BitColumn = true, Char10Column = ['A'], Varbinary50Column = null!, VarbinaryMaxColumn = GetBytes("Velia") }, new()));
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
        SqlTableDependency<BinaryBitCharVarbinaryTypesModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<BinaryBitCharVarbinaryTypesModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken);
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

        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.Binary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BitColumn, _checkValues[ChangeType.Insert].Item2.BitColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Char10Column, _checkValues[ChangeType.Insert].Item2.Char10Column);
        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert].Item2.VarbinaryMaxColumn));

        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.Binary50Column), GetString(_checkValues[ChangeType.Update].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Update].Item1.BitColumn, _checkValues[ChangeType.Update].Item2.BitColumn);
        Assert.Equal(new string(_checkValues[ChangeType.Update].Item1.Char10Column).Trim(), new string(_checkValues[ChangeType.Update].Item2.Char10Column).Trim());
        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Update].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Update].Item2.VarbinaryMaxColumn));

        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.Binary50Column), GetString(_checkValues[ChangeType.Delete].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.BitColumn, _checkValues[ChangeType.Delete].Item2.BitColumn);
        Assert.Equal(new string(_checkValues[ChangeType.Delete].Item1.Char10Column).Trim(), new string(_checkValues[ChangeType.Delete].Item2.Char10Column).Trim());
        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Delete].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Delete].Item2.VarbinaryMaxColumn));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestWithOldEntity()
    {
        SqlTableDependency<BinaryBitCharVarbinaryTypesModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<BinaryBitCharVarbinaryTypesModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                includeOldEntity: true,
                ct: TestContext.Current.CancellationToken);

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

        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.Binary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.BitColumn, _checkValues[ChangeType.Insert].Item2.BitColumn);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Char10Column, _checkValues[ChangeType.Insert].Item2.Char10Column);
        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Insert].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert].Item2.VarbinaryMaxColumn));

        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Insert));

        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.Binary50Column), GetString(_checkValues[ChangeType.Update].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Update].Item1.BitColumn, _checkValues[ChangeType.Update].Item2.BitColumn);
        Assert.Equal(new string(_checkValues[ChangeType.Update].Item1.Char10Column).Trim(), new string(_checkValues[ChangeType.Update].Item2.Char10Column).Trim());
        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Update].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Update].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Update].Item2.VarbinaryMaxColumn));

        Assert.Equal(GetString(_checkValuesOld[ChangeType.Update].Item2.Binary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Binary50Column));
        Assert.Equal(_checkValuesOld[ChangeType.Update].Item2.BitColumn, _checkValues[ChangeType.Insert].Item2.BitColumn);
        Assert.Equal(new string(_checkValuesOld[ChangeType.Update].Item2.Char10Column).Trim(), new string(_checkValues[ChangeType.Insert].Item2.Char10Column).Trim());
        Assert.Equal(GetString(_checkValuesOld[ChangeType.Update].Item2.Varbinary50Column), GetString(_checkValues[ChangeType.Insert].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValuesOld[ChangeType.Update].Item2.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Insert].Item2.VarbinaryMaxColumn));

        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.Binary50Column), GetString(_checkValues[ChangeType.Delete].Item2.Binary50Column));
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.BitColumn, _checkValues[ChangeType.Delete].Item2.BitColumn);
        Assert.Equal(new string(_checkValues[ChangeType.Delete].Item1.Char10Column).Trim(), new string(_checkValues[ChangeType.Delete].Item2.Char10Column).Trim());
        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.Varbinary50Column), GetString(_checkValues[ChangeType.Delete].Item2.Varbinary50Column));
        Assert.Equal(GetString(_checkValues[ChangeType.Delete].Item1.VarbinaryMaxColumn), GetString(_checkValues[ChangeType.Delete].Item2.VarbinaryMaxColumn));

        Assert.False(_checkValuesOld.ContainsKey(ChangeType.Delete));

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<BinaryBitCharVarbinaryTypesModel> e)
    {
        _checkValues[e.ChangeType].Item2.BitColumn = e.Entity.BitColumn;
        _checkValues[e.ChangeType].Item2.Bit2Column = e.Entity.Bit2Column;
        _checkValues[e.ChangeType].Item2.Bit3Column = e.Entity.Bit3Column;
        _checkValues[e.ChangeType].Item2.Binary50Column = e.Entity.Binary50Column;
        _checkValues[e.ChangeType].Item2.Char10Column = e.Entity.Char10Column;
        _checkValues[e.ChangeType].Item2.Varbinary50Column = e.Entity.Varbinary50Column;
        _checkValues[e.ChangeType].Item2.VarbinaryMaxColumn = e.Entity.VarbinaryMaxColumn;

        if (e.OldEntity is not null)
        {
            _checkValuesOld[e.ChangeType].Item2.BitColumn = e.OldEntity.BitColumn;
            _checkValuesOld[e.ChangeType].Item2.Bit2Column = e.OldEntity.Bit2Column;
            _checkValuesOld[e.ChangeType].Item2.Bit3Column = e.OldEntity.Bit3Column;
            _checkValuesOld[e.ChangeType].Item2.Binary50Column = e.OldEntity.Binary50Column;
            _checkValuesOld[e.ChangeType].Item2.Char10Column = e.OldEntity.Char10Column;
            _checkValuesOld[e.ChangeType].Item2.Varbinary50Column = e.OldEntity.Varbinary50Column;
            _checkValuesOld[e.ChangeType].Item2.VarbinaryMaxColumn = e.OldEntity.VarbinaryMaxColumn;
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
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([binary50Column], [bitColumn], [bit2Column], [bit3Column], [char10Column], varbinary50Column, varbinaryMAXColumn) VALUES (@binary50Column, @bitColumn, 0, 0, null, @varbinary50Column, null)";
        sqlCommand.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Size = 50, Value = _checkValues[ChangeType.Insert].Item1.Binary50Column });
        sqlCommand.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Insert].Item1.BitColumn.GetValueOrDefault() });
        sqlCommand.Parameters.Add(new SqlParameter("@varbinary50Column", SqlDbType.VarBinary) { Size = 50, Value = _checkValues[ChangeType.Insert].Item1.Varbinary50Column });
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand2 = sqlConnection.CreateCommand();
        sqlCommand2.CommandText = $"UPDATE [{TableName}] SET [binary50Column] = @binary50Column, [bitColumn] = @bitColumn, [bit2Column] = 1, [bit3Column] = 1 ,[char10Column] = @char10Column, varbinary50Column = null, varbinaryMAXColumn = @varbinaryMAXColumn";
        sqlCommand2.Parameters.Add(new SqlParameter("@binary50Column", SqlDbType.Binary) { Value = _checkValues[ChangeType.Update].Item1.Binary50Column });
        sqlCommand2.Parameters.Add(new SqlParameter("@bitColumn", SqlDbType.Bit) { Value = _checkValues[ChangeType.Update].Item1.BitColumn.GetValueOrDefault() });
        sqlCommand2.Parameters.Add(new SqlParameter("@char10Column", SqlDbType.Char) { Size = 10, Value = _checkValues[ChangeType.Update].Item1.Char10Column });
        sqlCommand2.Parameters.Add(new SqlParameter("@varbinaryMAXColumn", SqlDbType.VarBinary) { Value = _checkValues[ChangeType.Update].Item1.VarbinaryMaxColumn });
        await sqlCommand2.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand3 = sqlConnection.CreateCommand();
        sqlCommand3.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand3.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}