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

namespace TableDependency.SqlClient.Test.Features.Mapping;

internal class PrivateMembersIgnoredTestModel
{
    public string Name { get; set; } = string.Empty;
    public string Surname { get; set; } = string.Empty;

    private string PrivateProperty { get; set; } = PrivatePropertyDefault;
    private string PrivateField = PrivateFieldDefault;

    public string PrivatePropertyValue => PrivateProperty;
    public string PrivateFieldValue => PrivateField;

    public const string PrivatePropertyDefault = "property-initial";
    public const string PrivateFieldDefault = "field-initial";
}

public class PrivateMembersIgnoredTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private static readonly string TableName = typeof(PrivateMembersIgnoredTestModel).Name;
    private int _counter;
    private readonly Dictionary<ChangeType, (PrivateMembersIgnoredTestModel, PrivateMembersIgnoredTestModel)> _checkValues = [];
    private readonly Dictionary<ChangeType, (string SecretProperty, string SecretField)> _privateValues = [];

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText =
            $"CREATE TABLE [{TableName}]( " +
            "[Id][int] IDENTITY(1, 1) NOT NULL, " +
            "[Name] [NVARCHAR](50) NOT NULL, " +
            "[Surname] [NVARCHAR](50) NOT NULL, " +
            "[PrivateProperty] [NVARCHAR](50) NOT NULL, " +
            "[PrivateField] [NVARCHAR](50) NOT NULL)";
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
        SqlTableDependency<PrivateMembersIgnoredTestModel>? tableDependency = null;
        string naming;

        try
        {
            tableDependency = await SqlTableDependency<PrivateMembersIgnoredTestModel>.CreateSqlTableDependencyAsync(ConnectionString, tableName: TableName, ct: TestContext.Current.CancellationToken);
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
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Name, _checkValues[ChangeType.Insert].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Insert].Item1.Surname, _checkValues[ChangeType.Insert].Item2.Surname);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Name, _checkValues[ChangeType.Update].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Update].Item1.Surname, _checkValues[ChangeType.Update].Item2.Surname);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Name, _checkValues[ChangeType.Delete].Item2.Name);
        Assert.Equal(_checkValues[ChangeType.Delete].Item1.Surname, _checkValues[ChangeType.Delete].Item2.Surname);

        Assert.Equal(PrivateMembersIgnoredTestModel.PrivatePropertyDefault, _privateValues[ChangeType.Insert].SecretProperty);
        Assert.Equal(PrivateMembersIgnoredTestModel.PrivatePropertyDefault, _privateValues[ChangeType.Update].SecretProperty);
        Assert.Equal(PrivateMembersIgnoredTestModel.PrivatePropertyDefault, _privateValues[ChangeType.Delete].SecretProperty);
        Assert.Equal(PrivateMembersIgnoredTestModel.PrivateFieldDefault, _privateValues[ChangeType.Insert].SecretField);
        Assert.Equal(PrivateMembersIgnoredTestModel.PrivateFieldDefault, _privateValues[ChangeType.Update].SecretField);
        Assert.Equal(PrivateMembersIgnoredTestModel.PrivateFieldDefault, _privateValues[ChangeType.Delete].SecretField);

        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private void TableDependency_Changed(RecordChangedEventArgs<PrivateMembersIgnoredTestModel> e)
    {
        _counter++;
        _checkValues[e.ChangeType].Item2.Name = e.Entity.Name;
        _checkValues[e.ChangeType].Item2.Surname = e.Entity.Surname;
        _privateValues[e.ChangeType] = (e.Entity.PrivatePropertyValue, e.Entity.PrivateFieldValue);
    }

    private async Task ModifyTableContent()
    {
        _checkValues.Add(ChangeType.Insert, (new() { Name = "Christian", Surname = "Del Bianco" }, new()));
        _checkValues.Add(ChangeType.Update, (new() { Name = "Velia", Surname = "Ceccarelli" }, new()));
        _checkValues.Add(ChangeType.Delete, (new() { Name = "Velia", Surname = "Ceccarelli" }, new()));
        _privateValues.Add(ChangeType.Insert, (string.Empty, string.Empty));
        _privateValues.Add(ChangeType.Update, (string.Empty, string.Empty));
        _privateValues.Add(ChangeType.Delete, (string.Empty, string.Empty));

        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([Name], [Surname], [PrivateProperty], [PrivateField]) VALUES ('{_checkValues[ChangeType.Insert].Item1.Name}', '{_checkValues[ChangeType.Insert].Item1.Surname}', 'property-insert', 'field-insert')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [Name] = '{_checkValues[ChangeType.Update].Item1.Name}', [Surname] = '{_checkValues[ChangeType.Update].Item1.Surname}', [PrivateProperty] = 'property-update', [PrivateField] = 'field-update'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}