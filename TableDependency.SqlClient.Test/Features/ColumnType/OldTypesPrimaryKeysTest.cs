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

namespace TableDependency.SqlClient.Test.Features.ColumnType;

public class OldTypesPrimaryKeysTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = nameof(OldTypesPrimaryKeysTest);

    [Theory]
    [InlineData("TEXT")]
    [InlineData("NTEXT")]
    [InlineData("XML")]
    [InlineData("GEOGRAPHY")]
    [InlineData("GEOMETRY")]
    [InlineData("IMAGE")]
    [InlineData("STRUCTURED")]
    [InlineData("TABLE")]
    [InlineData("CURSOR")]
    public async Task PrimaryKeyIsRejected(string sqlTypeName)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        try
        {
            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([MyKey] {sqlTypeName} NOT NULL PRIMARY KEY, [Description] NVARCHAR(100) NULL)";

            await Assert.ThrowsAsync<SqlException>(() => sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            await using var cleanupCommand = sqlConnection.CreateCommand();
            cleanupCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
            await cleanupCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }
}