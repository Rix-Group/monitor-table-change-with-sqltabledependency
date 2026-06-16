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
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Extensions;

namespace TableDependency.SqlClient.Test.Features.Misc;

// EnsureBrokerSchemaExistsAsync creates the schema when missing, is idempotent, and surfaces a clear error
// when the principal cannot create it.
public class EnsureBrokerSchemaTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    public override async ValueTask DisposeAsync()
    {
        await DropSchemaAsync("td_ensure_create");
        await DropSchemaAsync("td_ensure_idempotent");
    }

    [Fact]
    public async Task CreatesSchemaWhenMissing()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;
        Assert.False(await SchemaExistsAsync("td_ensure_create", ct));

        // ACT
        await ConnectionString.EnsureBrokerSchemaExistsAsync("td_ensure_create", ct);

        // ASSERT
        Assert.True(await SchemaExistsAsync("td_ensure_create", ct));
    }

    [Fact]
    public async Task IsIdempotentWhenSchemaExists()
    {
        // ARRANGE
        var ct = TestContext.Current.CancellationToken;

        // ACT - twice; the second call must not throw
        await ConnectionString.EnsureBrokerSchemaExistsAsync("td_ensure_idempotent", ct);
        await ConnectionString.EnsureBrokerSchemaExistsAsync("td_ensure_idempotent", ct);

        // ASSERT
        Assert.True(await SchemaExistsAsync("td_ensure_idempotent", ct));
    }

    [Fact]
    public async Task ThrowsWhenPrincipalCannotCreateSchema()
    {
        // ARRANGE - a CONNECT-only principal (no CREATE SCHEMA, no db-wide ALTER/CONTROL), schema absent
        var ct = TestContext.Current.CancellationToken;
        Assert.False(await SchemaExistsAsync("td_ensure_denied", ct));

        // ACT / ASSERT
        await Assert.ThrowsAsync<BrokerSchemaUnavailableException>(
            () => ConnectOnlyConnectionString.EnsureBrokerSchemaExistsAsync("td_ensure_denied", ct));
        Assert.False(await SchemaExistsAsync("td_ensure_denied", ct));
    }

    private async Task<bool> SchemaExistsAsync(string schemaName, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "SELECT CASE WHEN SCHEMA_ID(@schema) IS NULL THEN 0 ELSE 1 END;";
        sqlCommand.Parameters.AddWithValue("@schema", schemaName);
        return await sqlCommand.ExecuteScalarAsync(ct) is 1;
    }

    private async Task DropSchemaAsync(string schemaName)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF SCHEMA_ID('{schemaName}') IS NOT NULL DROP SCHEMA [{schemaName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }
}