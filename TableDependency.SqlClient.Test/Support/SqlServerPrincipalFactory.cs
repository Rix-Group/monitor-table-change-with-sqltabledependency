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

namespace TableDependency.SqlClient.Test.Support;

internal static class SqlServerPrincipalFactory
{
    private const string ProbePassword = "Td_Probe_Pass123!";

    // Every legacy broad grant except CONTROL; used by baseline and negative permission probes.
    public static readonly string[] LegacyDatabasePermissions =
    [
        "ALTER",
        "CONNECT",
        "CREATE CONTRACT",
        "CREATE MESSAGE TYPE",
        "CREATE PROCEDURE",
        "CREATE QUEUE",
        "CREATE SERVICE",
        "EXECUTE",
        "SELECT",
        "SUBSCRIBE QUERY NOTIFICATIONS",
        "VIEW DATABASE STATE",
        "VIEW DEFINITION",
    ];

    // Database-level rights the effective-permission guard requires; schema/table rights are granted separately.
    public static readonly string[] BrokerCreatePermissions =
    [
        "CONNECT",
        "CREATE CONTRACT",
        "CREATE MESSAGE TYPE",
        "CREATE PROCEDURE",
        "CREATE QUEUE",
        "CREATE SERVICE",
    ];

    public static async Task<string> CreateLoginAsync(
        string adminConnectionString,
        string login,
        IReadOnlyList<string> databaseGrants,
        IReadOnlyList<string> extraStatements,
        CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(adminConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        var quotedLogin = QuoteIdentifier(login);
        sqlCommand.CommandText = $"IF SUSER_ID(N'{SqlLiteral(login)}') IS NULL CREATE LOGIN {quotedLogin} WITH PASSWORD = '{ProbePassword}', CHECK_POLICY = OFF;";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        sqlCommand.CommandText = $"IF USER_ID(N'{SqlLiteral(login)}') IS NULL CREATE USER {quotedLogin} FOR LOGIN {quotedLogin};";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        foreach (var permission in databaseGrants)
        {
            sqlCommand.CommandText = $"GRANT {permission} TO {quotedLogin};";
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }

        foreach (var statement in extraStatements)
        {
            sqlCommand.CommandText = statement;
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }

        return new SqlConnectionStringBuilder(adminConnectionString)
        {
            UserID = login,
            Password = ProbePassword,
            IntegratedSecurity = false,
            // No pooling: connection-kill tests would otherwise reuse a dead pooled connection on the next operation.
            Pooling = false,
        }.ConnectionString;
    }

    public static async Task GrantTableObjectPermissionsAsync(
        string adminConnectionString,
        string login,
        string schemaName,
        string tableName,
        CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(adminConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        foreach (var permission in (string[])["ALTER", "SELECT"])
        {
            sqlCommand.CommandText = $"GRANT {permission} ON OBJECT::{QuoteIdentifier(schemaName)}.{QuoteIdentifier(tableName)} TO {QuoteIdentifier(login)};";
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }
    }

    public static string CreateSchemaStatement(string schemaName, string? ownerLogin = null)
    {
        var ownerClause = ownerLogin is null ? string.Empty : $" AUTHORIZATION {QuoteIdentifier(ownerLogin)}";
        var alterOwnerStatement = ownerLogin is null ? string.Empty : $" ALTER AUTHORIZATION ON SCHEMA::{QuoteIdentifier(schemaName)} TO {QuoteIdentifier(ownerLogin)};";
        return $"IF SCHEMA_ID(N'{SqlLiteral(schemaName)}') IS NULL EXEC(N'CREATE SCHEMA {QuoteIdentifierForDynamicSql(schemaName)}{ownerClause.Replace("'", "''")}');{alterOwnerStatement}";
    }

    public static string GrantSchemaControlStatement(string schemaName, string login)
        => $"GRANT CONTROL ON SCHEMA::{QuoteIdentifier(schemaName)} TO {QuoteIdentifier(login)};";

    public static string QuoteIdentifier(string value)
        => $"[{value.Replace("]", "]]")}]";

    private static string QuoteIdentifierForDynamicSql(string value)
        => QuoteIdentifier(value).Replace("'", "''");

    private static string SqlLiteral(string value)
        => value.Replace("'", "''");
}