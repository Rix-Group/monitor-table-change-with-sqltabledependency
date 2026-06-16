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
using Testcontainers.MsSql;

[assembly: AssemblyFixture(typeof(DatabaseFixture))]

namespace TableDependency.SqlClient.Test.Fixtures;

public sealed class DatabaseFixture : IAsyncLifetime
{
    private const string BaselineLogin = "td_baseline";
    private const string NoControlLogin = "td_no_control";
    private const string SchemaControlLogin = "td_schema_control";
    private const string ConnectOnlyLogin = "td_connect_only";
    private const string ProbePassword = "Td_Probe_Pass123!";

    // Logins used by tests to grant table-object permissions after the table exists.
    public const string OwnsBrokerLogin = "td_owns_broker";
    public const string UnrelatedControlLogin = "td_unrelated_control";

    // Dedicated broker schema the library uses; td_owns_broker owns it.
    public static readonly string BrokerSchemaName = SqlTableDependency<object>.DefaultBrokerSchemaName;

    // Schema td_unrelated_control holds CONTROL on - unrelated to the broker objects.
    private const string UnrelatedSchemaName = "unrelated";

    // Admin connection for test scaffolding.
    public string MsSqlContainerConnectionString { get; private set; } = string.Empty;

    // Non-admin baseline connection handed to SqlTableDependency under test: every grant plus db-wide CONTROL.
    public string BaselineConnectionString { get; private set; } = string.Empty;

    // Every current grant except CONTROL, broker objects in dbo.
    public string NoControlConnectionString { get; private set; } = string.Empty;

    // ...plus GRANT CONTROL ON SCHEMA::[dbo] (no db-wide CONTROL).
    public string SchemaControlConnectionString { get; private set; } = string.Empty;

    // No CONTROL anywhere; only the broker-create grants and ownership of the dedicated SqlTableDependency schema.
    public string OwnsBrokerConnectionString { get; private set; } = string.Empty;

    // No broker-schema rights, but holds CONTROL on an unrelated schema (passes the old name-scan guard, fails the effective guard).
    public string UnrelatedControlConnectionString { get; private set; } = string.Empty;

    // Only CONNECT - cannot create a schema (no CREATE SCHEMA, no db-wide ALTER/CONTROL).
    public string ConnectOnlyConnectionString { get; private set; } = string.Empty;

    private MsSqlContainer? _msSqlContainer;
    public async ValueTask InitializeAsync()
    {
        _msSqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:latest").Build();
        await _msSqlContainer.StartAsync(TestContext.Current.CancellationToken);

        var connectionString = _msSqlContainer.GetConnectionString();

        await using var sqlConnection = new SqlConnection(connectionString);
        await sqlConnection.OpenAsync();

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = "CREATE DATABASE [TableDependencyDB];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        MsSqlContainerConnectionString = connectionString.Replace("Database=master", "Database=TableDependencyDB");

        var ct = TestContext.Current.CancellationToken;
        BaselineConnectionString = await CreateProbeLoginAsync(BaselineLogin, [.. BaseDatabasePermissions, "CONTROL"], extraStatements: [], ct);
        NoControlConnectionString = await CreateProbeLoginAsync(NoControlLogin, BaseDatabasePermissions, extraStatements: [], ct);
        SchemaControlConnectionString = await CreateProbeLoginAsync(SchemaControlLogin, BaseDatabasePermissions, extraStatements: [$"GRANT CONTROL ON SCHEMA::[dbo] TO [{SchemaControlLogin}];"], ct);
        OwnsBrokerConnectionString = await CreateProbeLoginAsync(OwnsBrokerLogin, BrokerCreatePermissions, extraStatements: [$"CREATE SCHEMA [{BrokerSchemaName}] AUTHORIZATION [{OwnsBrokerLogin}];"], ct);
        UnrelatedControlConnectionString = await CreateProbeLoginAsync(UnrelatedControlLogin, BrokerCreatePermissions, extraStatements: [$"CREATE SCHEMA [{UnrelatedSchemaName}];", $"GRANT CONTROL ON SCHEMA::[{UnrelatedSchemaName}] TO [{UnrelatedControlLogin}];"], ct);
        ConnectOnlyConnectionString = await CreateProbeLoginAsync(ConnectOnlyLogin, ["CONNECT"], extraStatements: [], ct);
    }

    private async Task<string> CreateProbeLoginAsync(string login, IReadOnlyList<string> databaseGrants, IReadOnlyList<string> extraStatements, CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(MsSqlContainerConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();

        sqlCommand.CommandText = $"CREATE LOGIN [{login}] WITH PASSWORD = '{ProbePassword}', CHECK_POLICY = OFF;";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        sqlCommand.CommandText = $"CREATE USER [{login}] FOR LOGIN [{login}];";
        await sqlCommand.ExecuteNonQueryAsync(ct);

        foreach (var permission in databaseGrants)
        {
            sqlCommand.CommandText = $"GRANT {permission} TO [{login}];";
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }

        foreach (var statement in extraStatements)
        {
            sqlCommand.CommandText = statement;
            await sqlCommand.ExecuteNonQueryAsync(ct);
        }

        return new SqlConnectionStringBuilder(MsSqlContainerConnectionString)
        {
            UserID = login,
            Password = ProbePassword,
            IntegratedSecurity = false,
            // No pooling: connection-kill tests would otherwise reuse a dead pooled connection on the next operation.
            Pooling = false,
        }.ConnectionString;
    }

    // Every current required grant except CONTROL.
    private static readonly string[] BaseDatabasePermissions =
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

    // Database-level rights the effective-permission guard actually requires (the rest comes from broker-schema ownership + table-object grants).
    private static readonly string[] BrokerCreatePermissions =
    [
        "CONNECT",
        "CREATE CONTRACT",
        "CREATE MESSAGE TYPE",
        "CREATE PROCEDURE",
        "CREATE QUEUE",
        "CREATE SERVICE",
    ];

    public async ValueTask DisposeAsync()
    {
        if (_msSqlContainer is not null)
            await _msSqlContainer.DisposeAsync();
    }
}