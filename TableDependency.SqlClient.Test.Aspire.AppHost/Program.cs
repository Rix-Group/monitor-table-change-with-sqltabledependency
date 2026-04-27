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

using TableDependency.SqlClient.Test.Aspire.AppHost.ServiceDefaults;

namespace TableDependency.SqlClient.Test.Aspire.AppHost;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = DistributedApplication.CreateBuilder(args);

        var sqlDb = builder.ConfigureSqlServer();

        var app = builder.Build();

        // Close once database is setup
        sqlDb.OnResourceReady((_, _, ct) => app.StopAsync(ct));

        await app.RunAsync();
    }

    private static IResourceBuilder<SqlServerDatabaseResource> ConfigureSqlServer(this IDistributedApplicationBuilder builder)
    {
        var sqlPassword = builder.AddParameter("sqlpassword", secret: false);
        var sqlServer = builder.AddSqlServer(ServiceNames.SqlServerName, password: sqlPassword, port: ServiceNames.SqlServerPort)
            .WithImage("mssql/server", "2025-latest")
            .WithLifetime(ContainerLifetime.Persistent)
            .WithEndpointProxySupport(false); // Make accessible after Aspire closes

        var sqlDb = sqlServer.AddDatabase(ServiceNames.SqlDatabaseName);
        string creationScript = File.ReadAllText("sql/createDatabase.sql");
        sqlDb.WithCreationScript(creationScript);
        return sqlDb;
    }
}