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
    public string MsSqlContainerConnectionString { get; private set; } = string.Empty;

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
    }

    public async ValueTask DisposeAsync()
    {
        if (_msSqlContainer is not null)
            await _msSqlContainer.DisposeAsync();
    }
}