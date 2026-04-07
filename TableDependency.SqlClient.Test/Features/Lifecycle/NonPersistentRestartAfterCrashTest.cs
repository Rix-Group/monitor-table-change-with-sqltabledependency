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
using System.Diagnostics;
using System.Globalization;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class NonPersistentRestartAfterCrashTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private sealed class NonPersistentRestartAfterCrashTestModel
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public string Surname { get; set; } = string.Empty;
    }

    private const string TableName = nameof(NonPersistentRestartAfterCrashTestModel);
    private int _changeCounter;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}] ([Id] INT IDENTITY(1, 1) NOT NULL, [First Name] NVARCHAR(50) NOT NULL, [Second Name] NVARCHAR(50) NOT NULL);";
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
        const int timeoutSeconds = 60;
        const int watchdogTimeoutSeconds = 120;

        var crashedNaming = await ExecuteCrashSimulationAsync(timeoutSeconds, watchdogTimeoutSeconds, TestContext.Current.CancellationToken);

        // Force-killed non-persistent listeners rely on watchdog cleanup, so objects should still exist immediately after crash.
        Assert.False(await AreAllDbObjectDisposedAsync(crashedNaming, TestContext.Current.CancellationToken));

        SqlTableDependency<NonPersistentRestartAfterCrashTestModel>? tableDependency = null;
        string restartedNaming = string.Empty;

        try
        {
            var mapper = new ModelToTableMapper<NonPersistentRestartAfterCrashTestModel>()
                .AddMapping(c => c.Name, "First Name")
                .AddMapping(c => c.Surname, "Second Name");

            tableDependency = await SqlTableDependency<NonPersistentRestartAfterCrashTestModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                mapper: mapper,
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += TableDependencyOnChanged;
            await tableDependency.StartAsync(timeoutSeconds, watchdogTimeoutSeconds, ct: TestContext.Current.CancellationToken);

            restartedNaming = tableDependency.NamingPrefix;
            Assert.NotEqual(crashedNaming, restartedNaming);

            await ModifyTableContentAsync();
            await WaitForCounterAsync(expectedCount: 3, timeout: TimeSpan.FromSeconds(20), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.Equal(3, _changeCounter);
        Assert.False(string.IsNullOrWhiteSpace(restartedNaming));
        Assert.True(await AreAllDbObjectDisposedAsync(restartedNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(restartedNaming, TestContext.Current.CancellationToken));
    }

    private void TableDependencyOnChanged(RecordChangedEventArgs<NonPersistentRestartAfterCrashTestModel> _)
        => _changeCounter++;

    private async Task ModifyTableContentAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('A', 'A')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"UPDATE [{TableName}] SET [First Name] = 'B', [Second Name] = 'B'";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"DELETE FROM [{TableName}]";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task WaitForCounterAsync(int expectedCount, TimeSpan timeout, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            if (_changeCounter >= expectedCount)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(200), ct);
        }

        Assert.Fail($"Expected {expectedCount} notifications but observed {_changeCounter} in {timeout.TotalSeconds} seconds.");
    }

    private async Task<string> ExecuteCrashSimulationAsync(int timeoutSeconds, int watchdogTimeoutSeconds, CancellationToken ct)
    {
        var crashSimPath = Path.Combine(AppContext.BaseDirectory, "TableDependency.SqlClient.Test.CrashSim.dll");
        if (!File.Exists(crashSimPath))
            throw new FileNotFoundException("Crash simulator not found.", crashSimPath);

        var startInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        startInfo.ArgumentList.Add(crashSimPath);
        startInfo.ArgumentList.Add("--connectionString");
        startInfo.ArgumentList.Add(ConnectionString);
        startInfo.ArgumentList.Add("--tableName");
        startInfo.ArgumentList.Add(TableName);
        startInfo.ArgumentList.Add("--timeout");
        startInfo.ArgumentList.Add(timeoutSeconds.ToString(CultureInfo.InvariantCulture));
        startInfo.ArgumentList.Add("--watchdogTimeout");
        startInfo.ArgumentList.Add(watchdogTimeoutSeconds.ToString(CultureInfo.InvariantCulture));

        using var process = Process.Start(startInfo);
        if (process is null)
            Assert.Fail("Failed to start crash simulation process.");

        try
        {
            return await ReadNamingFromConsoleAsync(process, ct);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(true);
                await process.WaitForExitAsync(ct);
            }
        }
    }

    private static async Task<string> ReadNamingFromConsoleAsync(Process process, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(30));

        while (true)
        {
            var line = await process.StandardOutput.ReadLineAsync(timeoutCts.Token);
            if (line is null)
            {
                var error = await process.StandardError.ReadToEndAsync(timeoutCts.Token);
                throw new InvalidOperationException($"Crash simulator exited before reporting naming. {error}");
            }

            const string prefix = "NAMING: ";
            if (line.StartsWith(prefix, StringComparison.Ordinal))
                return line[prefix.Length..].Trim();
        }
    }
}