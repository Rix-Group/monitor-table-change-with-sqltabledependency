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
using System.Reflection;
using System.Runtime.Loader;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Test.Inheritance;

namespace TableDependency.SqlClient.Test.Features.Recovery;

public class DatabaseObjectCleanUpTestSqlServerModel
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Surname { get; set; } = string.Empty;
    public DateTime Born { get; set; }
    public int Quantity { get; set; }
}

public class DatabaseObjectCleanUpTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private const string TableName = "DatabaseObjectCleanUpTestTable";

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
            "[First Name] [nvarchar](50) NOT NULL, " +
            "[Second Name] [nvarchar](50) NOT NULL, " +
            "[Born] [datetime] NULL)";
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
    public async Task TestInsertAfterStop()
    {
        var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
        mapper.AddMapping(c => c.Name, "FIRST name").AddMapping(c => c.Surname, "Second Name");

        var tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
            ConnectionString,
            tableName: TableName,
            mapper: mapper,
            includeOldEntity: true,
            ct: TestContext.Current.CancellationToken);

        tableDependency.OnChanged += _ => { };
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        var dbObjectsNaming = tableDependency.NamingPrefix;

        await BigModifyTableContent();
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        await tableDependency.StopAsync();

        await SmallModifyTableContent();

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        Assert.True(await AreAllDbObjectDisposedAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestCleanUpAfter2InsertsTest()
    {
        var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
        mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

        var tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
            ConnectionString,
            tableName: TableName,
            mapper: mapper,
            includeOldEntity: true,
            ct: TestContext.Current.CancellationToken);

        tableDependency.OnChanged += _ => { };
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        var dbObjectsNaming = tableDependency.NamingPrefix;

        await Task.Delay(TimeSpan.FromSeconds(0.5), TestContext.Current.CancellationToken);

        await tableDependency.StopAsync();

        await ModifyTableContent();
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        Assert.True(await AreAllDbObjectDisposedAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestCleanUpAfterHugeInserts()
    {
        var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
        mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

        var tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
            ConnectionString,
            tableName: TableName,
            mapper: mapper,
            includeOldEntity: true,
            ct: TestContext.Current.CancellationToken);

        tableDependency.OnChanged += _ => { };
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        var dbObjectsNaming = tableDependency.NamingPrefix;

        await Task.Delay(TimeSpan.FromSeconds(0.5), TestContext.Current.CancellationToken);

        await tableDependency.StopAsync();

        await BigModifyTableContent();

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        Assert.True(await AreAllDbObjectDisposedAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(dbObjectsNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestStopWhileStillInserting()
    {
        var tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
            ConnectionString,
            tableName: TableName,
            ct: TestContext.Current.CancellationToken);

        string naming = tableDependency.NamingPrefix;

        tableDependency.OnChanged += _ => { };
        await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

        await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        // Run async tasks insering 100 rows in table every 250 milliseconds
        var task1 = ModifyTableContent();
        var task2 = ModifyTableContent();
        var task3 = ModifyTableContent();

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        await tableDependency.StopAsync();
        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        // Even if the background thread is still inserting data in table, db objects must be removed
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

        await Task.WhenAll(task1, task2, task3); // Wait for the tasks to complete to not interfeer with other tests!
    }

    #region Isolated Context

    [Fact]
    public async Task TestCollapsingIsolatedContext()
    {
        // Run async tasks insering 100 rows in table every 250 milliseconds
        var task1 = ModifyTableContent();
        var task2 = ModifyTableContent();
        var task3 = ModifyTableContent();

        // Execute the logic in a separate method scope
        // This unloads the ALC
        (var naming, var alcWeakRef) = await ExecuteInIsolatedContext();

        // Force the Garbage Collector to clean up
        for (int i = 0; i < 10 && alcWeakRef.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            // Give SQL Server Service Broker a moment to process the teardown
            await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        Assert.False(alcWeakRef.IsAlive, "The AssemblyLoadContext failed to unload after disposal.");

        // Even if the background thread is still inserting data in table, db objects must be removed
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));

        await Task.WhenAll(task1, task2, task3); // Wait for the tasks to complete to not interfeer with other tests!
    }

    private async Task<(string, WeakReference)> ExecuteInIsolatedContext()
    {
        var alc = new AssemblyLoadContext("TestContext", isCollectible: true);
        var alcWeakRef = new WeakReference(alc);

        var assemblyPath = typeof(RunsInAnIsolatedContextCheckDatabaseObjectCleanUp).Assembly.Location;
        Assembly assembly = alc.LoadFromAssemblyPath(assemblyPath);
        var type = assembly.GetType(typeof(RunsInAnIsolatedContextCheckDatabaseObjectCleanUp).FullName!);

        var instance = Activator.CreateInstance(type!);

        var runTableDependencyMethod = type!.GetMethod(nameof(RunsInAnIsolatedContextCheckDatabaseObjectCleanUp.RunTableDependency));
        var namingTask = (Task<string>)runTableDependencyMethod!.Invoke(instance, [ConnectionString, TableName])!;
        var naming = await namingTask;

        // Request unload immediately before leaving the method
        await ((IAsyncDisposable)instance!).DisposeAsync();
        alc.Unload();

        return (naming, alcWeakRef);
    }

    public class RunsInAnIsolatedContextCheckDatabaseObjectCleanUp : IAsyncDisposable
    {
        private SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? _tableDependency = null;

        public async Task<string> RunTableDependency(string connectionString, string tableName)
        {
            var mapper = new ModelToTableMapper<DatabaseObjectCleanUpTestSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name").AddMapping(c => c.Surname, "Second Name");

            _tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(connectionString, tableName: tableName, mapper: mapper);
            _tableDependency.OnChanged += OnChanged;
            await _tableDependency.StartAsync(60, 120);
            return _tableDependency.NamingPrefix;
        }

        private void OnChanged(RecordChangedEventArgs<DatabaseObjectCleanUpTestSqlServerModel> e)
        {
            // Leaving empty as only needed to unsubscribe in Dispose
        }

        public async ValueTask DisposeAsync()
        {
            if (_tableDependency is not null)
            {
                _tableDependency.OnChanged -= OnChanged;
                await _tableDependency.StopAsync();
                _tableDependency = null!;
            }

            GC.SuppressFinalize(this);
        }
    }

    #endregion Isolated Context

    #region Crash Simulation

    [Fact]
    public async Task TestCrashCleanupWithoutDispose()
    {
        var cts = new CancellationTokenSource();
        var modifyTask = ModifyTableContentContinuously(cts.Token);

        try
        {
            const int timeoutSeconds = 60;
            const int watchdogTimeoutSeconds = 180;

            var naming = await ExecuteCrashSimulationAsync(timeoutSeconds, watchdogTimeoutSeconds, TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(watchdogTimeoutSeconds + 5), TestContext.Current.CancellationToken);

            // Even if the background thread is still inserting data in table, db objects must be removed
            Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
            Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
        }
        finally
        {
            if (cts is not null)
            {
                await cts.CancelAsync();
                if (modifyTask is not null)
                    await modifyTask;

                cts.Dispose();
            }
        }
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
            if (process.HasExited)
                Assert.Fail("Crash sim exited before we could force kill.");

            process.Kill(true);
            await process.WaitForExitAsync(ct);
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

    #endregion Crash Simulation

    [Fact]
    public async Task TestThrowExceptionInCreateSqlServerDatabaseObjects()
    {
        SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? tableDependency = null;
        string objectNaming = string.Empty;
        Exception? ex = null;

        try
        {
            tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                logger: new TestLogger(throwExceptionCreateSqlServerDatabaseObjects: true),
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += _ => { };
            objectNaming = tableDependency.NamingPrefix;

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
        }
        catch (Exception e)
        {
            ex = e;
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        // Exception thrown in ConfigureAsync
        Assert.NotNull(ex);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestThrowExceptionInWaitForNotificationsPoint3()
    {
        SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? tableDependency = null;
        string objectNaming = string.Empty;
        Exception? ex = null;

        try
        {
            tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                logger: new TestLogger(throwExceptionInWaitForNotificationsPoint3: true),
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += _ => { };
            tableDependency.OnException += e => ex = e.Exception;
            objectNaming = tableDependency.NamingPrefix;

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);

            await SmallModifyTableContent(); // Needed to trigger reader
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.NotNull(ex);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestThrowExceptionInWaitForNotificationsPoint2()
    {
        SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? tableDependency = null;
        string objectNaming = string.Empty;
        Exception? ex = null;

        try
        {
            tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                logger: new TestLogger(throwExceptionInWaitForNotificationsPoint2: true),
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += _ => { };
            tableDependency.OnExceptionAsync = async e => ex = e.Exception;
            objectNaming = tableDependency.NamingPrefix;

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.NotNull(ex);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestThrowExceptionInWaitForNotificationsPoint1()
    {
        SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? tableDependency = null;
        string objectNaming = string.Empty;
        Exception? ex = null;

        try
        {
            tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                logger: new TestLogger(throwExceptionInWaitForNotificationsPoint1: true),
                ct: TestContext.Current.CancellationToken);

            tableDependency.OnChanged += _ => { };
            tableDependency.OnException += e => ex = e.Exception;
            objectNaming = tableDependency.NamingPrefix;

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.NotNull(ex);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TestStartWithError()
    {
        SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>? tableDependency = null;
        string objectNaming = string.Empty;
        Exception? ex = null;

        try
        {
            tableDependency = await SqlTableDependency<DatabaseObjectCleanUpTestSqlServerModel>.CreateSqlTableDependencyAsync(
                ConnectionString,
                tableName: TableName,
                logger: new TestLogger(throwExceptionBeforeWaitForNotifications: true),
                ct: TestContext.Current.CancellationToken);

            objectNaming = tableDependency.NamingPrefix;

            await tableDependency.StartAsync(ct: TestContext.Current.CancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        catch (Exception e)
        {
            ex = e;
        }
        finally
        {
            if (tableDependency is not null)
                await tableDependency.DisposeAsync();
        }

        Assert.NotNull(ex);

        Assert.True(await AreAllDbObjectDisposedAsync(objectNaming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(objectNaming, TestContext.Current.CancellationToken));
    }

    private async Task ModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        for (int i = 0; i < 100; i++)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{Guid.NewGuid()}', 'mah')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(0.25), TestContext.Current.CancellationToken);
        }
    }

    private async Task ModifyTableContentContinuously(CancellationToken ct)
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(ct);

        await using var sqlCommand = sqlConnection.CreateCommand();
        try
        {
            while (true)
            {
                sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{Guid.NewGuid()}', 'mah')";
                await sqlCommand.ExecuteNonQueryAsync(ct);

                await Task.Delay(TimeSpan.FromSeconds(0.25), ct);
            }
        }
        catch when (ct.IsCancellationRequested)
        {
            // Stop writing when the test completes.
        }
    }

    private async Task BigModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        for (var i = 0; i < 1000; i++)
        {
            sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('{i}', '{i}')";
            await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task SmallModifyTableContent()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"INSERT INTO [{TableName}] ([First Name], [Second Name]) VALUES ('allora', 'mah')";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}