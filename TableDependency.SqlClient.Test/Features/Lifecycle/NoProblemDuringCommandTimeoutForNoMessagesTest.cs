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
using System.Runtime.Loader;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class NoProblemDuringCommandTimeoutForNoMessagesTest(DatabaseFixture databaseFixture) : SqlTableDependencyBaseTest(databaseFixture)
{
    private class NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Surname { get; set; } = string.Empty;
        public DateTime Born { get; set; }
        public int Quantity { get; set; }
    }

    private static readonly string TableName = typeof(NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel).Name;

    public override async ValueTask InitializeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(TestContext.Current.CancellationToken);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        sqlCommand.CommandText = $"CREATE TABLE [{TableName}]([Id][int] IDENTITY(1, 1) NOT NULL, [First Name] [NVARCHAR](50) NOT NULL)";
        await sqlCommand.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Test()
    {
        // Execute the logic in a separate method scope
        // This unloads the ALC
        (var naming, var status, var alcWeakRef) = await ExecuteInIsolatedContext();

        // Force the Garbage Collector to clean up
        for (int i = 0; i < 10 && alcWeakRef.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();

            // Give SQL Server Service Broker a moment to process the teardown
            await Task.Delay(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }

        Assert.False(alcWeakRef.IsAlive, "The AssemblyLoadContext failed to unload!");
        Assert.True(status is not nameof(TableDependencyStatus.StopDueToError) and not nameof(TableDependencyStatus.StopDueToCancellation));
        Assert.True(await AreAllDbObjectDisposedAsync(naming, TestContext.Current.CancellationToken));
        Assert.Equal(0, await CountConversationEndpointsAsync(naming, TestContext.Current.CancellationToken));
    }

    private async Task<(string, string, WeakReference)> ExecuteInIsolatedContext()
    {
        var alc = new AssemblyLoadContext("TestContext", isCollectible: true);
        var alcWeakRef = new WeakReference(alc);

        var assemblyPath = typeof(RunsInAnIsolatedContextNoMessage).Assembly.Location;
        var assembly = alc.LoadFromAssemblyPath(assemblyPath);
        var type = assembly.GetType(typeof(RunsInAnIsolatedContextNoMessage).FullName!);

        var instance = Activator.CreateInstance(type!)!;

        var runTableDependencyMethod = type!.GetMethod(nameof(RunsInAnIsolatedContextNoMessage.RunTableDependency));
        var naming = await (Task<string>)runTableDependencyMethod!.Invoke(instance, [ConnectionString, TableName])!;

        var getTableDependencyStatusMethod = type.GetMethod(nameof(RunsInAnIsolatedContextNoMessage.GetTableDependencyStatus));
        var status = getTableDependencyStatusMethod!.Invoke(instance, [])?.ToString();

        // Request unload immediately before leaving the method
        await ((IAsyncDisposable)instance).DisposeAsync();
        alc.Unload();

        return (naming, status!, alcWeakRef);
    }

    public override async ValueTask DisposeAsync()
    {
        await using var sqlConnection = new SqlConnection(ConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        await using var sqlCommand = sqlConnection.CreateCommand();
        sqlCommand.CommandText = $"IF OBJECT_ID('{TableName}', 'U') IS NOT NULL DROP TABLE [{TableName}];";
        await sqlCommand.ExecuteNonQueryAsync(CancellationToken.None);
    }

    public sealed class RunsInAnIsolatedContextNoMessage : IAsyncDisposable
    {
        private SqlTableDependency<NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel>? _tableDependency;

        public TableDependencyStatus GetTableDependencyStatus()
            => _tableDependency?.Status ?? TableDependencyStatus.None;

        public async Task<string> RunTableDependency(string connectionString, string tableName)
        {
            var mapper = new ModelToTableMapper<NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel>();
            mapper.AddMapping(c => c.Name, "First Name");

            _tableDependency = await SqlTableDependency<NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel>.CreateSqlTableDependencyAsync(connectionString, tableName: tableName, mapper: mapper);
            _tableDependency.OnChanged += OnChanged;
            await _tableDependency.StartAsync(60, 120);

            return _tableDependency.NamingPrefix;
        }

        private void OnChanged(RecordChangedEventArgs<NoProblemDurignCommandTimeoutForNoMessagesSqlServerModel> e)
        {
            // Implementation not needed for test
        }

        public async ValueTask DisposeAsync()
        {
            if (_tableDependency is not null)
            {
                _tableDependency.OnChanged -= OnChanged;
                await _tableDependency.DisposeAsync();
            }

            _tableDependency = null;
            GC.SuppressFinalize(this);
        }
    }
}