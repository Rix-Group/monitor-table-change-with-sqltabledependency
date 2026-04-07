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
using TableDependency.SqlClient.Test.Aspire.AppHost.ServiceDefaults;

namespace TableDependency.SqlClient.Test.LoadTests.Listener;

internal static class Program
{
    private static int _insertCounter;
    private static int _updateCounter;
    private static int _deleteCounter;

    private static async Task DropAndCreateTableAsync(string connectionString)
    {
        await using var sqlConnection = new SqlConnection(connectionString);
        await sqlConnection.OpenAsync();

        await using var sqlCommand = sqlConnection.CreateCommand();
        try
        {
            sqlCommand.CommandText = "DROP TABLE [dbo].[LoadTest]";
            await sqlCommand.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }

        sqlCommand.CommandText = "CREATE TABLE [LoadTest] ([Id] [int], [FirstName] nvarchar(50), [SecondName] nvarchar(50))";
        await sqlCommand.ExecuteNonQueryAsync();
    }

    private static async Task Main()
    {
        var connectionString = ServiceNames.TestUserConnectionString;
        await DropAndCreateTableAsync(connectionString);

        await using var tableDependency = await SqlTableDependency<LoadTest>.CreateSqlTableDependencyAsync(connectionString);
        tableDependency.OnChanged += TableDependency_Changed;
        tableDependency.OnException += TableDependency_OnException;

        await tableDependency.StartAsync();
        Console.WriteLine("Waiting for receiving notifications...");
        Console.WriteLine("Press a key to stop");
        Console.ReadKey();

        await tableDependency.StopAsync();
    }

    private static void TableDependency_OnException(ExceptionEventArgs e)
        => Console.WriteLine(e.Exception?.Message ?? "An error has occurred.");

    private static void TableDependency_Changed(RecordChangedEventArgs<LoadTest> e)
    {
        Console.WriteLine(Environment.NewLine);

        if (e.ChangeType is ChangeType.Insert)
            _insertCounter++;

        if (e.ChangeType is ChangeType.Update)
            _updateCounter++;

        if (e.ChangeType is ChangeType.Delete)
            _deleteCounter++;

        var changedEntity = e.Entity;
        Console.WriteLine("DML operation: " + e.ChangeType);
        Console.WriteLine("Id: " + changedEntity.Id);
        Console.WriteLine("FirstName: " + changedEntity.FirstName);
        Console.WriteLine("SecondName: " + changedEntity.SecondName);
        Console.WriteLine("---------------------------------------------");
        Console.WriteLine("Insert: " + _insertCounter);
        Console.WriteLine("Update: " + _updateCounter);
        Console.WriteLine("Delete: " + _deleteCounter);
    }
}