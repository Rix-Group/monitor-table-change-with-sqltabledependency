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

using System.Linq.Expressions;
using TableDependency.SqlClient.Base;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Test.Aspire.AppHost.ServiceDefaults;
using TableDependency.SqlClient.Test.Client.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Client;

public static class Program
{
    public static async Task Main()
    {
        ConsoleKeyInfo consoleKeyInfo;
        var originalForegroundColor = Console.ForegroundColor;

        do
        {
            Console.Clear();

            Console.Write("TableDependency, SqlTableDependency");
            Console.ForegroundColor = ConsoleColor.Blue;
            Console.WriteLine($".NET {Environment.Version}");
            Console.ForegroundColor = originalForegroundColor;
            Console.WriteLine("Copyright (c) 2015-2020 Christian Del Bianco.");
            Console.WriteLine("All rights reserved." + Environment.NewLine);
            Console.WriteLine("**********************************************************************************************");
            Console.WriteLine("Choose connection string:");
            Console.WriteLine(" - F4: user sa");
            Console.WriteLine(" - F5: user Test_User");
            Console.WriteLine(" - ESC to exit");
            Console.WriteLine("**********************************************************************************************");

            consoleKeyInfo = Console.ReadKey();
            if (consoleKeyInfo.Key is ConsoleKey.Escape)
                Environment.Exit(0);

        } while (consoleKeyInfo.Key is not ConsoleKey.F4 and not ConsoleKey.F5);

        Console.ResetColor();

        var connectionString = consoleKeyInfo.Key is ConsoleKey.F4
            ? ServiceNames.SaConnectionString
            : ServiceNames.TestUserConnectionString;

        // Mapper for DB columns not matching Model's columns
        var mapper = new ModelToTableMapper<Product>();
        mapper.AddMapping(c => c.Expiring, "ExpiringDate");

        // Define WHERE filter
        Expression<Func<Product, bool>> expression = p => (p.CategoryId == (int)CategoryEnum.Food || p.CategoryId == (int)CategoryEnum.Drink) && p.Quantity <= 10;
        ITableDependencyFilter whereCondition = new SqlTableDependencyFilter<Product>(expression, mapper);

        // As table name (Products) does not match model name (Product), its definition is needed.
        await using var dep = await SqlTableDependency<Product>.CreateSqlTableDependencyAsync(connectionString, tableName: "Products", mapper: mapper, filter: whereCondition, includeOldEntity: true, persistentId: "persistent");

        dep.OnChanged += Changed;
        dep.OnException += OnException;
        dep.OnStatusChanged += OnStatusChanged;

        await dep.StartAsync();

        Console.WriteLine();
        Console.WriteLine("Waiting for receiving notifications (db objects naming: " + dep.NamingPrefix + ")...");
        Console.WriteLine("Press a key to stop.");
        Console.ReadKey();
    }

    private static void OnException(ExceptionEventArgs e)
    {
        Console.WriteLine(Environment.NewLine);

        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(e.Message);
        Console.WriteLine(e.Exception?.Message);
        Console.WriteLine(e.Exception?.StackTrace);
        Console.ResetColor();
    }

    private static void OnStatusChanged(StatusChangedEventArgs e)
    {
        Console.WriteLine(Environment.NewLine);

        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"SqlTableDependency Status = {e.Status.ToString()}");
        Console.ResetColor();
    }

    private static void Changed(RecordChangedEventArgs<Product> e)
    {
        Console.WriteLine(Environment.NewLine);

        if (e.ChangeType is not ChangeType.None)
        {
            var changedEntity = e.Entity;
            Console.WriteLine("Change Type: " + e.ChangeType);
            Console.WriteLine("Id: " + changedEntity.Id);
            Console.WriteLine("Name: " + changedEntity.Name);
            Console.WriteLine("Expiring: " + changedEntity.Expiring);
            Console.WriteLine("Quantity: " + changedEntity.Quantity);
            Console.WriteLine("Price: " + changedEntity.Price);
        }

        if (e.ChangeType is ChangeType.Update && e.OldEntity is not null)
        {
            Console.WriteLine(Environment.NewLine);

            var changedEntity = e.OldEntity;
            Console.WriteLine("Id (OLD): " + changedEntity.Id);
            Console.WriteLine("Name (OLD): " + changedEntity.Name);
            Console.WriteLine("Expiring (OLD): " + changedEntity.Expiring);
            Console.WriteLine("Quantity (OLD): " + changedEntity.Quantity);
            Console.WriteLine("Price (OLD): " + changedEntity.Price);
        }
    }
}