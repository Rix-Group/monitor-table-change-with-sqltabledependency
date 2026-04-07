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
using TableDependency.SqlClient.Test.Aspire.AppHost.ServiceDefaults;

namespace TableDependency.SqlClient.Test.LoadTests.Writer1;

public static class Program
{
    public static async Task Main()
    {
        int deletedCnt = 0;
        int insertedCnt = 0;
        int updatedCnt = 0;
        int index = 1;
        int i = 1;

        Console.Title = new string('*', 10) + " SQL ServerDB Writer 1 " + new string('*', 10);
        await Task.Delay(TimeSpan.FromSeconds(4));

        await using var sqlConnection = new SqlConnection(ServiceNames.TestUserConnectionString);
        await sqlConnection.OpenAsync();

        await using var sqlCommand = sqlConnection.CreateCommand();
        while (index < 999999)
        {
            switch (i)
            {
                case 1:
                    sqlCommand.CommandText = "INSERT INTO [LoadTest] ([Id], [FirstName], [SecondName]) VALUES (1, 'mannaggia', 'alla puttana')";
                    if (await sqlCommand.ExecuteNonQueryAsync() > 0)
                        insertedCnt++;
                    i++;
                    break;

                case 2:
                    sqlCommand.CommandText = "UPDATE [LoadTest] SET [FirstName] = 'cazzarola', [SecondName] = '" + Guid.NewGuid() + "' WHERE [Id] = 1";
                    if (await sqlCommand.ExecuteNonQueryAsync() > 0)
                        updatedCnt++;
                    i++;
                    break;

                case 3:
                    sqlCommand.CommandText = "DELETE FROM [LoadTest] WHERE [Id] = 1";
                    if (await sqlCommand.ExecuteNonQueryAsync() > 0)
                        deletedCnt++;
                    i = 1;
                    break;
            }

            Console.WriteLine("Writer 1 executed: " + Environment.NewLine + sqlCommand.CommandText);
            await Task.Delay(50);
            index++;
        }

        Console.WriteLine("INSERT counter: " + insertedCnt);
        Console.WriteLine("UPDATE counter: " + updatedCnt);
        Console.WriteLine("DELETE counter: " + deletedCnt);

        Console.WriteLine(Environment.NewLine + "Press a key to exit");
        Console.ReadKey();
    }
}