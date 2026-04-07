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

using System.Globalization;
using TableDependency.SqlClient.Base;

namespace TableDependency.SqlClient.Test.CrashSim;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var options = ParseOptions(args);
            if (string.IsNullOrWhiteSpace(options.ConnectionString) || string.IsNullOrWhiteSpace(options.TableName))
            {
                await Console.Error.WriteLineAsync("Missing required args: --connectionString and --tableName");
                return 1;
            }

            var mapper = new ModelToTableMapper<CrashSimModel>()
                .AddMapping(c => c.Name, "First Name")
                .AddMapping(c => c.Surname, "Second Name");

            var tableDependency = await SqlTableDependency<CrashSimModel>.CreateSqlTableDependencyAsync(
                options.ConnectionString,
                tableName: options.TableName,
                mapper: mapper);

            tableDependency.OnChanged += _ => { };
            await tableDependency.StartAsync(options.TimeoutSeconds, options.WatchdogTimeoutSeconds);

            Console.WriteLine($"NAMING: {tableDependency.NamingPrefix}");
            await Console.Out.FlushAsync();

            await Task.Delay(Timeout.InfiniteTimeSpan);
            return 0;
        }
        catch (Exception ex)
        {
            await Console.Error.WriteLineAsync(ex.ToString());
            return 1;
        }
    }

    private static CrashSimOptions ParseOptions(string[] args)
    {
        var options = new CrashSimOptions();
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--connectionString":
                    options.ConnectionString = ReadValue(args, ref i);
                    break;
                case "--tableName":
                    options.TableName = ReadValue(args, ref i);
                    break;
                case "--timeout":
                    options.TimeoutSeconds = ParseInt(ReadValue(args, ref i), args[i]);
                    break;
                case "--watchdogTimeout":
                    options.WatchdogTimeoutSeconds = ParseInt(ReadValue(args, ref i), args[i]);
                    break;
            }
        }

        return options;
    }

    private static string ReadValue(string[] args, ref int index)
    {
        if (index + 1 >= args.Length)
            throw new ArgumentException($"Missing value for {args[index]}");

        index += 1;
        return args[index];
    }

    private static int ParseInt(string value, string argName)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var result))
            throw new ArgumentException($"Invalid value for {argName}: {value}");

        return result;
    }
}