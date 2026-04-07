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

using Microsoft.Extensions.Logging;

namespace TableDependency.SqlClient.Test.Inheritance;

internal class TestLogger(
    bool throwExceptionBeforeWaitForNotifications = false,
    bool throwExceptionCreateSqlServerDatabaseObjects = false,
    bool throwExceptionInWaitForNotificationsPoint1 = false,
    bool throwExceptionInWaitForNotificationsPoint2 = false,
    bool throwExceptionInWaitForNotificationsPoint3 = false)
    : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        => null;

    public bool IsEnabled(LogLevel logLevel)
        => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (throwExceptionBeforeWaitForNotifications && state?.ToString() is "Starting wait for notifications.")
            throw new Exception();

        if (throwExceptionCreateSqlServerDatabaseObjects && state?.ToString() is "Associated Activation Store Procedure to sender queue.")
            throw new Exception();

        if (throwExceptionInWaitForNotificationsPoint1 && state?.ToString() is "Get in WaitForNotifications.")
            throw new Exception();

        if (throwExceptionInWaitForNotificationsPoint2 && state?.ToString() is "Executing WAITFOR command.")
            throw new Exception();

        if (throwExceptionInWaitForNotificationsPoint3 && state?.ToString() is "Starting to read.")
            throw new Exception();
    }
}