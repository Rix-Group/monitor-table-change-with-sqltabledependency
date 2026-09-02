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
using System;
using System.Diagnostics;
using System.Linq;

namespace TableDependency.SqlClient;

/// <summary>
/// Writes one record to both sinks the library exposes: the caller's <see cref="ILogger"/> and an <see cref="ActivityEvent"/> on the
/// current span. Not tied to <see cref="SqlTableDependency{T}"/> so anything in the library - the receive loop's <see cref="SpinGuard"/>
/// included - can report through the same pair.
/// </summary>
internal static class Telemetry
{
    internal static void Log(ILogger? logger, LogLevel level, Exception? exception, string template, (string Name, object? Value)[] values)
    {
        logger?.Log(level, exception, template, [.. values.Select(v => v.Value)]);

        // The span event is a second sink for the same record, so it honours the same level. Without this a Debug line still
        // reaches the telemetry backend, because the WAITFOR span is started with an always-Recorded parent and nothing downstream
        // can sample it away - making the loop's per-iteration Debug lines unsuppressable by Logging:LogLevel.
        if (logger?.IsEnabled(level) is false)
            return;

        var activity = Activity.Current;
        if (activity is null)
            return;

        var tags = new ActivityTagsCollection { { "log.level", level.ToString() } };

        if (exception is not null)
        {
            tags.Add("exception.type", exception.GetType().FullName ?? "unknown");
            tags.Add("exception.message", exception.Message);
        }

        foreach (var (name, value) in values)
            tags[name] = value ?? "null";

        var eventName = template;
        foreach (var (name, value) in values)
            eventName = eventName.Replace($"{{{name}}}", value?.ToString() ?? "null");

        activity.AddEvent(new ActivityEvent(eventName, tags: tags));
    }
}