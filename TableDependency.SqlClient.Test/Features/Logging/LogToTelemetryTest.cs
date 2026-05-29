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

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace TableDependency.SqlClient.Test.Features.Logging;

public class LogToTelemetryTest
{
    private sealed class SampleModel;

    [Fact]
    public void NamedPlaceholderTemplate_RendersEventName_WithoutThrowing()
    {
        // ARRANGE
        using var activity = new Activity("test");
        activity.Start();
        (string Name, object? Value)[] values = [("NamingPrefix", "TblDep")];

        // ACT
        SqlTableDependency<SampleModel>.LogToTelemetry(null, LogLevel.Debug, null, "Queue {NamingPrefix}_Receiver created.", values);

        // ASSERT
        var recordedEvent = Assert.Single(activity.Events);
        Assert.Equal("Queue TblDep_Receiver created.", recordedEvent.Name);
    }
}