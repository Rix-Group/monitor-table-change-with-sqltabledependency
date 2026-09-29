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

namespace TableDependency.SqlClient.Test.Features.Misc;

public class RemoveLogOperationsTest
{
    [Fact]
    public void RemovesEveryLogStatementAndKeepsTheRest()
    {
        // ARRANGE
        const string script = "BEGIN\r\n    PRINT N'SqlTableDependency: First [a].';\r\n    SELECT 1;\r\n    PRINT N'SqlTableDependency: Second.';\r\nEND";

        // ACT
        var result = SqlTableDependency<object>.RemoveLogOperations(script);

        // ASSERT
        Assert.Equal("BEGIN\r\n    \r\n    SELECT 1;\r\n    \r\nEND", result);
    }

    [Fact]
    public void LeavesUnterminatedLogStatementUntouched()
    {
        // ARRANGE
        const string script = "BEGIN PRINT N'SqlTableDependency: never closed";

        // ACT
        var result = SqlTableDependency<object>.RemoveLogOperations(script);

        // ASSERT
        Assert.Equal(script, result);
    }
}