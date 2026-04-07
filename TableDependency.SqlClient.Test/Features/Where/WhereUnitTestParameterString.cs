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
using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Features.Where;

public class WhereUnitTestParameterString
{
    [Fact]
    public void UnitTestParameters1()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).EndsWith(par1);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW'", where);
    }

    [Fact]
    public void UnitTestParameters2()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).Contains(par1);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE '%WWW%'", where);
    }

    [Fact]
    public void UnitTestParameters3()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).StartsWith(par1);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) LIKE 'WWW%'", where);
    }

    [Fact]
    public void UnitTestParameters4()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3).Equals(par1);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) = 'WWW'", where);
    }

    [Fact]
    public void UnitTestParameters5()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToUpper().Substring(0, 3) == par1;

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("(SUBSTRING(UPPER(LTRIM(RTRIM([Code]))), 0, 3) = 'WWW')", where);
    }

    [Fact]
    public void UnitTestParameters6()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToLower().Substring(0, 3) == par1.ToLower();

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("(SUBSTRING(LOWER(LTRIM(RTRIM([Code]))), 0, 3) = LOWER('WWW'))", where);
    }

    [Fact]
    public void UnitTestParameters7()
    {
        const string par1 = "WWW";

        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Trim().ToLower().Substring(0, 3) == par1.Trim().ToLower().Substring(0, 3);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("(SUBSTRING(LOWER(LTRIM(RTRIM([Code]))), 0, 3) = SUBSTRING(LOWER(LTRIM(RTRIM('WWW'))), 0, 3))", where);
    }
}