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
using System.Linq.Expressions;
using TableDependency.SqlClient.Test.Models;
using TableDependency.SqlClient.Where;

namespace TableDependency.SqlClient.Test.Features.Where;

public class WhereUnitTestContains
{
    [Fact]
    public void StringContains()
    {
        // Arrange
        Expression<Func<Product, bool>> expression = p => p.Code.Contains("123");

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("[Code] LIKE '%123%'", where);
    }

    [Fact]
    public void ColumnContainsNumbers()
    {
        var ids = new[] { 1, 2, 3 };

        // Arrange
        Expression<Func<Product, bool>> expression = p => ids.Contains(p.Id);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("[Id] IN (1,2,3)", where);
    }

    [Fact]
    public void ColumnContainsStrings()
    {
        var codes = new[] { "one", "two" };

        // Arrange
        Expression<Func<Product, bool>> expression = p => codes.Contains(p.Code);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("[Code] IN ('one','two')", where);
    }

    [Fact]
    public void ColumnContainsDecimals()
    {
        var prices = new[] { 123.45M, 432.10M };

        // Arrange
        Expression<Func<Product, bool>> expression = p => prices.Contains(p.Price);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("[Price] IN (123.45,432.10)", where);
    }

    [Fact]
    public void ColumnContainsFloats()
    {
        var prices = new[] { 123.45f, 432.10f };

        // Arrange
        Expression<Func<Product, bool>> expression = p => prices.Contains(p.ExcangeRate);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal($"[ExcangeRate] IN ({123.45},{432.1})", where);
    }

    [Fact]
    public void ColumnContainsDates()
    {
        var codes = new[] {
            DateTime.ParseExact("2010-05-18 14:40:52,531", "yyyy-MM-dd HH:mm:ss,fff", CultureInfo.InvariantCulture),
            DateTime.ParseExact("2009-05-18 14:40:52,531", "yyyy-MM-dd HH:mm:ss,fff", CultureInfo.InvariantCulture)
        };

        // Arrange
        Expression<Func<Product, bool>> expression = p => codes.Contains(p.ExpireDateTime);

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("[ExpireDateTime] IN ('2010-05-18T14:40:52','2009-05-18T14:40:52')", where);
    }
}