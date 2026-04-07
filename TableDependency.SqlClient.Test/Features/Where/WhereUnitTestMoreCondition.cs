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

public class WhereUnitTestMoreCondition
{
    [Fact]
    public void MoreConditions1()
    {
        var ids = new[] { 1, 2, 3 };

        // Arrange
        Expression<Func<Product, bool>> expression = p => ids.Contains(p.Id) && p.Code.Trim().Substring(0, 3).Equals("WWW");

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("([Id] IN (1,2,3) AND SUBSTRING(LTRIM(RTRIM([Code])), 0, 3) = 'WWW')", where);
    }

    [Fact]
    public void MoreConditions2()
    {
        var ids = new[] { 1, 2, 3 };

        // Arrange
        Expression<Func<Product, bool>> expression = p =>
            ids.Contains(p.Id) &&
            p.Code.Trim().Substring(0, 3).Equals("WWW") &&
            p.Id == 100;

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("(([Id] IN (1,2,3) AND SUBSTRING(LTRIM(RTRIM([Code])), 0, 3) = 'WWW') AND ([Id] = 100))", where);
    }

    [Fact]
    public void MoreConditions3()
    {
        var ids = new[] { 1 };

        // Arrange
        Expression<Func<Product, bool>> expression = p =>
            ids.Contains(p.Id) ||
            (p.Code.Equals("WWW") && p.Code.Substring(0, 3) == "22");

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("([Id] IN (1) OR ([Code] = 'WWW' AND (SUBSTRING([Code], 0, 3) = '22')))", where);
    }

    [Fact]
    public void MoreConditions4()
    {
        var ids = new[] { 1 };

        // Arrange
        Expression<Func<Product, bool>> expression = p =>
            ids.Contains(p.Id) ||
            p.Code.Equals("WWW") &&
            p.Code.Substring(0, 3) == "22" ||
            p.ExcangeRate > 1;

        // Act
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // Assert
        Assert.Equal("(([Id] IN (1) OR ([Code] = 'WWW' AND (SUBSTRING([Code], 0, 3) = '22'))) OR ([ExcangeRate] > 1))", where);
    }

    [Fact]
    public void MoreConditions5()
    {
        // 1 OR 0 AND 0 => 0
        // (1 OR 0) AND 0 => 0
        // 1 OR (0 AND 0) => 1

        // Arrange
        Expression<Func<Product, bool>> expression1 = p => p.Id == 1 || p.Id == 0 && p.Id == 0;
        Expression<Func<Product, bool>> expression2 = p => (p.Id == 1 || p.Id == 0) && p.Id == 0;
        Expression<Func<Product, bool>> expression3 = p => p.Id == 1 || (p.Id == 0 && p.Id == 0);

        // Act
        var where1 = new SqlTableDependencyFilter<Product>(expression1).Translate();
        var where2 = new SqlTableDependencyFilter<Product>(expression2).Translate();
        var where3 = new SqlTableDependencyFilter<Product>(expression3).Translate();

        // Assert
        Assert.Equal("(([Id] = 1) OR (([Id] = 0) AND ([Id] = 0)))", where1);
        Assert.Equal("((([Id] = 1) OR ([Id] = 0)) AND ([Id] = 0))", where2);
        Assert.Equal("(([Id] = 1) OR (([Id] = 0) AND ([Id] = 0)))", where3);
    }
}