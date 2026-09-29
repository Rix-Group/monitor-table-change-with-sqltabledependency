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

public class WhereUnitTestMethodDispatch
{
    [Fact]
    public void Equals_TranslatesToEquality()
    {
        // ARRANGE
        Expression<Func<Product, bool>> expression = p => p.Code.Equals("123");

        // ACT
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // ASSERT
        Assert.Equal("[Code] = '123'", where);
    }

    [Fact]
    public void EndsWith_PrependsWildcardOnly()
    {
        // ARRANGE
        Expression<Func<Product, bool>> expression = p => p.Code.EndsWith("123");

        // ACT
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // ASSERT
        Assert.Equal("[Code] LIKE '%123'", where);
    }

    [Fact]
    public void StartsWith_AppendsWildcardOnly()
    {
        // ARRANGE
        Expression<Func<Product, bool>> expression = p => p.Code.StartsWith("123");

        // ACT
        var where = new SqlTableDependencyFilter<Product>(expression).Translate();

        // ASSERT
        Assert.Equal("[Code] LIKE '123%'", where);
    }

    [Fact]
    public void UnsupportedMethod_Throws()
    {
        // ARRANGE
        Expression<Func<Product, bool>> expression = p => p.Code.PadLeft(5) == "123";

        // ACT
        var exception = Record.Exception(() => new SqlTableDependencyFilter<Product>(expression).Translate());

        // ASSERT
        var notSupported = Assert.IsType<NotSupportedException>(exception);
        Assert.Equal("The method 'PadLeft' is not supported.", notSupported.Message);
    }
}