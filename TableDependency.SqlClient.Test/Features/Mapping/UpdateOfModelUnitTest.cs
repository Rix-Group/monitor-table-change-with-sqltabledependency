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
using TableDependency.SqlClient.Base.Exceptions;

namespace TableDependency.SqlClient.Test.Features.Mapping;

public class UpdateOfModelUnitTest
{
    private sealed class SampleModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void Add_AddsPropertyInfos()
    {
        var updateOf = new UpdateOfModel<SampleModel>();

        updateOf.Add(model => model.Id, model => model.Name);

        Assert.Collection(updateOf.PropertyInfo,
            p => Assert.Equal(nameof(SampleModel.Id), p.Name),
            p => Assert.Equal(nameof(SampleModel.Name), p.Name));
    }

    [Fact]
    public void Add_EmptyArray_Throws()
    {
        var updateOf = new UpdateOfModel<SampleModel>();

        var ex = Assert.Throws<UpdateOfModelException>(() => updateOf.Add());
        Assert.Equal("UpdateOfModel cannot be empty.", ex.Message);
    }

    [Fact]
    public void Add_InvalidExpression_Throws()
    {
        var updateOf = new UpdateOfModel<SampleModel>();
        Expression<Func<SampleModel, object>> expression = model => new { model.Id };

        var ex = Assert.Throws<UpdateOfModelException>(() => updateOf.Add(expression));
        Assert.Equal("'expression' parameter should be a member expression.", ex.Message);
    }
}