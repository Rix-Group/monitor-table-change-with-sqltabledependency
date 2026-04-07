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

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Extensions;

namespace TableDependency.SqlClient;

public sealed class UpdateOfModel<T> : IUpdateOfModel<T> where T : class
{
    public IReadOnlyList<PropertyInfo> PropertyInfo => _propertyInfo.AsReadOnly();
    private readonly IList<PropertyInfo> _propertyInfo = [];

    public void Add(params Expression<Func<T, object>>[] expressions)
    {
        if (expressions?.Length is not > 0)
            throw new UpdateOfModelException("UpdateOfModel cannot be empty.");

        foreach (var expression in expressions)
            _propertyInfo.Add(expression.GetPropertyInfo("'expression' parameter should be a member expression."));
    }
}