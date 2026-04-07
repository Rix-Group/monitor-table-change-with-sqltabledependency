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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Extensions;

namespace TableDependency.SqlClient;

public sealed class ModelToTableMapper<T> : IModelToTableMapper<T> where T : class
{
    public IReadOnlyDictionary<string, string> Mappings => _mappings.AsReadOnly();
    private readonly Dictionary<string, string> _mappings = [];

    public ModelToTableMapper<T> AddMapping(PropertyInfo pi, string columnName)
    {
        if (_mappings.Values.Any(cn => cn == columnName))
            throw new ModelToTableMapperException("Duplicate mapping for column " + columnName);

        _mappings[pi.Name] = columnName;
        return this;
    }

    public ModelToTableMapper<T> AddMapping(Expression<Func<T, object>> expression, string columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
            throw new ModelToTableMapperException("ModelToTableMapper cannot contains null or empty strings.");

        var propertyInfo = expression.GetPropertyInfo();
        _mappings[propertyInfo.Name] = columnName;
        return this;
    }

    public string? GetMapping(string tableColumnName)
        => _mappings.Values.FirstOrDefault(value => value.Equals(tableColumnName, StringComparison.OrdinalIgnoreCase));

    public string? GetMapping(PropertyInfo propertyInfo)
        => _mappings.TryGetValue(propertyInfo.Name, out var value)
            ? value
            : null;
}