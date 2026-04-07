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

namespace TableDependency.SqlClient.Base.Interfaces;

/// <summary>
/// Model to column database table mapper.
/// </summary>
public interface IModelToTableMapper<T> where T : class
{
    public IReadOnlyDictionary<string, string> Mappings { get; }

    /// <summary>
    /// Adds the mapping.
    /// </summary>
    /// <param name="pi">The pi.</param>
    /// <param name="columnName">Name of the column.</param>
    public ModelToTableMapper<T> AddMapping(PropertyInfo pi, string columnName);

    /// <summary>
    /// Adds the mapping between a model property and a database table column.
    /// </summary>
    /// <param name="expression">The expression.</param>
    /// <param name="columnName">Name of the column.</param>
    public ModelToTableMapper<T> AddMapping(Expression<Func<T, object>> expression, string columnName);

    /// <summary>
    /// Gets the mapping.
    /// </summary>
    /// <param name="tableColumnName">Name of the table column.</param>
    public string? GetMapping(string tableColumnName);

    /// <summary>
    /// Gets the mapping.
    /// </summary>
    /// <param name="propertyInfo">The property information.</param>
    public string? GetMapping(PropertyInfo propertyInfo);
}