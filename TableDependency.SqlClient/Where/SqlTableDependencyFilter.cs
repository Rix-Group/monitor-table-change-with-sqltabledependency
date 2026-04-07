#region License

// TableDependency, SqlTableDependency, SqlTableDependencyFilter
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
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Extensions;
using TableDependency.SqlClient.Where.Helpers;

namespace TableDependency.SqlClient.Where;

public sealed class SqlTableDependencyFilter<T> : ExpressionVisitor, ITableDependencyFilter where T : class, new()
{
    #region Constructors

    private static readonly HashSet<Type> AllowedTypes =
    [
        typeof(short),
        typeof(short?),
        typeof(int),
        typeof(int?),
        typeof(long),
        typeof(long?),
        typeof(string),
        typeof(decimal),
        typeof(decimal?),
        typeof(float),
        typeof(float?),
        typeof(DateTime),
        typeof(DateTime?),
        typeof(double),
        typeof(double?),
        typeof(bool),
        typeof(bool?)
    ];

    private readonly ParameterHelper _parameter = new();
    private readonly Expression _filter;
    private readonly IDictionary<string, string>? _modelMapperDictionary;

    private readonly StringBuilder _whereConditionBuilder = new();

    #endregion

    #region Constructors

    public SqlTableDependencyFilter(Expression<Func<T, bool>> filter, IModelToTableMapper<T>? mapper = null)
    {
        _filter = filter;

        mapper = mapper?.Mappings.Count > 0
            ? mapper
            : CreateModelToTableMapperHelper();

        _modelMapperDictionary = mapper?.Mappings.ToDictionary();
    }

    #endregion

    #region Public Methods

    public string Translate()
    {
        if (_whereConditionBuilder.Length > 0)
            return _whereConditionBuilder.ToString().Trim();

        Visit(_filter);
        return _whereConditionBuilder.ToString().Trim();
    }

    #endregion

    #region Protected Methods

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(Queryable) && node.Method.Name is "Where")
            throw new ArgumentException("Cannot translate where of type Queryable.");

        #region Trim

        if (node.Method.Name is "Trim")
        {
            _whereConditionBuilder.Append("LTRIM(RTRIM(");
            Visit(node.Object);
            _whereConditionBuilder.Append("))");

            return node;
        }

        #endregion

        #region StartsWith

        if (node.Method.Name is "StartsWith")
        {
            Visit(node.Object);
            _whereConditionBuilder.Append(" LIKE ");

            _parameter.Append = "%";
            Visit(node.Arguments[0]);
            return node;
        }

        #endregion

        #region EndsWith

        if (node.Method.Name is "EndsWith")
        {
            Visit(node.Object);
            _whereConditionBuilder.Append(" LIKE ");

            _parameter.Prepend = "%";
            Visit(node.Arguments[0]);
            return node;
        }

        #endregion

        #region Contains

        if (node.Method.Name is "Contains")
        {
            if (node.Object is null)
            {
                // Enumerable.Contains(collection, item) -> translate to SQL: [column] IN (val1, val2, ...)
                // arguments: [0] = collection, [1] = item
                Visit(node.Arguments[1]);
                _whereConditionBuilder.Append(" IN ");

                // Evaluate the collection expression to get the values
                var methodCallExpression = (MethodCallExpression)node.Arguments[0];
                var memberExpression = (MemberExpression)methodCallExpression.Arguments[0];

                var lambda = Expression.Lambda(memberExpression);
                var getter = lambda.Compile();
                var collectionObj = getter.DynamicInvoke();

                if (collectionObj is not IEnumerable values)
                    throw new ArgumentException("The first argument of Contains must be an IEnumerable.");

                _whereConditionBuilder.Append(FormatInValues(values));
            }
            else
            {
                Visit(node.Object);
                _whereConditionBuilder.Append(" LIKE ");

                _parameter.Prepend = "%";
                _parameter.Append = "%";
                Visit(node.Arguments[0]);
            }

            return node;
        }

        #endregion

        #region TrimStart

        if (node.Method.Name is "TrimStart")
        {
            _whereConditionBuilder.Append("LTRIM(");
            Visit(node.Object);
            _whereConditionBuilder.Append(')');

            return node;
        }

        #endregion

        #region TrimEnd

        if (node.Method.Name is "TrimEnd")
        {
            _whereConditionBuilder.Append("RTRIM(");
            Visit(node.Object);
            _whereConditionBuilder.Append(')');

            return node;
        }

        #endregion

        #region ToUpper

        if (node.Method.Name is "ToUpper")
        {
            _whereConditionBuilder.Append("UPPER(");
            Visit(node.Object);
            _whereConditionBuilder.Append(')');

            return node;
        }

        #endregion

        #region ToLower

        if (node.Method.Name is "ToLower")
        {
            _whereConditionBuilder.Append("LOWER(");
            Visit(node.Object);
            _whereConditionBuilder.Append(')');

            return node;
        }

        #endregion

        #region Substring

        if (node.Method.Name is "Substring")
        {
            _whereConditionBuilder.Append("SUBSTRING(");
            Visit(node.Object);

            var startParameter = (ConstantExpression)node.Arguments[0];
            if (!int.TryParse(startParameter?.Value?.ToString(), out int intResult))
                throw new ArgumentNullException(nameof(node), "Could not parse arguments[0] as an int.");
            _whereConditionBuilder.Append(", " + intResult);

            var lenParameter = (ConstantExpression)node.Arguments[1];
            if (!int.TryParse(lenParameter?.Value?.ToString(), out intResult))
                throw new ArgumentNullException(nameof(node), "Could not parse arguments[1] as an int.");
            _whereConditionBuilder.Append(", " + intResult + ")");

            return node;
        }

        #endregion

        #region ToString

        if (node.Method.Name is "ToString")
        {
            _whereConditionBuilder.Append("CONVERT(varchar(MAX), ");
            Visit(node.Object);
            _whereConditionBuilder.Append(')');

            return node;
        }

        #endregion

        #region Equals

        if (node.Method.Name is "Equals" && node.Object is not null)
        {
            Visit(node.Object);
            _whereConditionBuilder.Append(" = ");
            Visit(node.Arguments[0]);

            return node;
        }

        #endregion

        throw new NotSupportedException($"The method '{node.Method.Name}' is not supported.");
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        switch (node.NodeType)
        {
            case ExpressionType.Not:
                _whereConditionBuilder.Append(" NOT ");
                Visit(node.Operand);
                break;

            case ExpressionType.Convert:
                Visit(node.Operand);
                break;

            default:
                throw new NotSupportedException($"The unary operator '{node.NodeType}' is not supported.");
        }

        return node;
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        _whereConditionBuilder.Append('(');

        Visit(node.Left);

        switch (node.NodeType)
        {
            case ExpressionType.And or ExpressionType.AndAlso:
                _whereConditionBuilder.Append(" AND ");
                break;

            case ExpressionType.Or or ExpressionType.OrElse:
                _whereConditionBuilder.Append(" OR ");
                break;

            case ExpressionType.Equal:
                _whereConditionBuilder.Append(IsNullConstant(node.Right) ? " IS " : " = ");
                break;

            case ExpressionType.NotEqual:
                _whereConditionBuilder.Append(IsNullConstant(node.Right) ? " IS NOT " : " <> ");
                break;

            case ExpressionType.LessThan:
                _whereConditionBuilder.Append(" < ");
                break;

            case ExpressionType.LessThanOrEqual:
                _whereConditionBuilder.Append(" <= ");
                break;

            case ExpressionType.GreaterThan:
                _whereConditionBuilder.Append(" > ");
                break;

            case ExpressionType.GreaterThanOrEqual:
                _whereConditionBuilder.Append(" >= ");
                break;

            default:
                throw new NotSupportedException($"The binary operator '{node.NodeType}' is not supported.");
        }

        Visit(node.Right);

        _whereConditionBuilder.Append(')');

        return node;
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        var q = node.Value as IQueryable;
        if (q is null && node.Value is null)
        {
            _whereConditionBuilder.Append("NULL");
        }
        else if (q is null && node.Value is not null)
        {
            switch (Type.GetTypeCode(node.Value.GetType()))
            {
                case TypeCode.Boolean:
                    _whereConditionBuilder.Append(ToSqlFormat(node.Value.GetType(), node.Value));
                    break;

                case TypeCode.String:
                    _whereConditionBuilder.Append('\'');
                    _whereConditionBuilder.Append(_parameter.Prepend);
                    _whereConditionBuilder.Append(ToSqlFormat(node.Value.GetType(), node.Value));
                    _whereConditionBuilder.Append(_parameter.Append);
                    _whereConditionBuilder.Append('\'');
                    break;

                case TypeCode.Decimal or TypeCode.Double:
                    _whereConditionBuilder.Append(ToSqlFormat(node.Value.GetType(), node.Value));
                    break;

                case TypeCode.DateTime:
                    _whereConditionBuilder.Append('\'');
                    _whereConditionBuilder.Append(ToSqlFormat(node.Value.GetType(), node.Value));
                    _whereConditionBuilder.Append('\'');
                    break;

                case TypeCode.Object:
                    var fieldInfos = node.Type.GetFields(BindingFlags.Public | BindingFlags.Instance);
                    if (typeof(IEnumerable).IsAssignableFrom(fieldInfos[0].FieldType))
                    {
                        var values = (IEnumerable?)fieldInfos[0].GetValue(node.Value);
                        _whereConditionBuilder.Append(FormatInValues(values));
                    }
                    else
                    {
                        throw new NotSupportedException($"The constant for '{node.Value}' is not supported");
                    }

                    break;

                default:
                    _whereConditionBuilder.Append(ToSqlFormat(node.Value.GetType(), node.Value));
                    break;
            }
        }

        return node;
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Expression is ConstantExpression)
        {
            var lambda = Expression.Lambda(node);
            var fn = lambda.Compile();
            return Visit(Expression.Constant(fn.DynamicInvoke(null), node.Type));
        }

        if (node.Expression is MemberExpression)
            throw new NotSupportedException("Cannot manage complex properties.");

        _whereConditionBuilder.Append($"[{GetDatabaseColumnName(node.Member.Name)}]");

        return node;
    }

    #endregion

    #region Private Methods

    private static string FormatInValues(IEnumerable? values)
    {
        if (values is null)
            return "()";

        var valuesToPrint = new List<string>();
        Type? previousType = null;

        foreach (var value in values)
        {
            if (value is null)
                throw new ArgumentException("Null values are not supported in Contains.");

            var valueType = value.GetType();
            if (!AllowedTypes.Contains(valueType))
                throw new ArgumentException("Type not supported.");
            if (previousType is not null && previousType != valueType)
                throw new ArgumentException("Type mismatch.");

            var quotes = Quotes(valueType);
            valuesToPrint.Add($"{quotes}{ToSqlFormat(valueType, value)}{quotes}");
            previousType = valueType;
        }

        return "(" + string.Join(",", valuesToPrint) + ")";
    }

    private static ModelToTableMapper<T>? CreateModelToTableMapperHelper()
    {
        var modelPropertyInfosWithColumnAttribute = typeof(T)
            .GetProperties(BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(x => x.IsPublicOrInternal() && CustomAttributeExtensions.IsDefined(x, typeof(ColumnAttribute), false))
            .ToArray();

        if (modelPropertyInfosWithColumnAttribute.Length is 0)
            return null;

        var mapper = new ModelToTableMapper<T>();
        foreach (var propertyInfo in modelPropertyInfosWithColumnAttribute)
        {
            var attribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();
            var dbColumnName = string.IsNullOrWhiteSpace(attribute?.Name)
                ? propertyInfo.Name
                : attribute.Name;
            mapper.AddMapping(propertyInfo, dbColumnName);
        }

        return mapper;
    }

    private string GetDatabaseColumnName(string memberName)
    {
        if (_modelMapperDictionary?.Any() is not true)
            return memberName;
        var mapping = _modelMapperDictionary.FirstOrDefault(mm => mm.Key.Equals(memberName, StringComparison.InvariantCultureIgnoreCase));

        return default(KeyValuePair<string, string>).Equals(mapping)
            ? memberName
            : mapping.Value.Replace("[", string.Empty).Replace("]", string.Empty);
    }

    private static string? ToSqlFormat(Type type, object value)
    {
        if (type == typeof(bool))
            return (bool)value ? "1" : "0";

        if (type == typeof(string))
            return value.ToString();

        if (type == typeof(decimal))
            return Convert.ToDecimal(value).ToString("g", CultureInfo.InvariantCulture);

        if (type == typeof(double))
            return Convert.ToDouble(value).ToString("g", CultureInfo.InvariantCulture);

        if (type == typeof(DateTime))
            return Convert.ToDateTime(value).ToString("s", CultureInfo.InvariantCulture);

        return value.ToString();
    }

    private static string Quotes(Type type)
        => type == typeof(string) || type == typeof(DateTime) ? "'" : string.Empty;

    private static bool IsNullConstant(Expression exp)
        => exp.NodeType is ExpressionType.Constant && ((ConstantExpression)exp).Value is null;

    #endregion
}