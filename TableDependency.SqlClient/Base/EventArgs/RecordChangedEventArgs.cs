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
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;
using TableDependency.SqlClient.Extensions;

namespace TableDependency.SqlClient.Base.EventArgs;

public class RecordChangedEventArgs<T> : BaseEventArgs where T : class, new()
{
    #region Instance variables

    protected readonly MessagesBag? _messagesBag;
    private readonly PropertyInfo[] _entityPropertiesInfo = [];

    #endregion

    #region Properties

    public T Entity { get; }
    public T? OldEntity { get; }
    public ChangeType ChangeType { get; }

    #endregion

    #region Constructors

    /// <summary>
    /// Create a record changed event arg for test purposes
    /// </summary>
    /// <param name="cultureInfo">Defaults to current culture</param>
    public RecordChangedEventArgs(ChangeType changeType, T entity, T? oldEntity = null, string server = "", string database = "", string sender = "", CultureInfo? cultureInfo = null)
        : base(server, database, sender, cultureInfo ?? CultureInfo.CurrentCulture)
    {
        ChangeType = changeType;
        Entity = entity;
        OldEntity = oldEntity;
    }

    public RecordChangedEventArgs(
        MessagesBag messagesBag,
        IModelToTableMapper<T>? mapper,
        string server,
        string database,
        string sender,
        CultureInfo cultureInfo,
        bool includeOldEntity = false) : base(server, database, sender, cultureInfo)
    {
        _messagesBag = messagesBag;
        _entityPropertiesInfo = [.. ModelUtil.GetModelPropertiesInfo<T>()];

        ChangeType = messagesBag.MessageType;
        Entity = MaterializeEntity([.. messagesBag.Messages.Where(m => !m.IsOldValue)], mapper);

        if (includeOldEntity && ChangeType is ChangeType.Update)
            OldEntity = MaterializeEntity([.. messagesBag.Messages.Where(m => m.IsOldValue)], mapper);
    }

    #endregion

    #region Public methods

    public object? GetValue(PropertyInfo propertyInfo, byte[]? message)
    {
        if (message?.Length is null or 0)
            return null;

        if (propertyInfo.PropertyType.GetTypeInfo().IsEnum)
        {
            var stringValue = Encoding.Unicode.GetString(message);
            var value = Enum.Parse(propertyInfo.PropertyType, stringValue);
            return value.GetHashCode();
        }

        if (propertyInfo.PropertyType == typeof(byte[]))
            return message;

        if (propertyInfo.PropertyType == typeof(bool) || propertyInfo.PropertyType == typeof(bool?))
            return Encoding.Unicode.GetString(message).ToBoolean();

        if (propertyInfo.PropertyType == typeof(char[]))
            return Encoding.Unicode.GetString(message).ToCharArray();

        return GetValueObject(propertyInfo, message ?? []);
    }

    #endregion

    #region Private Methods

    private object? GetValueObject(PropertyInfo propertyInfo, byte[] message)
    {
        var value = Convert.ToString(_messagesBag!.Encoding.GetString(message), CultureInfo);
        var propertyType = Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType;
        var typeCode = Type.GetTypeCode(propertyType);

        try
        {
            switch (typeCode)
            {
                case TypeCode.Boolean:
                    return bool.Parse(value);

                case TypeCode.Char:
                    return char.Parse(value);

                case TypeCode.SByte:
                    return sbyte.Parse(value, CultureInfo);

                case TypeCode.Byte:
                    return byte.Parse(value, CultureInfo);

                case TypeCode.Int16:
                    return short.Parse(value, CultureInfo);

                case TypeCode.UInt16:
                    return ushort.Parse(value, CultureInfo);

                case TypeCode.Int32:
                    return int.Parse(value, CultureInfo);

                case TypeCode.UInt32:
                    return uint.Parse(value, CultureInfo);

                case TypeCode.Int64:
                    return long.Parse(value, CultureInfo);

                case TypeCode.UInt64:
                    return ulong.Parse(value, CultureInfo);

                case TypeCode.Single:
                    return float.Parse(value, CultureInfo);

                case TypeCode.Double:
                    return double.Parse(value, CultureInfo);

                case TypeCode.Decimal:
                    return decimal.Parse(value, CultureInfo);

                case TypeCode.DateTime:
                    return DateTime.Parse(value, CultureInfo);

                case TypeCode.String:
                    return value;

                case TypeCode.Object:
                    if (Guid.TryParse(value, out var guid))
                        return guid;

                    if (TimeSpan.TryParse(value, out var timeSpan))
                        return timeSpan;

                    if (DateTimeOffset.TryParse(value, out var dateTimeOffset))
                        return dateTimeOffset;

                    break;
            }
        }
        catch
        {
            var errorMessage = $"Propery {propertyInfo.Name} cannot be set with db value {value}";
            throw new NoMatchBetweenModelAndTableColumns(errorMessage);
        }

        return null;
    }

    protected virtual T MaterializeEntity(List<Message> messages, IModelToTableMapper<T>? mapper)
    {
        var entity = new T();

        foreach (var entityPropertyInfo in _entityPropertiesInfo)
        {
            var propertyMappedTo = mapper?.GetMapping(entityPropertyInfo);
            var columnName = propertyMappedTo ?? entityPropertyInfo.Name;

            var message = messages.FirstOrDefault(m => string.Equals(m.Recipient, columnName, StringComparison.CurrentCultureIgnoreCase));
            if (message is null)
                continue;

            var value = GetValue(entityPropertyInfo, message.Body);
            SetValue(entity, entityPropertyInfo.Name, value);
        }

        return entity;
    }

    private static void SetValue(object inputObject, string propertyName, object? propertyVal)
    {
        var type = inputObject.GetType();
        var propertyInfo = type.GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        if (propertyInfo is not null && propertyVal is not null)
        {
            var setMethod = propertyInfo.SetMethod;
            if (setMethod is null || (!setMethod.IsPublic && !setMethod.IsAssembly))
                return;

            Type propertyType = propertyInfo.PropertyType;

            var targetType = IsNullableType(propertyType)
                ? Nullable.GetUnderlyingType(propertyType)
                : propertyType;

            if (targetType is not null)
            {
                propertyVal = targetType.IsEnum
                    ? Enum.ToObject(targetType, propertyVal)
                    : Convert.ChangeType(propertyVal, targetType);

                propertyInfo.SetValue(inputObject, propertyVal, null);
            }
        }
    }

    private static bool IsNullableType(Type type)
        => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);

    #endregion
}