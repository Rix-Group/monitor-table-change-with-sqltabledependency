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

using TableDependency.SqlClient.Base.Utilities;

namespace TableDependency.SqlClient.Extensions;

public static class TableColumnInfoExtensions
{
    extension(TableColumnInfo column)
    {
        public bool IsRowVersionOrTimestamp()
            => column.Type.ToLowerInvariant() is "rowversion" or "timestamp";

        public bool IsLegacyTextOrImage()
            => column.Type.ToLowerInvariant() is "text" or "ntext" or "image";

        public string GetVariableSqlType()
        {
            var type = column.Type.ToLowerInvariant();

            return type switch
            {
                "text" => "varchar(max)",
                "ntext" => "nvarchar(max)",
                "image" => "varbinary(max)",
                "json" or "vector" => "nvarchar(max)",
                "sysname" => "nvarchar(128)",
                _ => type + (string.IsNullOrWhiteSpace(column.Size) ? string.Empty : $"({column.Size})")
            };
        }

        public string GetTableVariableSqlType()
            => column.Type.ToLowerInvariant() switch
            {
                "timestamp" => "BINARY(8)",
                "rowversion" => "VARBINARY(8)",
                "text" => "VARCHAR(MAX)",
                "ntext" or "xml" or "geography" or "geometry" or "json" or "vector" => "NVARCHAR(MAX)",
                "image" => "VARBINARY(MAX)",
                "sysname" => "NVARCHAR(128)",
                _ => string.IsNullOrWhiteSpace(column.Size) ? column.Type : $"{column.Type}({column.Size})"
            };

        public string PrepareColumnValueForInsertedDeletedTable()
            => column.Type.ToLowerInvariant() switch
            {
                "text" => $"CONVERT(VARCHAR(MAX), [{column.Name}])",
                "ntext" or "xml" or "geography" or "geometry" or "json" or "vector" => $"CONVERT(NVARCHAR(MAX), [{column.Name}])",
                "image" => $"CONVERT(VARBINARY(MAX), [{column.Name}])",
                _ => $"[{column.Name}]"
            };
    }
}