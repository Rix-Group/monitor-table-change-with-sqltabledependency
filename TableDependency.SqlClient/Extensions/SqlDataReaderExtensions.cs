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

using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;

namespace TableDependency.SqlClient.Extensions;

public static class SqlDataReaderExtensions
{
    extension (SqlDataReader reader)
    {
        public string? GetSafeString(int columnIndex)
        {
            if (reader.IsDBNull(columnIndex))
                return null;

            return reader.GetValue(columnIndex)?.ToString();
        }

        public IEnumerable<Dictionary<string, object>> Serialize()
        {
            var cols = Enumerable.Range(0, reader.FieldCount)
                .Select(reader.GetName)
                .ToArray();

            while (reader.Read())
                yield return cols.ToDictionary(c => c, c => reader[c]);
        }
    }
}