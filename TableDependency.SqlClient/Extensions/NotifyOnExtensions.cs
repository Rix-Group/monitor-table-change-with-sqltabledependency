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
using TableDependency.SqlClient.Base.Enums;

namespace TableDependency.SqlClient.Extensions;

internal static class NotifyOnExtensions
{
    extension(NotifyOn notifyOn)
    {
        internal IEnumerable<string> GetDmlTriggerTypes()
        {
            if (notifyOn.HasFlag(NotifyOn.All))
                return [
                    NotifyOn.Insert.ToString().ToLowerInvariant(),
                    NotifyOn.Update.ToString().ToLowerInvariant(),
                    NotifyOn.Delete.ToString().ToLowerInvariant()
                ];

            return [..Enum.GetValues<NotifyOn>()
                .Where(t => notifyOn.HasFlag(t))
                .Select(t => t.ToString().ToLowerInvariant())];
        }
    }
}