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
using System.Dynamic;
using System.Globalization;
using TableDependency.SqlClient.Base.Interfaces;
using TableDependency.SqlClient.Base.Messages;

namespace TableDependency.SqlClient.Base.EventArgs;

public sealed class ExpandoRecordChangedEventArgs(MessagesBag messagesBag, string server, string database, string sender, CultureInfo cultureInfo, bool includeOldEntity = false)
    : RecordChangedEventArgs<ExpandoObject>(messagesBag, null, server, database, sender, cultureInfo, includeOldEntity)
{
    protected override ExpandoObject MaterializeEntity(List<Message> messages, IModelToTableMapper<ExpandoObject>? mapper)
    {
        var eo = new ExpandoObject();
        foreach (var message in messages)
            eo.TryAdd(message.Recipient, message.Body?.Length is null or 0 ? null : Convert.ToString(_messagesBag!.Encoding.GetString(message.Body), CultureInfo));

        return eo;
    }
}