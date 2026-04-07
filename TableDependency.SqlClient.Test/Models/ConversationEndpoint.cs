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

namespace TableDependency.SqlClient.Test.Models;

public class ConversationEndpoint
{
    public Guid ConversationHandle { get; set; }
    public Guid ConversationId { get; set; }
    public int IsInitiator { get; set; }
    public int ServiceContractId { get; set; }
    public Guid ConversationGroupId { get; set; }
    public int ServiceId { get; set; }
    public DateTime Lifetime { get; set; }
    public string State { get; set; } = string.Empty;
    public string StateDesc { get; set; } = string.Empty;
    public string FarService { get; set; } = string.Empty;
    public string FarBrokerInstance { get; set; } = string.Empty;
    public int PrincipalId { get; set; }
    public int FarPrincipalId { get; set; }
    public Guid OutboundSessionKeyIdentifier { get; set; }
    public Guid InboundSessionKeyIdentifier { get; set; }
    public DateTime SecurityTimestamp { get; set; }
    public DateTime DialogTimer { get; set; }
}