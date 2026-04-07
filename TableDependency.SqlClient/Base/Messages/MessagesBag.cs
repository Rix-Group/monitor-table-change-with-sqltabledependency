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
using System.Text;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Exceptions;

namespace TableDependency.SqlClient.Base.Messages;

public sealed class MessagesBag(Encoding encoding, IList<string> startMessagesSignature, string endMessageSignature, ICollection<string> processableMessages)
{
    private readonly string _endMessageSignature = endMessageSignature;
    private readonly ICollection<string> _processableMessages = processableMessages;
    private readonly IList<string> _startMessagesSignature = startMessagesSignature;

    public Encoding Encoding { get; } = encoding;
    public ChangeType MessageType { get; private set; }
    public List<Message> Messages { get; } = [];
    public MessagesBagStatus Status { get; private set; } = MessagesBagStatus.Empty;
    public bool Ready => Status is MessagesBagStatus.Collecting;

    public void Reset()
        => Status = MessagesBagStatus.Empty;

    public MessagesBagStatus AddMessage(Message message)
    {
        if (_startMessagesSignature.Contains(message.MessageType))
        {
            if (Status != MessagesBagStatus.Empty)
                throw new MessageMisalignedException($"Received an StartMessege while current status is {Status}.");

            MessageType = GetMessageType(message.MessageType);
            Messages.Clear();
            return Status = MessagesBagStatus.Collecting;
        }

        if (message.MessageType == _endMessageSignature)
        {
            if (Status is not MessagesBagStatus.Collecting)
                throw new MessageMisalignedException($"Received an EndMessege while current status is {Status}.");

            return Status = MessagesBagStatus.Ready;
        }

        if (Status is MessagesBagStatus.Ready)
            throw new MessageMisalignedException($"Received {message.MessageType} message while current status is {MessagesBagStatus.Ready}.");

        if (!_processableMessages.Contains(message.MessageType))
            throw new MessageMisalignedException($"Queue containing a message type not expected [{message.MessageType}].");

        Messages.Add(message);

        return Status = MessagesBagStatus.Collecting;
    }

    private static ChangeType GetMessageType(string rawMessageType)
    {
        var messageChunk = rawMessageType.Split('/');
        return Enum.TryParse<ChangeType>(messageChunk[2], true, out var changeType)
            ? changeType
            : ChangeType.None;
    }
}