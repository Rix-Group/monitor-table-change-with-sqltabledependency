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

using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace TableDependency.SqlClient;

/// <summary>
/// Guards the receive loop against its own spin. A WAITFOR that comes back empty well before its TIMEOUT is not waiting - the loop
/// is spinning against the broker. The guard warns (at Warning, so the fault is visible when the loop's Debug lines are not) and
/// backs off, so a spin costs a bounded number of round trips instead of hammering the queue for as long as the condition lasts.
/// State is carried across iterations by the instance; the arithmetic is in the static members so it can be asserted directly.
/// </summary>
internal sealed class SpinGuard
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);

    /// <summary>Once the backoff is capped it stops changing, so without a reminder a spin lasting hours would warn only in its first minutes.</summary>
    private static readonly TimeSpan WarningReminderInterval = TimeSpan.FromMinutes(10);

    /// <summary>Headroom over watchdogTimeout, so a capped iteration comfortably outlasts the conversation timer armed at its start.</summary>
    private const int WatchdogHeadroomSeconds = 10;

    private readonly ILogger? _logger;
    private readonly int _timeout;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;

    private TimeSpan _backoff;
    private TimeSpan _sinceLastWarning;

    internal SpinGuard(ILogger? logger, int timeout, int watchdogTimeout, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _logger = logger;
        _timeout = timeout;

        // The receive command re-arms BEGIN CONVERSATION TIMER on every iteration, which replaces the pending timer, so while the
        // loop spins the DialogTimer never fires and the stale dialog is never retired - the spin cannot clear itself. Capping the
        // backoff above watchdogTimeout makes a throttled iteration outlast that timer, so it fires and the dialog gets torn down.
        BackoffMax = TimeSpan.FromSeconds(watchdogTimeout + WatchdogHeadroomSeconds);
        _delay = delay ?? Task.Delay;
    }

    /// <summary>The backoff in force, or <see cref="TimeSpan.Zero"/> while the loop is healthy.</summary>
    internal TimeSpan Backoff => _backoff;

    internal TimeSpan BackoffMax { get; }

    internal async Task ThrottleAsync(bool receivedDataMessage, TimeSpan elapsed, CancellationToken ct)
    {
        if (receivedDataMessage || !IsPrematureEmptyReceive(elapsed, _timeout))
        {
            _backoff = TimeSpan.Zero;
            _sinceLastWarning = TimeSpan.Zero;
            return;
        }

        var previous = _backoff;
        _backoff = NextSpinBackoff(previous, BackoffMax);

        // Nothing else runs while the loop spins, so summing the iterations is an adequate clock for the reminder interval.
        _sinceLastWarning += elapsed + _backoff;

        if (ShouldWarn(previous, _backoff, _sinceLastWarning))
        {
            _sinceLastWarning = TimeSpan.Zero;
            Telemetry.Log(
                _logger,
                LogLevel.Warning,
                null,
                "WAITFOR returned no messages after {ElapsedMilliseconds}ms but its timeout is {TimeoutSeconds}s; the receive loop is spinning. Backing off {BackoffMilliseconds}ms.",
                [
                    ("ElapsedMilliseconds", (long)elapsed.TotalMilliseconds),
                    ("TimeoutSeconds", _timeout),
                    ("BackoffMilliseconds", (long)_backoff.TotalMilliseconds)
                ]);
        }

        await _delay(_backoff, ct);
    }

    /// <summary>True when an empty RECEIVE returned well inside its TIMEOUT. Half is deliberately generous - a healthy empty receive returns at the timeout, and <see cref="SqlTableDependency{T}.StartAsync"/> floors the timeout at 60 seconds.</summary>
    internal static bool IsPrematureEmptyReceive(TimeSpan elapsed, int timeout)
        => elapsed < TimeSpan.FromSeconds(timeout) / 2;

    /// <summary>Doubles the backoff from one second up to <paramref name="max"/>.</summary>
    internal static TimeSpan NextSpinBackoff(TimeSpan backoff, TimeSpan max)
        => backoff <= TimeSpan.Zero
            ? BackoffInitial
            : TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, max.Ticks));

    /// <summary>Warns while the backoff is still climbing, then only once per <see cref="WarningReminderInterval"/>, so a long spin costs a handful of lines an hour rather than one per iteration.</summary>
    internal static bool ShouldWarn(TimeSpan previousBackoff, TimeSpan nextBackoff, TimeSpan sinceLastWarning)
        => nextBackoff != previousBackoff || sinceLastWarning >= WarningReminderInterval;
}