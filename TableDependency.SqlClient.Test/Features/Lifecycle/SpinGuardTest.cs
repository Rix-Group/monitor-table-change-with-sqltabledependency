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
using System.Diagnostics;

namespace TableDependency.SqlClient.Test.Features.Lifecycle;

public class SpinGuardTest
{
    [Theory]
    [InlineData(120, 641, true)]        // the observed spin: empty after 641ms against a 120s timeout
    [InlineData(120, 59_999, true)]
    [InlineData(120, 60_000, false)]
    [InlineData(120, 120_004, false)]   // a healthy empty receive returns at the timeout
    [InlineData(60, 29_999, true)]
    [InlineData(60, 30_000, false)]
    public void IsPrematureEmptyReceive_FlagsOnlyReturnsWellInsideTheTimeout(int timeout, int elapsedMilliseconds, bool expected)
    {
        // ARRANGE
        var elapsed = TimeSpan.FromMilliseconds(elapsedMilliseconds);

        // ACT
        var premature = SpinGuard.IsPrematureEmptyReceive(elapsed, timeout);

        // ASSERT
        Assert.Equal(expected, premature);
    }

    [Fact]
    public void NextSpinBackoff_DoublesFromOneSecondAndCapsAtTheMaximum()
    {
        // ARRANGE
        var max = TimeSpan.FromSeconds(30);
        double[] expected = [1, 2, 4, 8, 16, 30, 30, 30];
        var backoff = TimeSpan.Zero;
        List<double> seconds = [];

        // ACT
        for (var i = 0; i < expected.Length; i++)
        {
            backoff = SpinGuard.NextSpinBackoff(backoff, max);
            seconds.Add(backoff.TotalSeconds);
        }

        // ASSERT
        Assert.Equal(expected, seconds);
    }

    // The receive command re-arms the conversation timer every iteration, so a capped backoff must outlast watchdogTimeout for the
    // DialogTimer to ever fire and retire the stale dialog; a cap at or below it would leave a spin unable to clear itself.
    [Theory]
    [InlineData(120)]
    [InlineData(180)]
    [InlineData(3600)]
    public void BackoffMax_ExceedsTheWatchdogTimeout(int watchdogTimeout)
    {
        // ARRANGE
        var guard = new SpinGuard(logger: null, timeout: 60, watchdogTimeout);

        // ACT
        var max = guard.BackoffMax;

        // ASSERT
        Assert.True(max > TimeSpan.FromSeconds(watchdogTimeout), $"cap {max} must exceed watchdogTimeout of {watchdogTimeout}s");
    }

    [Fact]
    public async Task ThrottleAsync_PrematureEmptyReceive_BacksOffAndClimbsToTheCap()
    {
        // ARRANGE
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        for (var i = 0; i < 10; i++)
            await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal([1, 2, 4, 8, 16, 32, 64, 128, 190, 190], recorder.Delays.Select(d => d.TotalSeconds));
        Assert.Equal(TimeSpan.FromSeconds(190), guard.Backoff);
    }

    [Fact]
    public async Task ThrottleAsync_HealthyEmptyReceive_DoesNotDelay()
    {
        // ARRANGE
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(120_004), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Empty(recorder.Delays);
        Assert.Equal(TimeSpan.Zero, guard.Backoff);
    }

    [Fact]
    public async Task ThrottleAsync_HealthyEmptyReceive_AfterSpin_ResetsTheBackoff()
    {
        // ARRANGE
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(120_004), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal(TimeSpan.Zero, guard.Backoff);
        Assert.Equal([1, 2], recorder.Delays.Select(d => d.TotalSeconds));
    }

    // A torn dialog can deliver an error or an ignored system row on every iteration; those are not data messages, so it still throttles.
    [Fact]
    public async Task ThrottleAsync_RowsReadButNoDataMessage_StillThrottles()
    {
        // ARRANGE
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken);
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(5), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal([1, 2], recorder.Delays.Select(d => d.TotalSeconds));
    }

    [Fact]
    public async Task ThrottleAsync_DataMessage_ResetsTheBackoff()
    {
        // ARRANGE
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: true, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal(TimeSpan.Zero, guard.Backoff);
        Assert.Equal([1, 2], recorder.Delays.Select(d => d.TotalSeconds));
    }

    [Fact]
    public async Task ThrottleAsync_Cancelled_PropagatesTheCancellation()
    {
        // ARRANGE
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, Task.Delay);

        // ACT
        var throttle = async () => await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), cts.Token);

        // ASSERT
        await Assert.ThrowsAnyAsync<OperationCanceledException>(throttle);
    }

    [Fact]
    public async Task ThrottleAsync_WhileTheBackoffClimbs_WarnsOncePerChange()
    {
        // ARRANGE
        var logger = new CountingLogger();
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(logger, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        for (var i = 0; i < 4; i++)
            await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal(4, logger.Count(LogLevel.Warning));
    }

    [Fact]
    public async Task ThrottleAsync_WarningDisabledOnLogger_DelaysButDoesNotWriteSpanEvent()
    {
        // ARRANGE
        using var activity = new Activity("test");
        activity.Start();
        var logger = new CountingLogger(minimum: LogLevel.Error);
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(logger, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ASSERT
        Assert.Equal([1], recorder.Delays.Select(d => d.TotalSeconds));
        Assert.Equal(TimeSpan.FromSeconds(1), guard.Backoff);
        Assert.Empty(activity.Events);
    }

    [Fact]
    public async Task ThrottleAsync_NullLogger_WritesWarningSpanEvent()
    {
        // ARRANGE
        using var activity = new Activity("test");
        activity.Start();
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(null, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);

        // ACT
        await guard.ThrottleAsync(receivedDataMessage: false, TimeSpan.FromMilliseconds(641), TestContext.Current.CancellationToken);

        // ASSERT
        var recordedEvent = Assert.Single(activity.Events);
        var tags = recordedEvent.Tags.ToDictionary(tag => tag.Key, tag => tag.Value);
        Assert.Equal(LogLevel.Warning.ToString(), tags["log.level"]);
        Assert.Equal(641L, tags["ElapsedMilliseconds"]);
        Assert.Equal(120, tags["TimeoutSeconds"]);
        Assert.Equal(1000L, tags["BackoffMilliseconds"]);
    }

    // Once capped the backoff stops changing, so without the reminder a spin would go silent for the rest of its life.
    [Fact]
    public async Task ThrottleAsync_CappedForALongSpin_KeepsWarningOnTheReminderInterval()
    {
        // ARRANGE
        var logger = new CountingLogger();
        var recorder = new DelayRecorder();
        var guard = new SpinGuard(logger, timeout: 120, watchdogTimeout: 180, recorder.DelayAsync);
        var elapsed = TimeSpan.FromMilliseconds(641);
        while (guard.Backoff != guard.BackoffMax)
            await guard.ThrottleAsync(receivedDataMessage: false, elapsed, TestContext.Current.CancellationToken);
        var whileClimbing = logger.Count(LogLevel.Warning);

        // ACT - an hour of capped iterations
        var iterations = (int)(TimeSpan.FromHours(1) / (guard.BackoffMax + elapsed));
        for (var i = 0; i < iterations; i++)
            await guard.ThrottleAsync(receivedDataMessage: false, elapsed, TestContext.Current.CancellationToken);

        // ASSERT
        // The reminder fires on the first iteration that carries the accumulator past the interval, so at a ~190s iteration it lands
        // every fourth one: a few times an hour, and far rarer than the one line per iteration this replaced.
        var whileCapped = logger.Count(LogLevel.Warning) - whileClimbing;
        Assert.InRange(whileCapped, 3, 6);
        Assert.True(whileCapped * 3 <= iterations, $"{whileCapped} reminders over {iterations} iterations is not rare enough");
    }

    [Fact]
    public void ShouldWarn_AtTheCapWithinTheReminderInterval_StaysQuiet()
    {
        // ARRANGE
        var capped = TimeSpan.FromSeconds(190);

        // ACT
        var shouldWarn = SpinGuard.ShouldWarn(capped, capped, TimeSpan.FromMinutes(9));

        // ASSERT
        Assert.False(shouldWarn);
    }

    [Fact]
    public void ShouldWarn_AtTheCapPastTheReminderInterval_WarnsAgain()
    {
        // ARRANGE
        var capped = TimeSpan.FromSeconds(190);

        // ACT
        var shouldWarn = SpinGuard.ShouldWarn(capped, capped, TimeSpan.FromMinutes(10));

        // ASSERT
        Assert.True(shouldWarn);
    }

    private sealed class DelayRecorder
    {
        internal List<TimeSpan> Delays { get; } = [];

        internal Task DelayAsync(TimeSpan delay, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            Delays.Add(delay);
            return Task.CompletedTask;
        }
    }

    private sealed class CountingLogger(LogLevel minimum = LogLevel.Trace) : ILogger
    {
        private readonly List<LogLevel> _levels = [];

        internal int Count(LogLevel level)
            => _levels.Count(l => l == level);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel)
            => logLevel >= minimum;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                _levels.Add(logLevel);
        }
    }
}
