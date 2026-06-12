using System.Diagnostics;
using System.Reflection;

namespace TableDependency.SqlClient.Test.Features.Logging;

public class ActivitySourceTest
{
    private sealed class SampleModel;

    [Fact]
    public void ActivitySource_IsListenable_UnderPublishedConstantName()
    {
        // ARRANGE - a consumer registers OTel using the published constant, so the real source must answer to it.
        Activity? captured = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SqlTableDependency<SampleModel>.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => captured = activity
        };
        ActivitySource.AddActivityListener(listener);

        var source = (ActivitySource)typeof(SqlTableDependency<SampleModel>)
            .GetField("ActivitySource", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

        // ACT
        using var started = source.StartActivity("tabledependency.test");

        // ASSERT - listener keyed on the constant captured the span, proving the name consumers register is the name emitted.
        Assert.NotNull(captured);
        Assert.Equal(SqlTableDependency<SampleModel>.ActivitySourceName, source.Name);
    }

    [Fact]
    public void ActivitySourceName_MatchesPublishedString_SoExistingConsumersKeepWorking()
    {
        // ARRANGE & ACT
        var name = SqlTableDependency<SampleModel>.ActivitySourceName;

        // ASSERT - a rename is a breaking telemetry change for every AddSource("SqlTableDependency") already deployed.
        Assert.Equal("SqlTableDependency", name);
    }
}