# AGENTS

## Project overview
- SqlTableDependency library for SQL Server table change notifications.
- Fork of `https://github.com/IsNemoEqualTrue/monitor-table-change-with-sqltabledependency` but on .NET and async support.
- Main library: `TableDependency.SqlClient/` (net10.0).
- Unit tests: `TableDependency.SqlClient.Test/` (xUnit v3, Testcontainers MsSql).
- Other manual tests live under `TableDependency.SqlClient.Test.*` projects (Aspire AppHost, client, load tests, crash sim), see `RunningTests.md` for local SQL Server details.
- IDE solution file: `TableDependency.slnx`.

## Build
- `dotnet build TableDependency.SqlClient/TableDependency.SqlClient.csproj`

## Tests
- `dotnet test TableDependency.SqlClient.Test/TableDependency.SqlClient.Test.csproj`
- You can run tests, as the SQL Server is started by testcontainers when running `dotnet test` automatically.
- Don't run all tests at once as they take over 20 minutes, just ones added or updated.

## Notes
- Target framework is `net10.0`; use a matching SDK.
- Use modern features such as collection expressions, extension blocks, pattern matching, is null, file-scoped namespaces.
- All files should be formatted in CRLF.
- Don't add a trailing line to the end of a file.
- Always run the unit tests related to any code changes you make.
- Always add/update related unit tests.
- Keep test comments concise and one line - no multi-line preamble blocks above classes/assertions.
- Structure every test with `// ARRANGE`, `// ACT`, `// ASSERT` section markers, and make the test fail by observing the real defect (not by string-matching on production code).
- Nullable reference types are enabled; keep new code nullable-safe.
- If you add/remove projects, update the `.slnx`
- The `WaitForNotificationsAsync` loop must arm `BEGIN CONVERSATION TIMER` in persisted mode too: it is the only thing that retires an idle initiator dialog. The `DialogTimer` fires onto the `_Sender` queue, whose activation procedure runs `END CONVERSATION` (its drop-all script is empty when persisted, so objects survive). Drop it and persisted dialogs stay `CONVERSING` forever — any test that waits for a persisted conversation to self-close (e.g. `Pr232`) hangs.
- On container stop (Kubernetes rollout = SIGTERM) the long-running `WAITFOR` connection is severed. The resulting `SqlException` can be thrown *before* the `StartAsync` cancellation token has propagated, so checking only `ct.IsCancellationRequested` in the `WaitForNotificationsAsync` catch loses the race and reports a benign shutdown as `StopDueToError` + `OnException`. The fix latches `_shuttingDown` from a `PosixSignalRegistration` on SIGTERM/SIGINT/SIGQUIT; the signal precedes both the token cancel and the socket teardown, so the catch can treat the drop as a clean stop. A genuine mid-operation outage (no signal) still surfaces via `OnException` — do not broaden this to all `SqlException`s. Arm the registration in `StartAsync` and dispose it in `StopAsync`, NOT the ctor/`DisposeAsync`: the handler delegate captures `this`, and `PosixSignalRegistration` holds it in process-wide static state, so a ctor registration roots the instance until `DisposeAsync`. `TestCollapsingIsolatedContext` only calls `StopAsync` and then asserts its collectible `AssemblyLoadContext` unloads — a registration that outlives `StopAsync` pins the ALC and the test fails with "failed to unload after disposal".
- When you learn something non-obvious, add it here so future changes go faster.