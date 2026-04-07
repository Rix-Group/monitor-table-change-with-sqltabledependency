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
- Nullable reference types are enabled; keep new code nullable-safe.
- If you add/remove projects, update the `.slnx`
- When you learn something non-obvious, add it here so future changes go faster.