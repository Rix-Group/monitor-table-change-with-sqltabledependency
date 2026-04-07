# Running Tests
## Database Creation
To create a test database, you need to launch `TableDependency.SqlClient.Test.Aspire.AppHost`.

This will spin up a SQL Server on port `60501` with all the config needed.

After the database has been configured, Aspire will exit.

## Connecting to DB
Property | Value
-------- | -----
Server name | 127.0.0.1,60501
Authentication | SQL Server Authentication
Login | sa
Password | Casadolcecasa1
Encryption | Mandatory, Trust server certificate