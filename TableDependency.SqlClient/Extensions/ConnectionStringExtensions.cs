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

using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Utilities;
using TableDependency.SqlClient.Enums;
using TableDependency.SqlClient.Exceptions;
using TableDependency.SqlClient.Resources;

namespace TableDependency.SqlClient.Extensions;

internal static class ConnectionStringExtensions
{
    extension (string connectionString)
    {
        public async Task TestConnectionAsync(CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            SqlConnectionStringBuilder sqlConnectionStringBuilder;

            try
            {
                sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            }
            catch (Exception exception)
            {
                throw new InvalidConnectionStringException(connectionString, exception);
            }

            await using var sqlConnection = new SqlConnection(sqlConnectionStringBuilder.ConnectionString);
            try
            {
                await sqlConnection.OpenAsync(ct);
            }
            catch (SqlException exception)
            {
                throw new ImpossibleOpenSqlConnectionException(sqlConnectionStringBuilder.ConnectionString, exception);
            }
        }

        public async Task<SqlServerVersion> GetSqlServerVersionAsync(CancellationToken ct)
        {
            try
            {
                await using var sqlConnection = new SqlConnection(connectionString);
                await sqlConnection.OpenAsync(ct);

                var serverVersion = sqlConnection.ServerVersion;
                if (string.IsNullOrWhiteSpace(serverVersion))
                    return SqlServerVersion.Unknown;

                var serverVersionDetails = serverVersion.Split(['.'], StringSplitOptions.None);

                return int.Parse(serverVersionDetails[0]) switch
                {
                    < 8 => SqlServerVersion.Unknown,
                    8 => SqlServerVersion.SqlServer2000,
                    9 => SqlServerVersion.SqlServer2005,
                    10 => SqlServerVersion.SqlServer2008,
                    11 => SqlServerVersion.SqlServer2012,
                    _ => SqlServerVersion.SqlServerLatest
                };
            }
            catch
            {
                throw new SqlServerVersionNotSupportedException();
            }
        }

        public async Task CheckServiceBrokerIsEnabledAsync(CancellationToken ct)
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = "SELECT is_broker_enabled FROM sys.databases WITH (NOLOCK) WHERE database_id = db_id();";

            var o = await sqlCommand.ExecuteScalarAsync(ct);
            if (o is not bool enabled || !enabled)
            {
                string? database;
                try
                {
                    database = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
                }
                catch
                {
                    database = null;
                }

                throw new ServiceBrokerNotEnabledException(database);
            }
        }

        public async Task CheckUserHasPermissionsAsync(string schemaName, string tableName, string brokerSchemaName, CancellationToken ct)
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = SqlScripts.SelectEffectivePermissions;
            sqlCommand.Parameters.AddWithValue("@brokerSchema", brokerSchemaName);
            sqlCommand.Parameters.AddWithValue("@table", $"[{schemaName}].[{tableName}]");

            await ThrowOnMissingPermissionAsync(sqlCommand, ct);
        }

        // Ensure the broker schema exists, creating it (owned by the connecting principal) when missing.
        // Creation needs CREATE SCHEMA / CONTROL; without it a clear exception tells the operator to pre-create the schema.
        public async Task EnsureBrokerSchemaExistsAsync(string schemaName, CancellationToken ct)
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.Parameters.AddWithValue("@schema", schemaName);

            sqlCommand.CommandText = "IF SCHEMA_ID(@schema) IS NULL EXEC ('CREATE SCHEMA ' + QUOTENAME(@schema));";
            try
            {
                await sqlCommand.ExecuteNonQueryAsync(ct);
            }
            catch (SqlException exception)
            {
                // Lost a creation race with another listener, or no CREATE SCHEMA right: succeed if it now exists.
                sqlCommand.CommandText = "SELECT CASE WHEN SCHEMA_ID(@schema) IS NULL THEN 0 ELSE 1 END;";
                if (await sqlCommand.ExecuteScalarAsync(ct) is 1)
                    return;

                throw new BrokerSchemaUnavailableException(schemaName, exception);
            }
        }

        public async Task CheckTableExistsAsync(string schemaName, string tableName, CancellationToken ct)
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = string.Format(SqlScripts.InformationSchemaTables, tableName, schemaName);

            var o = await sqlCommand.ExecuteScalarAsync(ct);
            if (o is not int i || i is 0)
                throw new NotExistingTableException(tableName);
        }

        public async Task<TableColumnInfo[]> GetTableColumnsAsync(string schemaName, string tableName, CancellationToken ct)
        {
            var columnsList = new List<TableColumnInfo>();

            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            await using var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = string.Format(SqlScripts.InformationSchemaColumns, schemaName, tableName);

            var reader = await sqlCommand.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var name = reader["COLUMN_NAME"].ToString();
                var dataType = reader["DATA_TYPE"].ToString()?.ConvertNumericType();

                var characterMaximumLength = reader.GetSafeString(reader.GetOrdinal("CHARACTER_MAXIMUM_LENGTH"));
                var numericPrecision = reader.GetSafeString(reader.GetOrdinal("NUMERIC_PRECISION"));
                var numericScale = reader.GetSafeString(reader.GetOrdinal("NUMERIC_SCALE"));
                var dateTimePrecision = reader.GetSafeString(reader.GetOrdinal("DATETIME_PRECISION"));
                var isIdentity = reader.GetSafeString(reader.GetOrdinal("IS_IDENTITY")) is "1";
                var isComputed = reader.GetSafeString(reader.GetOrdinal("IS_COMPUTED")) is "1";

                var size = dataType?.ToUpperInvariant() switch
                {
                    "BINARY" or "VARBINARY" or "CHAR" or "NCHAR" or "VARCHAR" or "NVARCHAR" => characterMaximumLength is "-1" ? "MAX" : characterMaximumLength,
                    "DECIMAL" => $"{numericPrecision},{numericScale}",
                    "FLOAT" or "TEXT" or "NTEXT" or "IMAGE" => null,
                    "DATETIME2" or "DATETIMEOFFSET" or "TIME" => dateTimePrecision,
                    _ => null
                };

                if (name is not null && dataType is not null)
                    columnsList.Add(new TableColumnInfo(name, dataType, size, isIdentity, isComputed));
            }

            return [.. columnsList];
        }

        public async Task<bool> CheckIfDatabaseObjectsExistAsync(string prefix, CancellationToken ct)
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(ct);

            var sqlCommand = sqlConnection.CreateCommand();
            sqlCommand.CommandText = $"SELECT COUNT(*) FROM sys.service_queues WITH (NOLOCK) WHERE name LIKE N'{prefix}%';";
            return await sqlCommand.ExecuteScalarAsync(ct) is > 0;
        }
    }

    // Each column is a HAS_PERMS_BY_NAME probe: 1 = held (by grant, role, or ownership), 0 = not held,
    // NULL = securable does not exist; anything other than 1 is reported as the missing permission.
    private static async Task ThrowOnMissingPermissionAsync(SqlCommand sqlCommand, CancellationToken ct)
    {
        await using var reader = await sqlCommand.ExecuteReaderAsync(CommandBehavior.CloseConnection, ct);
        if (!await reader.ReadAsync(ct))
            throw new UserWithNoPermissionException();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (reader.IsDBNull(i) || reader.GetInt32(i) is not 1)
                throw new UserWithMissingPermissionException(reader.GetName(i));
        }
    }
}