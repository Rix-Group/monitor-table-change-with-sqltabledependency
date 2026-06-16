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

namespace TableDependency.SqlClient.Resources;

public static partial class SqlScripts
{
    public const string InformationSchemaColumns = @"SELECT DB_NAME() AS TABLE_CATALOG,
SCHEMA_NAME(o.schema_id) AS TABLE_SCHEMA,
o.name AS TABLE_NAME,
c.name AS COLUMN_NAME,
COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity') AS IS_IDENTITY,
COLUMNPROPERTY(c.object_id, c.name, 'IsComputed') AS IS_COMPUTED,
COLUMNPROPERTY(c.object_id, c.name, 'ordinal') AS ORDINAL_POSITION,
convert(nvarchar(4000), OBJECT_DEFINITION(c.default_object_id))	AS COLUMN_DEFAULT,
convert(varchar(3), CASE c.is_nullable WHEN 1 THEN 'YES' ELSE 'NO' END)	AS IS_NULLABLE,
TYPE_NAME(c.user_type_id) AS DATA_TYPE,
COLUMNPROPERTY(c.object_id, c.name, 'charmaxlen') AS CHARACTER_MAXIMUM_LENGTH,
COLUMNPROPERTY(c.object_id, c.name, 'octetmaxlen') AS CHARACTER_OCTET_LENGTH,
convert(tinyint, CASE -- int/decimal/numeric/real/float/money
WHEN c.system_type_id IN (48, 52, 56, 59, 60, 62, 106, 108, 122, 127) THEN c.precision
END) AS NUMERIC_PRECISION,
convert(smallint, CASE	-- int/money/decimal/numeric
WHEN c.system_type_id IN (48, 52, 56, 60, 106, 108, 122, 127) THEN 10
WHEN c.system_type_id IN (59, 62) THEN 2 END)	AS NUMERIC_PRECISION_RADIX,	-- real/float
convert(int, CASE	-- datetime/smalldatetime
WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN NULL
ELSE ODBCSCALE(c.system_type_id, c.scale) END)	AS NUMERIC_SCALE,
convert(smallint, CASE -- datetime/smalldatetime
WHEN c.system_type_id IN (40, 41, 42, 43, 58, 61) THEN ODBCSCALE(c.system_type_id, c.scale) END) AS DATETIME_PRECISION,
convert(sysname, null)	AS CHARACTER_SET_CATALOG,
convert(sysname, CASE WHEN c.system_type_id IN (35, 167, 175)	-- char/varchar/text
THEN COLLATIONPROPERTY(c.collation_name, 'sqlcharsetname')
WHEN c.system_type_id IN (99, 231, 239)	-- nchar/nvarchar/ntext
THEN N'UNICODE' END) AS CHARACTER_SET_NAME,
convert(sysname, null) AS COLLATION_CATALOG,
c.collation_name AS COLLATION_NAME,
convert(sysname, CASE WHEN c.user_type_id > 256
THEN DB_NAME() END)	AS DOMAIN_CATALOG, convert(sysname, CASE WHEN c.user_type_id > 256
THEN SCHEMA_NAME(t.schema_id)
END) AS DOMAIN_SCHEMA, convert(sysname, CASE WHEN c.user_type_id > 256
THEN TYPE_NAME(c.user_type_id)
END) AS DOMAIN_NAME
FROM sys.objects o JOIN sys.columns c ON c.object_id = o.object_id
LEFT JOIN sys.types t ON c.user_type_id = t.user_type_id
WHERE o.type IN ('U') and SCHEMA_NAME(o.schema_id) = '{0}' and o.name = '{1}'";

    public const string InformationSchemaTables = "SELECT COUNT(*) FROM sys.objects o LEFT JOIN sys.schemas s ON s.schema_id = o.schema_id WHERE o.type IN ('U', 'V') and o.name = '{0}' and s.name = '{1}'";

    public const string TableKeyColumns = @"SELECT i.name AS INDEX_NAME,
ic.key_ordinal AS KEY_ORDINAL,
c.name AS COLUMN_NAME
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND ic.is_included_column = 0
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.objects o ON o.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type IN ('U') AND s.name = '{0}' AND o.name = '{1}' AND (i.is_primary_key = 1 OR i.is_unique = 1)
ORDER BY i.is_primary_key DESC, i.is_unique DESC, i.index_id, ic.key_ordinal";

    // Effective-permission guard: HAS_PERMS_BY_NAME reports the right however it is conferred
    // (direct grant, role membership, or schema/database ownership), so it is robust to the
    // ownership-based broker schema model where no CONTROL row ever appears.
    // @brokerSchema = broker schema name; @table = [schema].[table] of the monitored table.
    public const string SelectEffectivePermissions = @"SELECT
    [CONNECT]             = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CONNECT'),
    [CREATE QUEUE]        = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE QUEUE'),
    [CREATE SERVICE]      = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SERVICE'),
    [CREATE CONTRACT]     = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE CONTRACT'),
    [CREATE MESSAGE TYPE] = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE MESSAGE TYPE'),
    [CREATE PROCEDURE]    = HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE PROCEDURE'),
    [ALTER ON BROKER SCHEMA]      = HAS_PERMS_BY_NAME(@brokerSchema, 'SCHEMA', 'ALTER'),
    [REFERENCES ON BROKER SCHEMA] = HAS_PERMS_BY_NAME(@brokerSchema, 'SCHEMA', 'REFERENCES'),
    [ALTER ON TABLE]      = HAS_PERMS_BY_NAME(@table, 'OBJECT', 'ALTER'),
    [SELECT ON TABLE]     = HAS_PERMS_BY_NAME(@table, 'OBJECT', 'SELECT');";
}