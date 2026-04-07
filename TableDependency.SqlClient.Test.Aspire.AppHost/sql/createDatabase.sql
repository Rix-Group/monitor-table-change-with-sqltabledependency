-- Create Test_User login
CREATE LOGIN Test_User WITH PASSWORD = 'Casadolcecasa1';
GO

-- Create database
IF DB_ID(N'TableDependencyDB') IS NULL
BEGIN
    CREATE DATABASE [TableDependencyDB];
END
GO

-- Switch to new database
USE [TableDependencyDB];
GO

-- Create database user
IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'Test_User'
)
BEGIN
    CREATE USER [Test_User] FOR LOGIN [Test_User];
	GRANT ALTER TO [Test_User];
	GRANT CONNECT TO [Test_User];
	GRANT CONTROL TO [Test_User];
	GRANT CREATE CONTRACT TO [Test_User];
	GRANT CREATE MESSAGE TYPE TO [Test_User];
	GRANT CREATE PROCEDURE TO [Test_User];
	GRANT CREATE QUEUE TO [Test_User];
	GRANT CREATE SERVICE TO [Test_User];
	GRANT EXECUTE TO [Test_User];
	GRANT SELECT TO [Test_User];
	GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [Test_User];
	GRANT VIEW DATABASE STATE TO [Test_User];
	GRANT VIEW DEFINITION TO [Test_User];
END
GO

-- SETUP FOR TEST CLIENT
-- Create category table
IF OBJECT_ID(N'dbo.Categories', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Categories (
        Id INT NOT NULL PRIMARY KEY,
        Name NVARCHAR(100) NOT NULL
    );
END
GO

-- Insert category rows based on CategorysEnum values
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM dbo.Categories WHERE Id = 1)
    INSERT INTO dbo.Categories (Id, Name) VALUES (1, N'Food');
IF NOT EXISTS (SELECT 1 FROM dbo.Categories WHERE Id = 2)
    INSERT INTO dbo.Categories (Id, Name) VALUES (2, N'Drink');
IF NOT EXISTS (SELECT 1 FROM dbo.Categories WHERE Id = 3)
    INSERT INTO dbo.Categories (Id, Name) VALUES (3, N'Dessert');
SET NOCOUNT OFF;
GO

-- Create product table
IF OBJECT_ID(N'dbo.Products', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Products (
        Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Quantity INT NOT NULL DEFAULT 0,
        CategoryId INT NOT NULL,
        Name NVARCHAR(200) NULL,
        ExpiringDate DATETIME2 NULL,
        Price DECIMAL(18,2) NULL,
        CONSTRAINT FK_Product_Category FOREIGN KEY (CategoryId) REFERENCES dbo.Categories(Id)
    );
END
GO