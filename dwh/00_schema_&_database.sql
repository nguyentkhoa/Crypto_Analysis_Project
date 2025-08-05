IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dwh')
BEGIN
    EXEC('CREATE SCHEMA dwh');
END
GO

-- Create Databse Coin_Analysis_DWH where storage dim and fact tables
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Coin_Analysis_DWH')
BEGIN
    CREATE DATABASE Coin_Analysis_DWH;
END
GO
USE Coin_Analysis_DWH;
GO



