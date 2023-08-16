--Create DB
CREATE DATABASE sampledata;
GO

--Uses DB
USE sampledata;
GO

--Create Construction Schema
CREATE SCHEMA construction;
GO

--Create Empty Table for Schema
CREATE TABLE construction.constructiondata (
[Project ID] VARCHAR(MAX) NOT NULL
,[Description] VARCHAR(MAX) NULL
,[Client Agency] VARCHAR(MAX) NULL
,[Building Type] VARCHAR(MAX) NULL
,[Phase] VARCHAR(MAX) NULL
,[Projected Construction Start] DATETIME2(7) NULL
,[Projected Construction Completion] DATETIME2(7) NULL
,[Project Duration (Days)] INT NULL
,[Scope] VARCHAR(MAX) NULL
,[Dollar Amount] VARCHAR(MAX) NULL
);
GO

--Import raw data from Local
BULK INSERT construction.constructiondata
FROM 'C:\Users\Admin\Downloads\constructiondata.csv'
WITH
(
        FORMAT='CSV',
        FIRSTROW=2
)
GO


/******************************
/*Initial Analysis of Raw Data*/
*******************************/
--View Table
SELECT * FROM construction.constructiondata;
GO

--6 Dollar Amounts
SELECT DISTINCT [Dollar Amount] FROM construction.constructiondata;
GO

--16 Agencies
SELECT DISTINCT [Client Agency] FROM construction.constructiondata;
GO

--Create Schema for view
CREATE SCHEMA acumine;
GO

--Create Fact view for Power BI
Create View acumine.fact

as 

SELECT 
[Project Id]
,[Client Agency]
,[Projected Construction Start] as [Start Date]
,[Projected Construction Completion] as [End Date]
,[Project Duration (Days)] as [Duration]
,[Dollar Amount] as [Budget Range (USD)]
,CASE WHEN [Dollar Amount] = '$100M to 500M' THEN 500000000
WHEN [Dollar Amount] = '$10M to 30M' THEN 30000000
WHEN [Dollar Amount] = '$1M or less' THEN 1000000
WHEN [Dollar Amount] = '$1M to 3M' THEN 3000000
WHEN [Dollar Amount] = '$30M to 100M' THEN 100000000
WHEN [Dollar Amount] = '$3M to 10M' THEN 10000000
END AS [Max Cost (USD)]
,CASE WHEN [Dollar Amount] = '$100M to 500M' THEN 6
WHEN [Dollar Amount] = '$10M to 30M' THEN 4
WHEN [Dollar Amount] = '$1M or less' THEN 1
WHEN [Dollar Amount] = '$1M to 3M' THEN 2
WHEN [Dollar Amount] = '$30M to 100M' THEN 5
WHEN [Dollar Amount] = '$3M to 10M' THEN 3
END AS [Budget Range ORDERING]
FROM construction.constructiondata;

--Testing Fact View
select * from acumine.fact;
GO
