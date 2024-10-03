USE GoldDB
GO

CREATE OR ALTER PROC CreateSQLServerlessView_gold @ViewName NVARCHAR(100)
AS
BEGIN

DECLARE @statement VARCHAR(MAX)
    
    SET @statement = N'CREATE or ALTER VIEW ' + @ViewName + ' AS
        SELECT *
        FROM
            OPENROWSET(
            BULK ''https://patrykdatalakegen2.dfs.core.windows.net/gold/SalesLT/' + @ViewName + '/'',
            FORMAT = ''DELTA''
        ) AS [result]
    '

EXEC (@statement)

END
GO
