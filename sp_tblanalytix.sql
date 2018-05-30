USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_tblAnalytix]    Script Date: 29/05/2018 13:40:39 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_tblAnalytix] --- EXEC sp_ms_marksystemobject 'sp_tblAnalytix'
	 @TableName NVARCHAR(50)
	,@TopX CHAR(10) = '5'
	WITH RECOMPILE
AS
BEGIN
	SET NOCOUNT ON
		-- CHECK FOR THE TABLE NAME IF EXISTS IN CURRENT DATABASE
	SET @TableName = UPPER(@TableName)
    DECLARE @db SYSNAME = UPPER(DB_NAME())
	IF 
    LEN(LTRIM(RTRIM(@tablename))) = 0
    OR 
    (SELECT COUNT(*) FROM sysobjects WHERE xtype = 'U' AND NAME = @tablename) = 0
	BEGIN
		PRINT 'table >> ' + @TableName + ' << does not exist in >> ' + @db + ' << database'
	END
	ELSE
        -- CHECK IF DEPRECATED DATA TYPES ARE USED IN THE TABLE - NTEXT/TEXT
    IF 
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.columns c join INFORMATION_SCHEMA.tables t on c.table_name = t.table_name WHERE table_type= 'BASE TABLE'
    AND 
    [DATA_TYPE] IN ('text','ntext') 
    AND 
    t.table_name = @tablename) > 0

	BEGIN
	
		PRINT '
        -- RUN FOLLOWING STATEMENT TO GET CODE READY FOR TABLE ALTER - CHANGE DATA TYPE FROM DEPRECATED TO NVARCHAR(MAX)
        USE ' + @db + '
        SELECT 
        t.table_name,[c].[COLUMN_NAME], data_type ,
        [RetypeColumnStatement]=''ALTER TABLE ''+c.[TABLE_SCHEMA]+''.''+t.table_name+'' ALTER COLUMN [''+[c].[COLUMN_NAME]+''] NVARCHAR(MAX)''
        FROM [INFORMATION_SCHEMA].[COLUMNS] c JOIN [INFORMATION_SCHEMA].[TABLES] t ON t.[TABLE_NAME] = c.[TABLE_NAME] AND c.[TABLE_SCHEMA] = t.[TABLE_SCHEMA] AND [t].[TABLE_TYPE] = ''base table''
        WHERE [DATA_TYPE] IN (''ntext'',''text'')'
        
    END
	ELSE

SET @topx = IIF(@TopX='',0,@TopX)
DECLARE @EmptyColumns NVARCHAR(MAX)
DECLARE @SelectColumns NVARCHAR(MAX)
DECLARE @sql NVARCHAR(MAX)  = (
SELECT STUFF((SELECT 'UNION ALL SELECT TOP 1 '''+@tablename+''','''+COLUMN_NAME+''','''+Data_Type+''' FROM '+@tablename +' WHERE DATALENGTH('+QUOTENAME(COLUMN_NAME)+') > 0 
' AS code
FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_NAME = c.table_name 
WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_NAME = @tablename AND c.DATA_TYPE NOT IN ('image','varbinary','timestamp','geography') FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' ),1,10,'') AS NotEmptyTables
)
IF OBJECT_ID('tempdb..#Results') IS NOT NULL BEGIN DROP TABLE #Results END
CREATE TABLE #Results 
(ID             INT IDENTITY(1,1) NOT NULL,
[TableName]		NVARCHAR(MAX),				    [ColumnName]	NVARCHAR(MAX),
[Datatype]		NVARCHAR(MAX),				    [FilledPC]		REAL,
[Cnt]			BIGINT,						    [UnqCnt]		BIGINT,
[NllCnt]		BIGINT,						    [MinValue]		NVARCHAR(MAX),
[MaxValue]		NVARCHAR(MAX),				    [RandValue]		NVARCHAR(MAX),
[MinLen]		NVARCHAR(MAX),				    [MaxLen]		NVARCHAR(MAX),
[TopX]			NVARCHAR(MAX),				    [UNQPC]			NVARCHAR(MAX),
[DUPPC]			NVARCHAR(MAX),				    [NULPC]			NVARCHAR(MAX),
[BlnkCnt]		BIGINT,						    [DType]			NVARCHAR(MAX)				
)
INSERT INTO #Results (tablename,[ColumnName],Datatype)
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------MINIMUM VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET MinValue = (SELECT MIN('+IIF(datatype IN ('datetime','int','uniqueidentifier'),'['+ColumnName+'])','CAST(['+ColumnName+'] AS NVARCHAR(MAX)))')+' FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------MAXIMUM VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET MaxValue = (SELECT MAX('+IIF(datatype IN ('datetime','int','uniqueidentifier'),'['+ColumnName+'])','CAST(['+ColumnName+'] AS NVARCHAR(MAX)))')+' FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------MAXIMUM LENGTH VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET MaxLen = '+IIF(datatype IN ('datetime','uniqueidentifier'),'''''','(SELECT MAX(LEN(CAST(['+ColumnName+'] AS NVARCHAR(MAX)))) FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0)')+' WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------MINIMUM LENGTH VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET MinLen = '+IIF(datatype IN ('datetime','uniqueidentifier'),'''''','(SELECT MIN(LEN(CAST(['+ColumnName+'] AS NVARCHAR(MAX)))) FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0)')+' WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------RANDOM VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET RandValue = (SELECT TOP 1 (CAST(['+ColumnName+'] AS NVARCHAR(MAX))) FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0 ORDER BY NEWID()) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------Cnt VALUE----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET Cnt = (SELECT COUNT(['+ColumnName+']) FROM ['+TableName+'] WHERE DATALENGTH(['+ColumnName+']) > 0) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------UNIQUE Cnt ----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET UnqCnt = (SELECT COUNT(DISTINCT ['+ColumnName+']) FROM '+TableName+' WHERE DATALENGTH(['+ColumnName+']) > 0) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------NULL Cnt ----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET NllCnt = (SELECT COUNT(*) FROM '+TableName+' WHERE ['+ColumnName+'] IS NULL) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------BLANK Cnt ----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET BlnkCnt = (SELECT COUNT(*) FROM ['+TableName+'] WHERE CAST(['+ColumnName+'] AS NVARCHAR(MAX)) IN (CHAR(13)+CHAR(10),'''')) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------TOP X ----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET Topx = (SELECT (SELECT TOP '+@topx+' CHAR(13)+CHAR(10)+''|''+CAST(COUNT(*) AS NVARCHAR(100))+''  >>  ''+CAST(['+ColumnName+'] AS NVARCHAR(MAX)) FROM ['+TableName+'] WHERE LEN(['+ColumnName+']) > 0 GROUP BY ['+ColumnName+'] ORDER BY COUNT(*) DESC FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''nvarchar(max)'')) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------FILLED PC----------------------------------------------------*/
SET @sql = (
SELECT '
UPDATE #Results SET FilledPC = CAST(CAST(100.*IIF([Cnt]=0,1,[Cnt])/(SELECT COUNT(*) FROM ['+TableName+']) AS NUMERIC(4, 1)) AS REAL) WHERE ColumnName = '''+ColumnName+'''' FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql
/*----------------------------------------------------STATS VALUE----------------------------------------------------*/
SET @sql = (
 SELECT '
 UPDATE #Results
 SET 
 UNQPC	= a.UNQPC
 ,DUPPC = a.DUPPC
 ,NULPC = a.NULPC
 FROM #Results r
 JOIN (
 SELECT 
 COLNM = '''+ColumnName+'''
,UNQPC = CAST(CAST(100.0*SUM(IIF(cnt=1 AND ColName IS NOT NULL,1,0))*1.0/SUM(cnt) AS NUMERIC(4, 1)) AS REAL)
,DUPPC = CAST(CAST(100.0*SUM(IIF(cnt>1 AND ColName IS NOT NULL,cnt,0))*1.0/SUM(cnt) AS NUMERIC(4, 1)) AS REAL)
,NULPC = CAST(CAST(100.0*SUM(IIF(ColName IS NULL,cnt,0))*1.0/SUM(cnt) AS NUMERIC(4, 1)) AS REAL)
FROM (SELECT ['+ColumnName+'] AS ColName,COUNT(*) AS CNT FROM ['+TableName+'] GROUP BY ['+ColumnName+']) AS X)  A ON a.COLNM = ColumnName'FROM #Results 
FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' )
--PRINT @sql
EXEC sp_executesql @sql

SELECT IIF(
           ROW_NUMBER() OVER (ORDER BY IIF([Cnt] = (SELECT MAX([Cnt]) FROM #Results) AND UnqCnt < 3, 1, [Cnt]) DESC,
                                       [Cnt] DESC,
                                       UnqCnt DESC
                             ) = 1,
           @TableName + ' =>',
           ' ') AS [ ],
       ColumnName,
       ' ' AS Notes,
       Datatype,
       [Cnt],
       UnqCnt,
       IIF(MinLen != '', CAST(MinLen AS VARCHAR(1000)) + '--' + CAST(MaxLen AS VARCHAR(1000)), 'NA') AS MinMaxLen,
       NllCnt,
       BlnkCnt,
       REPLACE(REPLACE(REPLACE(MinValue	,CHAR(13),''),CHAR(10),''),CHAR(13)+CHAR(10),'') AS MinValue	,
       REPLACE(REPLACE(REPLACE(MaxValue	,CHAR(13),''),CHAR(10),''),CHAR(13)+CHAR(10),'') AS MaxValue	,
       REPLACE(REPLACE(REPLACE(RandValue,CHAR(13),''),CHAR(10),''),CHAR(13)+CHAR(10),'') AS RandValue   ,
       REPLACE(REPLACE(REPLACE(TopX		,CHAR(13),''),CHAR(10),''),CHAR(13)+CHAR(10),'') AS TopX		,
       FilledPC,
       UNQPC,
       DUPPC,
       NULPC,
       'SELECT ' + ColumnName + CHAR(13) + ',COUNT(*) AS CNT ' + CHAR(13) + 'FROM ' + @TableName + CHAR(13)
       + '--WHERE DATALENGTH(' + ColumnName + ')>0 ' + CHAR(13) + 'GROUP BY ' + ColumnName + CHAR(13)
       + 'ORDER BY 2 DESC' AS StmtGBy
FROM #Results
ORDER BY IIF([Cnt] = (SELECT MAX([Cnt]) FROM #Results) AND UnqCnt < 3, 1, [Cnt]) DESC,
         [Cnt] DESC,
         UnqCnt DESC


;WITH SelectColumns AS
(
SELECT
ColumnName = ColumnName + REPLICATE(' ',5+(SELECT MAX(LEN(ColumnName)) FROM #Results)-LEN(ColumnName))+'--'+CAST(Cnt AS VARCHAR(MAX)),cnt,UnqCnt
FROM #Results																										  

)

SELECT @SelectColumns = STUFF((SELECT '
,'+ec.ColumnName FROM SelectColumns ec 
ORDER BY
IIF([Cnt] = (SELECT MAX([Cnt]) FROM #Results) AND UnqCnt < 3,1,[Cnt]) DESC,
[Cnt] DESC, 
UnqCnt DESC	
 FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' ),1,3,'')

PRINT '

----------------------------------------------------
SELECT TOP ROWS - POPULATED COLUMNS ONLY
----------------------------------------------------
SELECT TOP 10
'+@SelectColumns+'
FROM '+@tablename

;WITH EmptyColumns AS
(
SELECT c.COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_NAME = c.table_name 
WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_NAME = @tablename
EXCEPT
SELECT columnname COLLATE DATABASE_DEFAULT FROM #Results 
)

SELECT @EmptyColumns = STUFF((SELECT '
'+ec.COLUMN_NAME+' --<'+DATA_TYPE FROM EmptyColumns ec join INFORMATION_SCHEMA.COLUMNS c ON c.column_name = ec.COLUMN_NAME JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_NAME = c.table_name 
WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_NAME = @tablename order by IIF(data_type IN ('image','binary','varbinary'),0,1),1
 FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)' ),1,2,'')

PRINT '

----------------------------------------------------
'+@tablename+' table - Empty/binary columns :
----------------------------------------------------
'+@EmptyColumns

END
