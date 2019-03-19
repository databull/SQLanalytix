USE quarterfivesrc
SET NOCOUNT ON;
/*TODO:
PK if UNQ int/guid/varchar<50,drop defaults
*/
DECLARE @nl CHAR(2) = CHAR(13)
DECLARE @DB NVARCHAR(MAX) = UPPER(DB_NAME())
DECLARE @size NVARCHAR(MAX) = (SELECT STR((SUM(CONVERT(DEC(17,2),size))) / 128/1024,10,2) FROM sys.database_files WHERE type_desc = 'ROWS')
DECLARE @msg NVARCHAR(100)

RAISERROR('Runnng on [%s] %s GB',10,1,@DB,@size) WITH NOWAIT;

DROP TABLE IF EXISTS DataSummary;
SELECT 
RN = CAST(ROW_NUMBER() OVER(ORDER BY '['+c.TABLE_SCHEMA + '].[' + c.TABLE_NAME+']', c.ORDINAL_POSITION) AS NVARCHAR(MAX)),
'['+c.TABLE_SCHEMA + '].[' + c.TABLE_NAME+']' AS TableName,
c.COLUMN_NAME,										c.DATA_TYPE,
TopX			= CAST(NULL AS NVARCHAR(Max)),		0 AS RC,
RandValue		= CAST(NULL AS NVARCHAR(Max)),		0 AS CC,
[MinValue]		= CAST(NULL AS NVARCHAR(Max)),		0 AS ValueCnt,
[MaxValue]		= CAST(NULL AS NVARCHAR(Max)),		0 AS UnqCnt,
[MinLen]		= CAST(NULL AS NVARCHAR(Max)),		0 AS BlnkCnt,
[MaxLen]		= CAST(NULL AS NVARCHAR(Max)),		0 AS NullCnt,
AX = 'EXEC dbo.sp_tblanalytix ''' + c.TABLE_NAME+'''',
STT = 'SELECT TOP 10 [' + c.COLUMN_NAME +'] FROM ' + c.TABLE_NAME,
GBY = 'SELECT ['+c.COLUMN_NAME+'],COUNT(1) AS HitCount FROM ['+c.TABLE_NAME+'] GROUP BY ['+c.COLUMN_NAME+'] ORDER BY 2 DESC',
c.COLLATION_NAME,
c.ORDINAL_POSITION,
c.IS_NULLABLE,
c.COLUMN_DEFAULT
INTO DataSummary
FROM INFORMATION_SCHEMA.COLUMNS c JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_NAME = c.TABLE_NAME
AND c.TABLE_CATALOG = t.TABLE_CATALOG AND t.TABLE_TYPE = 'base table' 

IF (SELECT COUNT(1) FROM DataSummary WHERE [DATA_TYPE] IN ('text','ntext')) > 0 
BEGIN 
RAISERROR('
--<<< RUN FOLLOWING STATEMENTS TO DROP FULL TEXT INDEXES AND CONVERT UNSUPPORTED DATA TYPE TO NVARCHAR(MAX) >>>--',10,1)

DECLARE @DropFTI NVARCHAR(MAX) = ISNULL((SELECT STRING_AGG('ALTER FULLTEXT INDEX ON '+CONCAT('[',s.name,'].[',o.name,']')+' DISABLE '+CHAR(10)+'DROP FULLTEXT INDEX ON '+CONCAT('[',s.name,'].[',o.name,']'),CHAR(10))
FROM sys.fulltext_indexes fti 
JOIN sys.objects o ON o.object_id = fti.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id ),'')

SET @DropFTI += CHAR(10)+(SELECT STRING_AGG(CAST('ALTER TABLE '+tablename+' ALTER COLUMN ['+COLUMN_NAME+'] NVARCHAR(MAX)'  AS NVARCHAR(MAX)),CHAR(10)) FROM DataSummary WHERE [DATA_TYPE] IN ('ntext','text'))

RAISERROR('%s',10,1,@DropFTI) WITH NOWAIT;
end
ELSE 
BEGIN 
RAISERROR('
GETTING STATISTICS ON %s',10,1,@nl) WITH NOWAIT;

DECLARE @SummaryObjectPrint NVARCHAR(MAX) = ''
;WITH AllObjects AS
(SELECT type_DESC,name,create_date,relatedto = object_name(parent_object_id)
FROM sys.objects
), OK AS
(SELECT
CONCAT_WS(
'| ',
CAST(type_DESC AS CHAR(30)),
CAST(COUNT(1) AS CHAR(10) )+'|') AS RW
FROM AllObjects
GROUP BY type_DESC
)
SELECT @SummaryObjectPrint = CONCAT_WS(CHAR(13),'DATABASE OBJECTS SUMMARY',STRING_AGG(OK.RW,CHAR(13)),REPLICATE('-',60)) FROM OK

RAISERROR('%s',10,1,@SummaryObjectPrint) WITH NOWAIT;

DECLARE @SummaryTablePrint NVARCHAR(MAX) = ''
;WITH summary AS
(SELECT
datatype = UPPER(c.DATA_TYPE),
CASE
WHEN  c.DATA_TYPE IN ('CHAR','VARCHAR','NCHAR','NVARCHAR','NTEXT','TEXT')															THEN	'TEXT'
WHEN  c.DATA_TYPE IN ('BIGINT','DECIMAL','TINYINT','INT','SMALLINT','NUMERIC','FLOAT','DECIMAL','MONEY','SMALLMONEY','REAL')		THEN	'INT'
WHEN  c.DATA_TYPE IN ('UNIQUEIDENTIFIER')																							THEN	'GUID'
WHEN  c.DATA_TYPE IN ('DATETIME','DATETIME2','DATETIMEOFFSET','DATE')																THEN	'DATE'
WHEN  c.DATA_TYPE IN ('VARBINARY','BINARY','IMAGE','BIT','TIMESTAMP','XML','GEOGRAPHY')												THEN	'BIN'
ELSE c.DATA_TYPE
END AS SimplifiedType
FROM INFORMATION_SCHEMA.TABLES t
JOIN INFORMATION_SCHEMA.COLUMNS c ON c.TABLE_NAME = t.TABLE_NAME AND c.TABLE_SCHEMA = t.TABLE_SCHEMA WHERE t.TABLE_TYPE = 'base table'
), OK AS
(SELECT
CONCAT_WS('| ',CAST(summary.datatype AS CHAR(30)),CAST(summary.SimplifiedType AS CHAR(10)),CAST(COUNT(1) AS CHAR(10) )+'|') AS RW
FROM summary GROUP BY summary.datatype,summary.SimplifiedType)
SELECT @SummaryTablePrint = CONCAT_WS(CHAR(13),'TABLE COLUMNS DATA TYPE SUMMARY',STRING_AGG(OK.RW,CHAR(13)),REPLICATE('-',60)) FROM OK

RAISERROR('%s',10,1,@SummaryTablePrint) WITH NOWAIT;


DECLARE @startTime DATETIME 
DECLARE @sql NVARCHAR(MAX)

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET RC  = (SELECT COUNT(1) FROM '+TableName+' ) WHERE ORDINAL_POSITION = 1 and TableName = '''+TableName+'''' 
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE ORDINAL_POSITION = 1
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('01. Row Count %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

UPDATE DataSummary 
SET RC = (SELECT rc FROM DataSummary i WHERE i.TableName = e.tablename AND ORDINAL_POSITION = 1),
CC = (SELECT SUM(1) FROM DataSummary i WHERE i.TableName = e.tablename)
FROM DataSummary e

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET ValueCnt = (SELECT COUNT(1) FROM '+TableName+' WHERE ['+COLUMN_NAME+'] IS NOT NULL) WHERE TableName = '''+TableName+''' and column_name = '''+COLUMN_NAME+'''' 
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND DATA_TYPE NOT IN ('xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('02. Value Count %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET UnqCnt = (SELECT COUNT(distinct ['+COLUMN_NAME+']) FROM '+TableName+' WHERE ['+COLUMN_NAME+'] IS NOT NULL) WHERE TableName = '''+TableName+''' and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND  DATA_TYPE NOT IN ('xml','image','varbinary','geography','timestamp')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('03. Unique Count %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH


SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET BlnkCnt  = (SELECT COUNT(1) FROM '+TableName+' where datalength(['+COLUMN_NAME+']) = 0) WHERE TableName = '''+TableName+'''and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND  DATA_TYPE NOT IN ('xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('04. Blank Count %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET NullCnt  = (SELECT COUNT(1) FROM '+TableName+' where ['+COLUMN_NAME+'] IS NULL) WHERE TableName = '''+TableName+'''and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('05. Null Count %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET randValue  = (SELECT top 1 left(cast(['+COLUMN_NAME+'] as nvarchar(max)),100) FROM '+TableName+' TABLESAMPLE(1 PERCENT) where datalength(['+COLUMN_NAME+']) > 0) WHERE TableName = '''+TableName+'''and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND  DATA_TYPE NOT IN ('uniqueidentifier','timestamp','xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('06. Random value %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET MinValue = (SELECT MIN('+IIF(DATA_TYPE IN ('datetime','int','money','float'),'['+COLUMN_NAME+'])','CAST(['+COLUMN_NAME+'] AS NVARCHAR(MAX)))')+' FROM '+TableName+' WHERE DATALENGTH(['+COLUMN_NAME+']) > 0) WHERE TableName = '''+TableName+'''and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND  DATA_TYPE NOT IN ('uniqueidentifier','timestamp','xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('07. Min value %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE DataSummary SET MaxValue = (SELECT MAX('+IIF(DATA_TYPE IN ('datetime','int','money','float'),'['+COLUMN_NAME+'])','CAST(['+COLUMN_NAME+'] AS NVARCHAR(MAX)))')+' FROM '+TableName+' WHERE DATALENGTH(['+COLUMN_NAME+']) > 0) WHERE TableName = '''+TableName+'''and column_name = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary WHERE rc > 0 AND  DATA_TYPE NOT IN ('uniqueidentifier','timestamp','xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('08. Max value %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE  DataSummary SET MaxLen = '+IIF(DATA_TYPE IN ('datetime','uniqueidentifier'),'''''','(SELECT MAX(LEN(CAST(['+COLUMN_NAME+'] AS NVARCHAR(MAX)))) FROM '+TableName+' WHERE DATALENGTH(['+COLUMN_NAME+']) > 0)')+' WHERE TableName = '''+TableName+'''and [COLUMN_NAME] = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary  WHERE rc > 0 AND  DATA_TYPE not in ('uniqueidentifier','datetime','bit','timestamp','timestamp','xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('09. Max Len value %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG(CAST(
'UPDATE  DataSummary SET MinLen = '+IIF(DATA_TYPE IN ('datetime','uniqueidentifier'),'''''','(SELECT MIN(LEN(CAST(['+COLUMN_NAME+'] AS NVARCHAR(MAX)))) FROM '+TableName+' WHERE DATALENGTH(['+COLUMN_NAME+']) > 0)')+' WHERE TableName = '''+TableName+'''and  [COLUMN_NAME] = '''+COLUMN_NAME+''''
AS NVARCHAR(MAX)),CHAR(10))
FROM DataSummary  WHERE rc > 0 AND  DATA_TYPE not in ('uniqueidentifier','datetime','bit','timestamp','timestamp','xml','image','varbinary','geography')
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('10. Min len value %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SET @startTime = SYSDATETIME()
SET @sql = (SELECT STRING_AGG('
DECLARE @'+rn+' NVARCHAR(MAX) 
SELECT TOP 3 @'+rn+' = COALESCE(@'+rn+'+CHAR(149)+CHAR(10),'''')+CAST(COUNT(1) AS NVARCHAR(20))+''  -  ''+CAST(['+COLUMN_NAME+'] AS NVARCHAR(MAX)) 
FROM '+TableName+'
WHERE DATALENGTH(['+COLUMN_NAME+']) > 0 GROUP BY ['+COLUMN_NAME+'] ORDER BY COUNT(1) DESC;
update DataSummary
set topx = @'+rn+'
where tablename = '''+TableName+''' AND [COLUMN_NAME] = '''+COLUMN_NAME+'''',CHAR(10))
FROM DataSummary where DATA_TYPE NOT IN ('timestamp','xml','image','varbinary','geography') AND rc!=unqcnt AND rc > 0
)--PRINT @sql
BEGIN TRY
EXEC sp_executesql @sql
SET @msg = CONCAT(DATEDIFF(MILLISECOND,@startTime,SYSDATETIME())/1000.0 , 'sec elapsed.')
RAISERROR ('11. Top X %s', 10,1,@msg) WITH NOWAIT; 
END TRY
BEGIN CATCH
SELECT CONCAT_WS(CHAR(10),ERROR_MESSAGE(),@sql)
END CATCH

SELECT 
TRANSLATE(REPLACE(TableName,'[dbo].',''),'[]','  ') AS TableName,
COLUMN_NAME,
DATA_TYPE,
RC,
CC,
PopulatedCols = (SELECT COUNT(*) FROM DataSummary i WHERE i.TableName = e.tablename AND i.ValueCnt > 0),
ValueCnt,
UnqCnt,
BlnkCnt,
NullCnt,
TopX,
RandValue,
MinValue,
MaxValue,
concat_ws(' - ',MinLen,MaxLen,IIF(e.IS_NULLABLE='YES','(Y)','(N)'),'Dflt:'+COLUMN_DEFAULT) LengthRangeNullable,
AX,
STT,
GBY,
COLLATION_NAME,
ORDINAL_POSITION,
IS_NULLABLE,
PKCandidate = IIF(rc=e.UnqCnt AND RC> 0 AND e.DATA_TYPE IN ('INT','UNIQUEIDENTIFIER'),'ALTER TABLE '+TableName+' ADD CONSTRAINT PK_'+TableName+' PRIMARY KEY CLUSTERED (['+COLUMN_NAME+'])',null),
Relevant			= IIF(rc <2 AND e.UnqCnt < 2,0,1),
Comment				= CAST(NULL AS NVARCHAR(Max)),
--LikeColumns			= CAST(NULL AS NVARCHAR(Max)),
Scripted			= CAST(NULL AS NVARCHAR(Max)) 
--[Invenias TABLE]	= CAST(NULL AS NVARCHAR(Max)),
--[Invenias COLUMN]	= CAST(NULL AS NVARCHAR(Max))
FROM DataSummary e

DECLARE @idxsql NVARCHAR(MAX) = ''




DECLARE @RunMaintenance BIT = 0
DECLARE @maintenance NVARCHAR(MAX) = '
--create and update statistics 
EXEC sp_createstats  @fullscan =  ''fullscan'' ;
EXEC sp_updatestats'
IF @RunMaintenance = 1
EXEC sp_executesql @maintenance
ELSE
PRINT @maintenance
end