DECLARE @filename NVARCHAR(MAX) = 'alias'
--SET @filename = '''F:\Transearch\CSVs\'+@filename+'.csv'''
--PRINT	@filename

DECLARE @val NVARCHAR(MAX);
;WITH tableColumnNameFromHeaderRow AS
(
SELECT TOP 1 IIF(s.value LIKE '%","%',REPLACE(s.value,'"',''),s.value) AS HeaderRow
--,DATALENGTH(bulkcolumn)/1024/1024 AS filesize
FROM OPENROWSET(BULK 'F:\Transearch\CSVs\person.csv', SINGLE_CLOB) AS x
CROSS APPLY STRING_SPLIT(x.BulkColumn,CHAR(13)) AS s
)
--SELECT 
--CONCAT('CREATE TABLE ',@filename,CHAR(13),'(',STRING_AGG(QUOTENAME([value]),' NVARCHAR(MAX)'+CHAR(13)+','),'')
----[value] AS ColumnName
--FROM tableColumnNameFromHeaderRow 
--CROSS APPLY STRING_SPLIT(headerrow,',') hr

Select @val = COALESCE(@val + ', ' + QUOTENAME([value])+' NVARCHAR(MAX)'+CHAR(13), QUOTENAME([value])+' NVARCHAR(MAX)'+CHAR(13)) 
FROM tableColumnNameFromHeaderRow 
CROSS APPLY STRING_SPLIT(headerrow,',') hr

SET @val = 'DROP TABLE IF EXISTS ['+@filename+'];'+CHAR(13)+' CREATE TABLE '+CHAR(13)+' ['+@filename+'] ('+@val+')'
+'select * FROM ['+@filename+'];'
--PRINT @val

EXEC (@val)
/*
bcp TestDatabase.dbo.myFirstImport format nul -c -f D:\BCP\myFirstImport.fmt -t, -T
bcp report.dbo.assignment format nul -T -n -f f:\new\assignment.fmt
REM Review file
Notepad f:\new\assignment.fmt


SELECT *
FROM OPENROWSET(BULK N'f:\new\alias.csv',
    FORMATFILE = N'f:\new\alias.fmt', 
    FIRSTROW=1, 
    FORMAT='CSV') AS cars;


	*/