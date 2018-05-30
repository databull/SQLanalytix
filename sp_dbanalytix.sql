CREATE PROCEDURE [dbo].[sp_dbAnalytix] --USE master   EXEC sp_MS_marksystemobject sp_dbAnalytix
	@OrderBy INT = 1
    	,@DoExactCounts BIT  = 0
	,@TableNameLike NVARCHAR(100) = '%'
WITH RECOMPILE
AS
BEGIN

    SET NOCOUNT ON

    CREATE TABLE #MigTableSizeCountTables
    (
         tableName NVARCHAR(MAX)
        ,Rc INT
        ,[Size] INT
	,Cc INT
	,SF NVARCHAR(MAX)
        ,RN INT
    )

    DECLARE @DropCMD NVARCHAR(MAX)
    DECLARE @SQL NVARCHAR(MAX) = ''

    ;WITH extra AS
    (   -- Get info for FullText indexes, XML Indexes, etc
        SELECT  sit.[object_id],
                sit.[parent_id],
                ps.[index_id],
                SUM(ps.reserved_page_count) AS [reserved_page_count],
                SUM(ps.used_page_count) AS [used_page_count]
        FROM    sys.dm_db_partition_stats ps
        INNER JOIN  sys.internal_tables sit
                ON  sit.[object_id] = ps.[object_id]
        WHERE   sit.internal_type IN
                   (202, 204, 207, 211, 212, 213, 214, 215, 216, 221, 222, 236)
        GROUP BY    sit.[object_id],
                    sit.[parent_id],
                    ps.[index_id]
    ), agg AS
    (   -- Get info for Tables, Indexed Views, etc (including "extra")
        SELECT  ps.[object_id] AS [ObjectID],
                ps.index_id AS [IndexID],
                SUM(ps.in_row_data_page_count) AS [InRowDataPageCount],
                SUM(ps.used_page_count) AS [UsedPageCount],
                SUM(ps.reserved_page_count) AS [ReservedPageCount],
                SUM(ps.row_count) AS [RowCount],
                SUM(ps.lob_used_page_count + ps.row_overflow_used_page_count)
                        AS [LobAndRowOverflowUsedPageCount]
        FROM    sys.dm_db_partition_stats ps
        GROUP BY    ps.[object_id],
                    ps.[index_id]
        UNION ALL
        SELECT  ex.[parent_id] AS [ObjectID],
                ex.[object_id] AS [IndexID],
                0 AS [InRowDataPageCount],
                SUM(ex.used_page_count) AS [UsedPageCount],
                SUM(ex.reserved_page_count) AS [ReservedPageCount],
                0 AS [RowCount],
                0 AS [LobAndRowOverflowUsedPageCount]
        FROM    extra ex
        GROUP BY    ex.[parent_id],
                    ex.[object_id]
    ), spaceused AS
    (
    SELECT  agg.[ObjectID],
            OBJECT_SCHEMA_NAME(agg.[ObjectID]) AS [SchemaName],
            OBJECT_NAME(agg.[ObjectID]) AS [TableName],
            SUM(CASE
                    WHEN (agg.IndexID < 2) THEN agg.[RowCount]
                    ELSE 0
                END) AS [Rows],
            SUM(agg.ReservedPageCount) * 8 AS [ReservedKB],
            SUM(agg.LobAndRowOverflowUsedPageCount +
                CASE
                    WHEN (agg.IndexID < 2) THEN (agg.InRowDataPageCount)
                    ELSE 0
                END) * 8 AS [DataKB],
            SUM(agg.UsedPageCount - agg.LobAndRowOverflowUsedPageCount -
                CASE
                    WHEN (agg.IndexID < 2) THEN agg.InRowDataPageCount
                    ELSE 0
                END) * 8 AS [IndexKB],
            SUM(agg.ReservedPageCount - agg.UsedPageCount) * 8 AS [UnusedKB],
            SUM(agg.UsedPageCount) * 8 AS [UsedKB]
    FROM    agg
    GROUP BY    agg.[ObjectID],
                OBJECT_SCHEMA_NAME(agg.[ObjectID]),
                OBJECT_NAME(agg.[ObjectID])
    )
	INSERT INTO #MigTableSizeCountTables 	
      SELECT -- sp.SchemaName,
           sp.TableName,
           sp.[Rows],
           (sp.DataKB+sp.IndexKB+sp.UnusedKB) / 1024  AS [AllMB],
		   [Columns]=(SELECT COUNT(column_name) FROM [INFORMATION_SCHEMA].[COLUMNS] cl WHERE cl.[TABLE_NAME] = sp.TableName),
		   [SelectFrom]='SELECT top 10 * from ' + sp.TableName + REPLICATE(' ',5+(SELECT MAX(LEN(TableName)) FROM spaceused)-LEN(sp.tablename))+'--'+CAST(sp.[Rows] AS VARCHAR(MAX)),
		    ROW_NUMBER() OVER(PARTITION BY sp.TableName ORDER BY sp.UsedKB DESC) AS RN
    FROM spaceused sp
    INNER JOIN sys.objects so ON so.[object_id] = sp.ObjectID
    WHERE (so.is_ms_shipped = 0)
    AND (so.[name] NOT LIKE 'dt%' )     -- ????
    AND (so.OBJECT_ID > 255)            -- ????
    AND (so.[name] LIKE '%'+@TableNameLike+'%')


    IF @DoExactCounts > 0
    BEGIN
        SELECT @SQL = @SQL + '

            RAISERROR(''Fetching true rowcount for '+QUOTENAME(tablename)+''',0,1) WITH NOWAIT

            UPDATE #MigTableSizeCountTables SET
               rc = ( SELECT COUNT(*) FROM '+QUOTENAME(tablename)+' )'                   -- rc is approximate only, how much approximate is not documented :-)
        +'  WHERE QUOTENAME(tablename) = '''+QUOTENAME(tablename)+''''

        FROM #MigTableSizeCountTables

        EXEC(@sql)
    END

    SET @SQL = '
	    SELECT tableName,'''' Notes, Rc, Cc,[Size],Sf, ''EXEC sp_tblAnalytix ''''''+TableName+'''''''' AS SelectAnalyze
	    FROM #MigTableSizeCountTables
	    WHERE RN = 1
        '

    IF @OrderBy BETWEEN 1 AND 5
        SET @SQL = @SQL + 'ORDER BY ' + CAST(@OrderBy AS NVARCHAR(MAX)) +' DESC'
    EXEC(@sql)

    SET @SQL = 'Total DB size (without unused space): '+CAST( (SELECT SUM([size]) AS TotalMB FROM #MigTableSizeCountTables) AS NVARCHAR(MAX))+' MB'
    PRINT @SQL

	SELECT @DropCMD = COALESCE(@DropCMD + ', ', '') + QUOTENAME((tablename))
	FROM #MigTableSizeCountTables
	WHERE rc = 0

	PRINT '
  TO DELETE TABLES WITH NO ROWS RUN BELOW COMMAND
  '
	PRINT 'DROP TABLE ' + @DropCMD

    DROP TABLE #MigTableSizeCountTables
END
