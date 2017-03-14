-- rows : Number of rows existing in the table. If the object specified is a Service Broker queue, this column indicates the number of messages in the queue.
-- reserved : Total amount of reserved space for objname.
-- data : Total amount of space used by data in objname.
-- index_size : Total amount of space used by indexes in objname.
-- unused : Total amount of space reserved for objname but not yet used.
-- unused : Total amount of space reserved for objname but not yet used.

DBCC UPDATEUSAGE (0) WITH NO_INFOMSGS;
CREATE TABLE
    #temp (
          [name] varchar(250),
          [rows] varchar(50),
          [reserved] varchar(50),
          [data] varchar(50),
          [index_size] varchar(50),
          [unused] varchar(50)
          );

INSERT #temp EXEC ('sp_MSforeachtable ''sp_spaceused ''''?''''''');

UPDATE
    #temp
SET
    [rows] = LTRIM(RTRIM(REPLACE(t.rows,'KB',''))),
    [reserved] = LTRIM(RTRIM(REPLACE(t.reserved,'KB',''))),
    [data] = LTRIM(RTRIM(REPLACE(t.data,'KB',''))),
    [index_size] = LTRIM(RTRIM(REPLACE(t.index_size,'KB',''))),
    [unused] = LTRIM(RTRIM(REPLACE(t.unused,'KB','')))
FROM #temp AS t;

SELECT
    SUM(CAST([reserved] as decimal))/1024 AS 'Total reserved MB',
    SUM(CAST([data] as decimal))/1024 AS 'Total data MB',
    SUM(CAST([index_size] as decimal))/1024 AS 'Total index_size MB',
    SUM(CAST([unused] as decimal))/1024 AS 'Total unused MB'
FROM
    #temp;

SELECT
    [name] ,
    CAST([rows] as INT)'rows' ,CAST([reserved] as INT)/1024 'reserved MB',
    CAST([data] as INT)/1024 'data MB' ,
    CAST([index_size]/1024 as INT)'index_size MB',
    CAST([unused] as INT)/1024 'unused MB'
FROM
    #temp
ORDER BY
    CAST(reserved as INT) DESC;

DROP TABLE #temp;
