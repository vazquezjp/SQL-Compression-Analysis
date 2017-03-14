CREATE TABLE #ObjEst (
	PK int identity not null primary key,
	object_name sysname,
	schema_name sysname,
	index_id INT,
	partition_number int,
	size_with_current_compression_setting bigint,
	size_with_requested_compression_setting bigint,
	sample_size_with_current_compression_setting bigint,
	sample_size_with_requested_compresison_setting bigint
);

TRUNCATE TABLE dba.dbo.tblCompressAnalysis;

INSERT INTO DBA.dbo.tblCompressAnalysis (
	database_name
	, schema_name
	, object_name
	, index_id
	, ixName
	, ixType
	, partition_number
	, data_compression_desc
	, u_val
	, s_val
	)
SELECT db_name(ios.database_id) as [database_name]
	, s.name as [schema_name]
	, o.name AS [oject_name]
	, ios.index_id
	, i.name AS [ixName]
	, i.type_desc AS [ixType]
	, ios.partition_number AS [Partition]
	, p.data_compression_desc
	, cast(ios.leaf_update_count * 100.0 /
           (ios.range_scan_count + ios.leaf_insert_count
            + ios.leaf_delete_count + ios.leaf_update_count
            + ios.leaf_page_merge_count + ios.singleton_lookup_count
           ) as numeric(5,2)) AS u_val
	, cast(ios.range_scan_count * 100.0 /
           (ios.range_scan_count + ios.leaf_insert_count
            + ios.leaf_delete_count + ios.leaf_update_count
            + ios.leaf_page_merge_count + ios.singleton_lookup_count
           ) as numeric(5,2)) AS s_val
FROM sys.dm_db_index_operational_stats (db_id(), NULL, NULL, NULL) ios
	JOIN sys.objects o ON o.object_id = ios.object_id
	join sys.schemas s on o.schema_id = s.schema_id
	JOIN sys.indexes i ON i.object_id = ios.object_id AND i.index_id = ios.index_id
	join sys.partitions as p on i.object_id = p.object_id and i.index_id= p.index_id
WHERE (ios.range_scan_count + ios.leaf_insert_count
		   + ios.leaf_delete_count + leaf_update_count
		   + ios.leaf_page_merge_count + ios.singleton_lookup_count) != 0
	AND objectproperty(ios.object_id,'IsUserTable') = 1;

-- Determine Compression Estimates 
DECLARE
	@PK INT,
	@db_name varchar(250),
	@Schema varchar(150),
	@object varchar(150),
	@DAD varchar(25),
	@partNO int,
	@indexID int,
	@SQL nVARCHAR(max);
 
DECLARE cCompress CURSOR FAST_FORWARD FOR 
	SELECT database_name, schema_name, object_name, index_id, partition_number, data_compression_desc
	FROM DBA.dbo.tblCompressAnalysis
	WHERE database_name = DB_NAME(db_id());
   
OPEN cCompress;
FETCH cCompress INTO @db_name, @Schema, @object, @indexID, @partNO, @DAD;
	WHILE @@Fetch_Status = 0 
    BEGIN
		IF @DAD = 'NONE'
		BEGIN
			-- estimate Page compression
			INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
            EXEC sp_estimate_data_compression_savings
				@schema_name = @Schema,
				@object_name = @object,
				@index_id = @indexID,
				@partition_number = @partNO,
				@data_compression = 'PAGE';
	            
            UPDATE DBA.dbo.tblCompressAnalysis
            SET none_size = O.size_with_current_compression_setting,
				page_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
				ON  D.Schema_name = O.Schema_Name
					and D.Object_name = O.object_name
					and D.index_id = O.index_id
					and D.partition_number = O.partition_number
					and D.database_name = @db_name;

             DELETE #ObjEst;

             -- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
			EXEC sp_estimate_data_compression_savings
                @schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'ROW';
                
            UPDATE DBA.dbo.tblCompressAnalysis
            SET row_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
                ON  D.Schema_name = O.Schema_Name
					and D.Object_name = O.object_name
					and D.index_id = O.index_id
					and D.partition_number = O.partition_number  
					and D.database_name = @db_name;

            DELETE #ObjEst;
        END -- none compression estimate
 
		IF @DAD = 'ROW'
		BEGIN 
            -- estimate Page compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
            EXEC sp_estimate_data_compression_savings
                @schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'PAGE';
                
            UPDATE DBA.dbo.tblCompressAnalysis
            SET row_size = O.size_with_current_compression_setting,
                    page_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
                    ON  D.Schema_name = O.Schema_Name
						and D.Object_name = O.object_name
						and D.index_id = O.index_id
						and D.partition_number = O.partition_number  
						and D.database_name = @db_name;

            DELETE #ObjEst;
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
            EXEC sp_estimate_data_compression_savings
                @schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'NONE';
                
            UPDATE DBA.dbo.tblCompressAnalysis
            SET none_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
                ON  D.Schema_name = O.Schema_Name
					and D.Object_name = O.object_name
					and D.index_id = O.index_id
					and D.partition_number = O.partition_number  
					and D.database_name = @db_name;

            DELETE #ObjEst;
        END -- row compression estimate     
      
		IF @DAD = 'PAGE'
		BEGIN 
			-- estimate Row compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
            EXEC sp_estimate_data_compression_savings
                @schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'ROW';

            UPDATE DBA.dbo.tblCompressAnalysis
            SET page_size = O.size_with_current_compression_setting,
                    row_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
                ON  D.Schema_name = O.Schema_Name
					and D.Object_name = O.object_name
					and D.index_id = O.index_id
					and D.partition_number = O.partition_number  
					and D.database_name = @db_name;

             DELETE #ObjEst;
             
             -- estimate None compression
            INSERT #ObjEst (object_name,schema_name,index_id,partition_number,size_with_current_compression_setting,size_with_requested_compression_setting,sample_size_with_current_compression_setting,sample_size_with_requested_compresison_setting )
            EXEC sp_estimate_data_compression_savings
                @schema_name = @Schema,
                @object_name = @object,
                @index_id = @indexID,
                @partition_number = @partNO,
                @data_compression = 'NONE';
                
			UPDATE DBA.dbo.tblCompressAnalysis
            SET none_size = O.size_with_requested_compression_setting
            FROM DBA.dbo.tblCompressAnalysis D JOIN #ObjEst O
                ON  D.Schema_name = O.Schema_Name
					and D.Object_name = O.object_name
					and D.index_id = O.index_id
					and D.partition_number = O.partition_number  
					and D.database_name = @db_name;

			DELETE #ObjEst;
        END; -- page compression estimate 

       FETCH cCompress INTO @db_name, @Schema, @object, @indexID, @partNO, @DAD;
    END;

CLOSE cCompress;
DEALLOCATE cCompress;

DROP TABLE #ObjEst;
  
--Reporting Section
--Update tblCompressAnalysis
UPDATE dba.dbo.tblCompressAnalysis
SET savings_row_percent = CAST((1-(cast(Row_Size as float) / none_Size))*100 as int)
	, savings_page_percent = CAST((1-(cast(page_Size as float) / none_Size))*100 as int)
	, decision = CASE 
       WHEN (1-(cast(Row_Size as float) / none_Size)) >= .25 and (Row_Size <= Page_Size) then 'Row' 
       WHEN (1-(cast(page_Size as float) / none_Size)) >= .25 and (Page_Size <= row_Size) then 'Page' 
       ELSE 'None' 
     END
WHERE None_Size <> 0 
	and database_name = DB_NAME(db_id());
 
--Report findings
SELECT database_name
	, schema_name
	, object_name as [Object]
	, index_id
	, ixName
	, ixType
	, partition_number
	, data_compression_desc as Current_Compression
	, savings_row_percent --CAST((1-(cast(Row_Size as float) / none_Size))*100 as int)  as RowGain
	, savings_page_percent --CAST((1-(cast(page_Size as float) / none_Size))*100 as int) as PageGain
	, s_val
	, u_val
	, decision
	, case 
		when ([decision] != 'None') and ([data_compression_desc] <> [decision])
			then case
				when [index_id] = 0
					then 'ALTER TABLE [' + [schema_name] + '].[' + [object_name] + '] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = ' + UPPER([decision]) + ');'
				else 'ALTER INDEX [' + [ixName] + '] ON ['+ [schema_name] + '].[' + [object_name] + '] REBUILD PARTITION = ALL WITH ( PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON, ONLINE = OFF, SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = ' + UPPER([decision]) + ');'
			end
		else ''
	end AS [Scripts]
FROM DBA.dbo.tblCompressAnalysis
WHERE None_Size <> 0
	and (savings_page_percent + savings_row_percent) > 0	
	and (s_val > 75 and u_val < 20)
	or (s_val < 10 and u_val < 5)
	and database_name = DB_NAME(db_id()) --@db_name
ORDER BY [Object];
