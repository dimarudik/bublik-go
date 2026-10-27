package postgres

const (
	DDL_CREATE_CHUNK_TABLE = `create table %s (
                    chunk_id bigint generated always as identity primary key, 
                    uuid varchar(36), 
                    start_page bigint, 
                    end_page bigint, 
                    schema_name varchar(128), 
                    table_name varchar(256), 
                    required bigint, 
                    copied bigint, 
                    task_name varchar(128), 
                    status varchar(20)  default 'UNASSIGNED', 
                    start_ts timestamp, 
                    end_ts timestamp, 
                    err_msg varchar(2048), 
                    unique (uuid, start_page, end_page, task_name, status));`
	DDL_DROP_CHUNK_TABLE         = `DROP TABLE IF EXISTS %s`
	SQL_NUMBER_OF_TUPLES         = `SELECT reltuples::bigint, relpages::bigint FROM pg_class WHERE relnamespace::regnamespace::text = $1 AND relname = $2`
	DML_BATCH_INSERT_CHUNK_TABLE = `insert into %s (start_page, end_page, copied, task_name, schema_name, table_name, status, required) 
			 (select * from (
				 select 
					 n as start_page, 
					 n + $1 as end_page,   -- Аргумент 1: chunkSize
					 $2 as copied,         -- Аргумент 2: initial copied rows (0)
					 $3 as task_name,      -- Аргумент 3: taskName
					 $4 as schema_name,    -- Аргумент 4: schema
					 $5 as table_name,     -- Аргумент 5: table
					 $6 as status,         -- Аргумент 6: 'UNASSIGNED'
					 $7 as required       -- Аргумент 7: initial required (0)
					 from generate_series($8, $9, $10) as n -- Аргументы 8, 9, 10: start_page, stop_page, step
			 ) c where start_page <> end_page)`
	SQL_SELECT_UNASSIGNED_CHUNKS = `SELECT chunk_id, start_page, end_page, status FROM %s WHERE task_name = $1 AND status = 'UNASSIGNED' LIMIT $2 FOR UPDATE SKIP LOCKED`
	DML_LOCK_CHUNK               = `UPDATE %s SET status = 'LOCKED', uuid = $1, start_ts = NOW() WHERE chunk_id = $2`
	DML_COMPLETE_CHUNK           = `UPDATE %s SET status = 'PROCESSED', copied = $1, end_ts = NOW() WHERE chunk_id = $2`
	DML_ERROR_CHUNK              = `UPDATE %s SET status = 'PROCESSED_WITH_ERROR', err_msg = $1, end_ts = NOW() WHERE chunk_id = $2`
)
