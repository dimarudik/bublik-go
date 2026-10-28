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
			unique (uuid, start_page, end_page, task_name, status))`
	DDL_CREATE_OUTBOX_TABLE = `create table %s (
            chunk_id bigint primary key,
            task_name varchar(128),
            copied bigint)`
	DDL_DROP_CHUNK_TABLE         = `DROP TABLE IF EXISTS %s`
	SQL_NUMBER_OF_TUPLES         = `SELECT reltuples::bigint, relpages::bigint FROM pg_class WHERE relnamespace::regnamespace::text = $1 AND relname = $2`
	DML_BATCH_INSERT_CHUNK_TABLE = `
	INSERT INTO %s (
		start_page, 
		end_page, 
		task_name, 
		status
	) 
	SELECT 
		n AS start_page, 
		CASE WHEN (n + $1::bigint < $2::bigint) THEN (n + $1::bigint) ELSE $2::bigint END AS end_page, 
		$3 AS task_name, 
		'UNASSIGNED' AS status
	FROM generate_series($4::bigint, $5::bigint, $6::bigint) AS n
	WHERE n <> $2::bigint`
	/*	DML_BATCH_INSERT_CHUNK_TABLE = `
		INSERT INTO %s (
			start_page, end_page, copied, task_name, schema_name, table_name, status, required
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	*/
	SQL_SELECT_UNASSIGNED_CHUNKS = `SELECT chunk_id, start_page, end_page, status FROM %s WHERE task_name = $1 AND status = 'UNASSIGNED' LIMIT $2 FOR UPDATE SKIP LOCKED`
	DML_LOCK_CHUNK               = `UPDATE %s SET status = 'LOCKED', uuid = $1, start_ts = NOW() WHERE chunk_id = $2`
	DML_COMPLETE_CHUNK           = `UPDATE %s SET status = 'PROCESSED', copied = $1, end_ts = NOW() WHERE chunk_id = $2`
	DML_ERROR_CHUNK              = `UPDATE %s SET status = 'PROCESSED_WITH_ERROR', err_msg = $1, end_ts = NOW() WHERE chunk_id = $2`
)
