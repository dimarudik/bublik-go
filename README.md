```shell
docker run --name postgres \
-h postgres \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=postgres \
-e POSTGRES_DB=postgres \
-p 5432:5432 \
-d postgres  \ 
-c shared_preload_libraries="pg_stat_statements,auto_explain"
```