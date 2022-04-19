-- justin.thomson@viome.com
-- SQL used to create ERD information for import into lucidchart

-- Lucidchart doesn't seem to support cross-schema constraints 
-- create table names that include schema for better usage in lucidchart 

-- get list of FKs with PKs including column FK constraints
WITH tt as (
SELECT n.nspname as "from_schema", conrelid::regclass::text AS "from_table"
      ,CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) END AS "from_column"
      ,trim(both '"' from CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1) END) AS "to_table"
      ,trim(both '"' from CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1) END) AS "to_column"
FROM   pg_constraint c
JOIN   pg_namespace n ON n.oid = c.connamespace
WHERE  contype IN ('f')
AND pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %'
ORDER  BY pg_get_constraintdef(c.oid), conrelid::regclass::text, contype DESC
)

SELECT 'postgresql' AS dbms,t.table_catalog,
'public' as table_schema, --t.table_schema
CONCAT(t.table_schema, '.', t.table_name) as table_name,c.column_name,
c.ordinal_position,c.data_type,c.character_maximum_length,
case when tt.from_table is null then null else 'f' end as constraint_type,
case when tt.from_table is null then null else 'public' end as table_schema, --case when tt.from_table is null then null else t2.table_schema end as table_schema,
case when tt.from_table is null then null else CONCAT(tjn.table_schema, '.', tt.to_table) end as table_name, 
tt.to_column as column_name
FROM information_schema.tables t 
left JOIN information_schema.columns c on t.table_name=c.table_name and t.table_schema=c.table_schema
left join tt on tt.from_table=t.table_name AND tt.from_column=c.column_name
left join information_schema.tables tjn on tjn.table_name=tt.to_table
WHERE t.TABLE_TYPE='BASE TABLE' AND  t.table_schema NOT IN('information_schema','pg_catalog')
ORDER BY 3,4,5;


