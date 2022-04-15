-- justin.thomson@viome.com
-- this version is not used due to lucidchart limitation. Maybe in the future...

-- get list of FKs with PKs including column FK constraints
WITH tt as (
SELECT n.nspname as "from_schema", conrelid::regclass::text AS "from_table"
      ,CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), 14, position(')' in pg_get_constraintdef(c.oid))-14) END AS "from_column"
     --, 'public' as "to_schema" -- hack for testing 
      ,trim(both '"' from CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position(' REFERENCES ' in pg_get_constraintdef(c.oid))+12, position('(' in substring(pg_get_constraintdef(c.oid), 14))-position(' REFERENCES ' in pg_get_constraintdef(c.oid))+1) END) AS "to_table"
      ,trim(both '"' from CASE WHEN pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %' THEN substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14, position(')' in substring(pg_get_constraintdef(c.oid), position('(' in substring(pg_get_constraintdef(c.oid), 14))+14))-1) END) AS "to_column"
FROM   pg_constraint c
JOIN   pg_namespace n ON n.oid = c.connamespace
WHERE  contype IN ('f')
AND pg_get_constraintdef(c.oid) LIKE 'FOREIGN KEY %'
ORDER  BY pg_get_constraintdef(c.oid), conrelid::regclass::text, contype DESC
)
--select 'public' as table_schema, trim(both '"' from to_table) as table_name, trim(both '"' from to_column) as column_name from tt where tt.from_table='test'::regclass;

SELECT 'postgresql' AS dbms,t.table_catalog,
t.table_schema, --t.table_schema, hack for now. can add schemas later 
t.table_name,c.column_name,
c.ordinal_position,c.data_type,c.character_maximum_length,
case when tt.from_table is null then null else 'f' end as constraint_type,
case when tt.from_table is null then null else t2.table_schema end as table_schema,
tt.to_table as table_name, tt.to_column as column_name
FROM information_schema.tables t 
left JOIN information_schema.columns c on t.table_name=c.table_name and t.table_schema=c.table_schema
left join tt on tt.from_table=t.table_name AND tt.from_column=c.column_name
left join information_schema.tables t2 on t2.table_name = tt.to_table
WHERE t.TABLE_TYPE='BASE TABLE' AND  t.table_schema NOT IN('information_schema','pg_catalog');


