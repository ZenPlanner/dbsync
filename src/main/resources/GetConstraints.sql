select
	table_schema as schema_name,
	object_name(so.parent_obj) as table_name,
	so.name as constraint_name,
	cc.COLUMN_NAME
from sysconstraints c
inner join sysobjects so
	on c.constid=so.id
inner join information_schema.constraint_column_usage cc 
	on so.name = cc.constraint_name
where so.xtype = 'UQ'
order by schema_name, table_name, constraint_name
;

SELECT
    sch.name AS [schema_name],
	  obj.name AS FK_NAME,
    tab1.name AS [table],
    col1.name AS [column],
    tab2.name AS [referenced_table],
    col2.name AS [referenced_column],
	  objectproperty(constraint_object_id, 'CnstIsDisabled') as disabled
FROM sys.foreign_key_columns fkc
INNER JOIN sys.objects obj
    ON obj.object_id = fkc.constraint_object_id
INNER JOIN sys.tables tab1
    ON tab1.object_id = fkc.parent_object_id
INNER JOIN sys.schemas sch
    ON tab1.schema_id = sch.schema_id
INNER JOIN sys.columns col1
    ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id
INNER JOIN sys.tables tab2
    ON tab2.object_id = fkc.referenced_object_id
INNER JOIN sys.columns col2
    ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id
order by schema_name, FK_NAME
;
    