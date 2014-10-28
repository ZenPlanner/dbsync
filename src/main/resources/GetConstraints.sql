select
	object_name(so.parent_obj) as table_name,
	so.name as constraint_name,
	so.xtype
from sysconstraints c
inner join sysobjects so
	on c.constid=so.id
where so.xtype != 'D' and so.xtype != 'PK' and so.xtype != 'UQ'
	and objectproperty(so.id, 'CnstIsDisabled')=0
order by table_name;

