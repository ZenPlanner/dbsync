SELECT
  tab.name AS table_name,
  col.name AS column_name,
  typ.name AS DATA_TYPE,
  CASE
  WHEN pk.COLUMN_NAME IS NOT NULL THEN 1
  ELSE 0
  END      AS primary_key
FROM sysobjects tab
  INNER JOIN syscolumns col ON col.id = tab.id
  INNER JOIN systypes typ ON typ.xtype = col.xtype AND typ.xusertype = col.xusertype
  LEFT JOIN (
              SELECT
                tc.TABLE_NAME,
                ccu.COLUMN_NAME
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                  ON tc.CONSTRAINT_NAME = ccu.Constraint_name
              WHERE tc.CONSTRAINT_TYPE = 'Primary Key'
            ) pk
    ON pk.TABLE_NAME = tab.name
       AND pk.COLUMN_NAME = col.name
WHERE tab.type = 'U'
ORDER BY table_name, column_name;
