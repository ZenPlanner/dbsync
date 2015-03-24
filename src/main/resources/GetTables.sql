SELECT    tab.[name] AS "table_name",
          col.[name] AS "column_name",
          typ.[name] AS "data_type",
         CASE
            WHEN pk.[column_name] IS NOT NULL THEN 1
           ELSE 0
         END AS "primary_key"
FROM     sys.tables tab
  INNER JOIN sys.columns col ON col.[object_id] = tab.[object_id]
  INNER JOIN sys.types typ ON typ.[system_type_id] = col.[system_type_id]
    AND typ.[user_type_id] = col.[user_type_id]
  LEFT OUTER JOIN (
    SELECT  tc.[table_name],
            ccu.[column_name]
    FROM  information_schema.table_constraints tc
      INNER JOIN information_schema.constraint_column_usage ccu ON tc.[constraint_name] = ccu.[constraint_name]
    WHERE   tc.[constraint_type] = 'Primary Key'
  ) pk ON pk.[table_name] = tab.[name] AND pk.[column_name] = col.[name]
WHERE     tab.[type] = 'U'
  AND     tab.[name] NOT LIKE '!_%' ESCAPE '!'
  AND     col.[is_identity] = 0
ORDER BY  [table_name],
          [column_name]
