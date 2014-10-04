package com.zenplanner.sql;

import java.util.Arrays;
import java.util.List;

public class Column {
    private static final List<String> smallTypes = Arrays.asList(new String[]{
            "uniqueidentifier", "bigint", "date", "datetime", "datetime2", "smalldatetime", "tinyint", "smallint",
            "int", "decimal", "bit", "money", "smallmoney", "char", "float", "image"
    });
    private static final List<String> bigTypes = Arrays.asList(new String[]{"varchar", "nvarchar", "text"});

    private String columnName;
    private String dataType;
    private boolean isPrimaryKey;

    /**
     * @return the SQL to add to a select clause for the given column
     */
    public String getSelect() {
        if (smallTypes.contains(getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', " +
                    "case when [%s] is null then convert(varbinary,0) " +
                    "else convert(varbinary, [%s]) end)", getColumnName(), getColumnName());
        }
        if (bigTypes.contains(getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', " +
                    "case when [%s] is null then convert(varbinary,0) " +
                    "else convert(nvarchar(max), [%s]) end)", getColumnName(), getColumnName());
        }
        throw new RuntimeException("Unknown type: " + getDataType());
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

}
