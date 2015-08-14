package com.zenplanner.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class Column {
    private static final List<String> smallTypes = Arrays.asList(new String[]{
            "uniqueidentifier", "bigint", "date", "datetime", "datetime2", "smalldatetime", "tinyint", "smallint",
            "int", "decimal", "bit", "money", "smallmoney", "char", "float", "image", "nchar"
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
                    "else left(convert(nvarchar(max), [%s]), 500) end)", getColumnName(), getColumnName());
        }
        throw new RuntimeException("Unknown type: " + getDataType());
    }

    public Comparable<?> getValue(ResultSet rs) throws Exception {
        if("int".equals(dataType)) {
            return rs.getInt(columnName);
        }
        if("bigint".equals(dataType)) {
            return rs.getLong(columnName);
        }
        if("nchar".equals(dataType)) {
            return rs.getString(columnName);
        }
        if("varchar".equals(dataType)) {
            return rs.getString(columnName);
        }
        if("uniqueidentifier".equals(dataType)) {
            byte[] bytes = rs.getBytes(columnName);
            UUID uuid = UuidUtil.byteArrayToUuid(bytes);
            return uuid;
        }
        if("date".equals(dataType))
        {
        	return rs.getDate(columnName);
        }
        throw new RuntimeException("Type not recognized: " + dataType);
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
