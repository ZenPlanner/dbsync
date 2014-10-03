package com.zenplanner;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class App {
    private static final String filterCol = "partitionId";
    private static final List<String> smallTypes = Arrays.asList(new String[]{
            "uniqueidentifier", "bigint", "date", "datetime", "datetime2", "smalldatetime", "tinyint", "smallint",
            "int", "decimal", "bit", "money", "smallmoney", "char", "float"
    });
    private static final List<String> bigTypes = Arrays.asList(new String[]{
            "varchar", "nvarchar", "text"
    });

    public static void main(String[] args) throws Exception {
        String filterValue = args[0];
        String srcCon = args[1];
        String dstCon = args[2];

        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        // Get tables and columns
        try (Connection scon = DriverManager.getConnection(srcCon)) {
            try (Connection dcon = DriverManager.getConnection(dstCon)) {
                Map<String, Table> srcTables = filterTables(getTables(scon));
                Map<String, Table> dstTables = filterTables(getTables(dcon));
                for (Table srcTable : srcTables.values()) {
                    if(!dstTables.containsKey(srcTable.getName())) {
                        continue;
                    }
                    Table dstTable = dstTables.get(srcTable.getName());
                    compTables(srcTable, dstTable);
                }
            }
        }
    }

    private static void compTables(Table srcTable, Table dstTable) {
        Table lcd = findLcd(srcTable, dstTable);
        String sql = writeHashedQuery(lcd);
    }

    private static Table findLcd(Table srcTable, Table dstTable) {
        Table table = new Table(srcTable.getName());
        Set<String> colNames = new HashSet<>();
        colNames.addAll(srcTable.keySet());
        colNames.addAll(dstTable.keySet());
        for(String colName : colNames) {
            if(!srcTable.containsKey(colName) || !dstTable.containsKey(colName)) {
                continue;
            }
            table.put(colName, srcTable.get(colName));
        }
        return table;
    }

    private static String writeHashedQuery(Table table) {
        List<String> colNames = new ArrayList<>();
        List<String> pk = new ArrayList<>();
        for (Column col : table.values()) {
            if(col.isPrimaryKey()) {
                pk.add("[" + col.getColumnName() + "]");
            }
            colNames.add(getColSelect(col));
        }
        String selectClause = Joiner.on(",\n\t").join(colNames);
        String orderClause = Joiner.on(",").join(pk);
        String sql = String.format("select\n\t%s from [%s]\nwhere [%s]=?\norder by %s",
                selectClause, table.getName(), filterCol, orderClause);
        return sql;
    }

    private static String getColSelect(Column col) {
        if (smallTypes.contains(col.getDataType().toLowerCase())) {
            return "[" + col.getColumnName() + "]";
        }
        if (bigTypes.contains(col.getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', convert(nvarchar(max), [%s])) as [%s]",
                    col.getColumnName(), col.getColumnName());
        }
        throw new RuntimeException("Unknown type: " + col.getDataType());
    }

    // Removes tables that don't have a primary key, or aren't related to partitionId
    private static Map<String, Table> filterTables(Map<String, Table> in) {
        Map<String, Table> out = new HashMap<>();
        for (Map.Entry<String, Table> entry : in.entrySet()) {
            boolean hasPk = false;
            boolean hasCol = false;
            String name = entry.getKey();
            Table table = entry.getValue();
            for (Column col : table.values()) {
                if (col.isPrimaryKey()) {
                    hasPk = true;
                }
                if (filterCol.equalsIgnoreCase(col.getColumnName())) {
                    hasCol = true;
                }
            }
            if (hasPk && hasCol) {
                out.put(name, table);
            }
        }
        return out;
    }

    private static Map<String, Table> getTables(Connection con) throws Exception {
        // Collect the tables
        Map<String, Table> tables = new HashMap<>();
        try (Statement stmt = con.createStatement()) {
            String sql = Resources.toString(Resources.getResource("GetTables.sql"), Charsets.UTF_8);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    if (!tables.containsKey(tableName)) {
                        tables.put(tableName, new Table(tableName));
                    }
                    Table table = tables.get(tableName);

                    Column col = new Column();
                    String colName = rs.getString("column_name");
                    col.setColumnName(colName);
                    col.setDataType(rs.getString("data_type"));
                    col.setPrimaryKey(rs.getBoolean("primary_key"));
                    table.put(colName, col);
                }
            }
        }
        return tables;
    }
}
