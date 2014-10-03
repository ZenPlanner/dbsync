package com.zenplanner;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import sun.misc.IOUtils;

import java.sql.*;
import java.util.*;

public class App {
    private static final String filterCol = "partitionId";
    private static final List<String> smallTypes = Arrays.asList(new String[]{
            "uniqueidentifier",
            "bigint",
            "date",
            "datetime",
            "datetime2",
            "smalldatetime",
            "tinyint",
            "smallint",
            "int",
            "decimal",
            "bit",
            "money",
            "smallmoney",
            "char",
            "float"
    });
    private static final List<String> bigTypes = Arrays.asList(new String[]{
            "varchar",
            "nvarchar",
            "text"
    });

    public static void main(String[] args) throws Exception {
        String filterValue = args[0];
        String sourceCon = args[1];
        //String destCon = args[2];

        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        // Get tables and columns
        try (Connection scon = DriverManager.getConnection(sourceCon)) {
            Map<String, Table> sourceTables = filterTables(getTables(scon));
            for (Map.Entry<String, Table> entry : sourceTables.entrySet()) {
                String tableName = entry.getKey();
                Table table = entry.getValue();
                String sql = writeHashedQuery(tableName, table, filterValue);
                System.out.println("-----------------------------------------------\n");
                System.out.println(sql);
            }
        }
    }

    private static String writeHashedQuery(String tableName, Table table, String filterValue) {
        List<String> colNames = new ArrayList<>();
        for (Column col : table) {
            colNames.add(getColSelect(col));
        }
        String selectClause = Joiner.on(",\n\t").join(colNames);
        String sql = String.format("select\n\t%s from [%s]\nwhere [%s]='%s'",
                selectClause, tableName, filterCol, filterValue);
        return sql;
    }

    private static String getColSelect(Column col) {
        if(smallTypes.contains(col.getDataType().toLowerCase())) {
            return "[" + col.getColumnName() + "]";
        }
        if(bigTypes.contains(col.getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', [%s]) as [%s]", col.getColumnName(), col.getColumnName());
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
            for (Column col : table) {
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
        Map<String, Table> tables = new HashMap<>();
        try (Statement stmt = con.createStatement()) {
            String sql = Resources.toString(Resources.getResource("GetTables.sql"), Charsets.UTF_8);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    if (!tables.containsKey(tableName)) {
                        tables.put(tableName, new Table());
                    }
                    Table table = tables.get(tableName);

                    Column col = new Column();
                    col.setColumnName(rs.getString("column_name"));
                    col.setDataType(rs.getString("data_type"));
                    col.setPrimaryKey(rs.getBoolean("primary_key"));
                    table.add(col);
                }
            }
        }
        return tables;
    }
}
