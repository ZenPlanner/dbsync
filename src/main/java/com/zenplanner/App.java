package com.zenplanner;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import sun.misc.IOUtils;

import java.sql.*;
import java.util.*;

public class App {
    private static final String filterCol = "partitionId";

    public static void main(String[] args) throws Exception {
        String sourceCon = args[0];
        //String destCon = args[1];

        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        // Get tables and columns
        try(Connection scon = DriverManager.getConnection(sourceCon)) {
            Map<String,Table> sourceTables = filterTables(getTables(scon));
        }
    }

    // Removes tables that don't have a primary key, or aren't related to partitionId
    private static Map<String,Table> filterTables(Map<String,Table> in) {
        Map<String,Table> out = new HashMap<>();
        for(Map.Entry<String, Table> entry : in.entrySet()) {
            boolean hasPk = false;
            boolean hasCol = false;
            String name = entry.getKey();
            Table table = entry.getValue();
            for(Column col : table) {
                if(col.isPrimaryKey()) {
                    hasPk = true;
                }
                if(filterCol.equalsIgnoreCase(col.getColumnName())) {
                    hasCol = true;
                }
            }
            if(hasPk && hasCol) {
                out.put(name, table);
            }
        }
        return out;
    }

    private static Map<String,Table> getTables(Connection con) throws Exception {
        Map<String,Table> tables = new HashMap<>();
        try(Statement stmt = con.createStatement()) {
            String sql = Resources.toString(Resources.getResource("GetTables.sql"), Charsets.UTF_8);
            try(ResultSet rs = stmt.executeQuery(sql)) {
                while(rs.next()) {
                    String tableName = rs.getString("table_name");
                    if(!tables.containsKey(tableName)) {
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
