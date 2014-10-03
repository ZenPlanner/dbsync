package com.zenplanner;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import sun.misc.IOUtils;

import java.sql.*;
import java.util.*;

public class App {
    public static void main(String[] args) throws Exception {
        String sourceCon = args[0];
        //String destCon = args[1];

        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        // Get tables and columns
        try(Connection scon = DriverManager.getConnection(sourceCon)) {
            Map<String,List<Column>> sourceTables = getTables(scon);
        }
    }

    private static Map<String,List<Column>> getTables(Connection con) throws Exception {
        Map<String,List<Column>> tables = new HashMap<>();
        try(Statement stmt = con.createStatement()) {
            String sql = Resources.toString(Resources.getResource("GetTables.sql"), Charsets.UTF_8);
            try(ResultSet rs = stmt.executeQuery(sql)) {
                while(rs.next()) {
                    String tableName = rs.getString("table_name");
                    if(!tables.containsKey(tableName)) {
                        tables.put(tableName, new ArrayList<>());
                    }
                    List<Column> table = tables.get(tableName);

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
