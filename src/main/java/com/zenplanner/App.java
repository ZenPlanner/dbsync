package com.zenplanner;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String filterCol = "partitionId";
    private static final List<String> smallTypes = Arrays.asList(new String[]{
            "uniqueidentifier", "bigint", "date", "datetime", "datetime2", "smalldatetime", "tinyint", "smallint",
            "int", "decimal", "bit", "money", "smallmoney", "char", "float"
    });
    private static final List<String> bigTypes = Arrays.asList(new String[]{"varchar", "nvarchar", "text"});

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
                    if (!dstTables.containsKey(srcTable.getName())) {
                        continue;
                    }
                    Table dstTable = dstTables.get(srcTable.getName());
                    System.out.println("Comparing table: " + srcTable.getName());
                    compTables(scon, dcon, srcTable, dstTable, filterValue);
                }
            }
        }
    }

    /**
     * Compares two tables and syncronizes the results
     *
     * @param scon The source connection
     * @param dcon The destination connection
     * @param srcTable The source table
     * @param dstTable The destination table
     * @param filterValue A partitionId
     * @throws Exception
     */
    private static void compTables(Connection scon, Connection dcon, Table srcTable, Table dstTable,
                                   String filterValue) throws Exception {
        Table lcd = findLcd(srcTable, dstTable);
        String sql = writeHashedQuery(lcd);
        int i = 0;
        try (PreparedStatement stmt = scon.prepareStatement(sql); PreparedStatement dtmt = dcon.prepareStatement(sql)) {
            stmt.setObject(1, filterValue);
            dtmt.setObject(1, filterValue);
            try (ResultSet srs = stmt.executeQuery(); ResultSet drs = dtmt.executeQuery()) {
                srs.next();
                drs.next();
                while(srs.getRow() > 0 || drs.getRow() > 0) {
                    System.out.println("Syncing row " + (++i));
                    syncRow(srs, drs);
                    advance(srcTable, dstTable, srs, drs);
                }
            }
        }
    }

    private static void syncRow(ResultSet srs, ResultSet drs) throws Exception {
        byte[] shash = getHash(srs);
        byte[] dhash = getHash(drs);
        if(shash == null && dhash == null) {
            throw new RuntimeException("Both rows are null!");
        }
        if(shash != null && dhash != null) {
            update();
        }
        if(shash == null) {
            delete();
        }
        if(dhash == null) {
            insert();
        }
    }


    private static void update() {
        System.out.println("Update");
    }

    private static void delete() {
        System.out.println("delete");
    }

    private static void insert() {
        System.out.println("insert");
    }

    /**
     * Takes two RecordSets, and advances one cursor, or the other, or both to keep the PKs in sync
     *
     * @param srcTable The source table
     * @param dstTable The destination table
     * @param srs The source RecordSet
     * @param drs The destination RecordSet
     * @throws Exception
     */
    private static void advance(Table srcTable, Table dstTable, ResultSet srs, ResultSet drs) throws Exception {
        Key spk = getPk(srcTable, srs);
        Key dpk = getPk(dstTable, drs);
        int val = Key.compare(spk, dpk);
        if(val < 0) {
            srs.next();
            return;
        }
        if(val > 0) {
            drs.next();
            return;
        }
        srs.next();
        drs.next();
    }

    /**
     * Pulls an array of objects that represents the PK from a row
     *
     * @param tab The table definition
     * @param rs A ResultSet to check
     * @return A List representing the PK
     * @throws Exception
     */
    private static Key getPk(Table tab, ResultSet rs) throws Exception {
        Key key = new Key();
        if(rs.isClosed() || rs.isBeforeFirst() || rs.isAfterLast() || rs.getRow() == 0) {
            key.add(Double.POSITIVE_INFINITY);
            return key;
        }
        for(Column col : tab.values()) {
            if(col.isPrimaryKey()) {
                Comparable<?> val = (Comparable<?>)rs.getObject(col.getColumnName());
                key.add(val);
            }
        }
        return key;
    }

    /**
     * Creates a virtual table that contains the intersection of the columns of two other real tables
     *
     * @param srcTable The source table
     * @param dstTable The destination table
     * @return a virtual table that contains the intersection of the columns of two other real tables
     */
    private static Table findLcd(Table srcTable, Table dstTable) {
        Table table = new Table(srcTable.getName());
        Set<String> colNames = new HashSet<>();
        colNames.addAll(srcTable.keySet());
        colNames.addAll(dstTable.keySet());
        for (String colName : colNames) {
            if (!srcTable.containsKey(colName) || !dstTable.containsKey(colName)) {
                continue;
            }
            table.put(colName, srcTable.get(colName));
        }
        return table;
    }

    /**
     * Writes a magical query that returns the primary key and a hash of the row
     *
     * @param table The table to query
     * @return A magical query that returns the primary key and a hash of the row
     */
    private static String writeHashedQuery(Table table) {
        List<String> colNames = new ArrayList<>();
        List<String> pk = new ArrayList<>();
        for (Column col : table.values()) {
            if (col.isPrimaryKey()) {
                pk.add("[" + col.getColumnName() + "]");
            }
            colNames.add(getColSelect(col));
        }
        String selectClause = Joiner.on("+\n\t\t").join(colNames);
        String orderClause = Joiner.on(",").join(pk);
        selectClause = orderClause + ",\n\tHASHBYTES('md5',\n\t\t" + selectClause + "\n\t) as [Hash]";
        String sql = String.format("select\n\t%s\nfrom [%s]\nwhere [%s]=?\norder by %s",
                selectClause, table.getName(), filterCol, orderClause);
        return sql;
    }

    /**
     * Returns the SQL to add to a select clause for the given column
     *
     * @param col The Column object
     * @return the SQL to add to a select clause for the given column
     */
    private static String getColSelect(Column col) {
        if (smallTypes.contains(col.getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', " +
                    "case when [%s] is null then convert(varbinary,0) " +
                    "else convert(varbinary, [%s]) end)", col.getColumnName(), col.getColumnName());
        }
        if (bigTypes.contains(col.getDataType().toLowerCase())) {
            return String.format("HASHBYTES('md5', " +
                    "case when [%s] is null then convert(varbinary,0) " +
                    "else convert(nvarchar(max), [%s]) end)", col.getColumnName(), col.getColumnName());
        }
        throw new RuntimeException("Unknown type: " + col.getDataType());
    }

    /**
     * Filters a map of database tables and returns only the ones that are sync-able
     *
     * @param in The map to filter
     * @return The filtered map
     */
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

    /**
     * Retrieves a map of Tables from the database schema
     *
     * @param con The connection to use to query the DB for its schema
     * @return A map of Tables from the database schema
     * @throws Exception
     */
    private static Map<String, Table> getTables(Connection con) throws Exception {
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

    /**
     * Get the Hash from a ResultSet, or returns null if the ResultSet is exhausted
     *
     * @param rs The ResultSet
     * @return The Hash, or null
     */
    private static byte[] getHash(ResultSet rs) throws Exception {
        if(rs == null || rs.isBeforeFirst() || rs.isAfterLast() || rs.getRow() == 0) {
            return null;
        }
        return rs.getBytes("Hash");
    }
}
