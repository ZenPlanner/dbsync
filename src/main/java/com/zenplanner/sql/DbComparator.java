package com.zenplanner.sql;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class DbComparator {

    private static final String filterCol = "partitionId";

    public enum ChangeType {
        INSERT, UPDATE, DELETE, NONE
    }

    public DbComparator() {

    }

    public static void Syncronize(Connection scon, Connection dcon, String filterValue) {
        try {
            Map<String, Table> srcTables = filterTables(getTables(scon));
            Map<String, Table> dstTables = filterTables(getTables(dcon));
            for (Table srcTable : srcTables.values()) {
                if (!dstTables.containsKey(srcTable.getName())) {
                    continue;
                }
                Table dstTable = dstTables.get(srcTable.getName());
                System.out.println("Comparing table: " + srcTable.getName());
                syncTables(scon, dcon, srcTable, dstTable, filterValue);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error comparing databases!", ex);
        }
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
     * Compares two tables and syncronizes the results
     *
     * @param scon        The source connection
     * @param dcon        The destination connection
     * @param srcTable    The source table
     * @param dstTable    The destination table
     * @param filterValue A partitionId
     * @throws Exception
     */
    private static void syncTables(Connection scon, Connection dcon, Table srcTable, Table dstTable,
                                   String filterValue) throws Exception {
        Table lcd = findLcd(srcTable, dstTable);
        String sql = lcd.writeHashedQuery(filterCol); // TODO: non-filtered table queries
        int i = 0;
        try (PreparedStatement stmt = scon.prepareStatement(sql); PreparedStatement dtmt = dcon.prepareStatement(sql)) {
            stmt.setObject(1, filterValue);
            dtmt.setObject(1, filterValue);
            try (ResultSet srs = stmt.executeQuery(); ResultSet drs = dtmt.executeQuery()) {
                srs.next();
                drs.next();
                Map<ChangeType, List<Key>> changes = new HashMap<>();
                changes.put(ChangeType.INSERT, new ArrayList<>());
                changes.put(ChangeType.UPDATE, new ArrayList<>());
                changes.put(ChangeType.DELETE, new ArrayList<>());
                while (srs.getRow() > 0 || drs.getRow() > 0) {
                    System.out.println("Syncing row " + (++i));
                    ChangeType change = detectChange(srs, drs);
                    Key key = getPk(lcd, srs, drs);
                    List<Key> changeset = changes.get(change);
                    if (changeset == null) {
                        continue;
                    }
                    changeset.add(key);
                    advance(srcTable, dstTable, srs, drs);
                }
                insertRows(scon, dcon, lcd, changes.get(ChangeType.INSERT));
            }
        }
    }

    private static void insertRows(Connection scon, Connection dcon, Table table, List<Key> keys) throws Exception {
        if(keys.size() <= 0) {
            return;
        }
        try(Statement stmt = dcon.createStatement()) {
            stmt.executeUpdate(String.format("SET IDENTITY_INSERT [%s] ON;", table.getName()));
        } catch (Exception ex) {
            // TODO: Nicer solution for tables that don't have an identity
        }
        try (PreparedStatement selectStmt = createSelectQuery(scon, table, keys)) {
            try (ResultSet rs = selectStmt.executeQuery()) {
                String sql = table.writeInsertQuery();
                try(PreparedStatement insertStmt = dcon.prepareStatement(sql)) {
                    while (rs.next()) {
                        insertRow(insertStmt, table, rs);
                    }
                    insertStmt.executeBatch();
                }
            }
        }
    }

    /**
     * Adds the values from a given row to the PreparedStatement
     *
     * @param stmt The PreparedStatement to help prepare
     * @param table The table definition
     * @param rs The ResultSet to pull values from
     * @throws Exception
     */
    private static void insertRow(PreparedStatement stmt, Table table, ResultSet rs) throws Exception {
        stmt.clearParameters();
        int i = 0;
        for (Column col : table.values()) {
            String colName = col.getColumnName();
            Object val = rs.getObject(colName);
            stmt.setObject(++i, val);
        }
        stmt.addBatch();
    }

    /**
     * Takes two RecordSets, and advances one cursor, or the other, or both to keep the PKs in sync
     *
     * @param srcTable The source table
     * @param dstTable The destination table
     * @param srs      The source RecordSet
     * @param drs      The destination RecordSet
     * @throws Exception
     */
    private static void advance(Table srcTable, Table dstTable, ResultSet srs, ResultSet drs) throws Exception {
        Key spk = srcTable.getPk(srs);
        Key dpk = dstTable.getPk(drs);
        int val = Key.compare(spk, dpk);
        if (val < 0) {
            srs.next();
            return;
        }
        if (val > 0) {
            drs.next();
            return;
        }
        srs.next();
        drs.next();
    }

    /**
     * Gets the primary key from whichever row exists
     *
     * @param table The table definition
     * @param srs   The source RecordSet
     * @param drs   The destination RecordSet
     * @return The primary key of the row
     * @throws Exception
     */
    private static Key getPk(Table table, ResultSet srs, ResultSet drs) throws Exception {
        ChangeType change = detectChange(srs, drs);
        if (change == ChangeType.DELETE) {
            return table.getPk(drs);
        }
        return table.getPk(srs);
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
     * Creates a PreparedStatement that returns all of the rows for the given set of keys.
     * Don't forget to close the statement!
     *
     * @param con   The connection to use to query the DB for its schema
     * @param table The table definition
     * @param keys  The keys of the rows for which to query
     * @return A PreparedStatement that returns all the rows for the given set of keys.
     * @throws Exception
     */
    // TODO: Break this monster out into separate methods for SQL and values
    private static PreparedStatement createSelectQuery(Connection con, Table table, List<Key> keys) throws Exception {
        List<Object> parms = new ArrayList<>();
        List<Column> pk = table.getPk();
        StringBuilder sb = new StringBuilder();
        for (Key key : keys) {
            if (sb.length() > 0) {
                sb.append("\tor ");
            }
            sb.append("(");
            for (int i = 0; i < pk.size(); i++) {
                if (i > 0) {
                    sb.append(" and ");
                }
                Column col = pk.get(i);
                sb.append("[");
                sb.append(col.getColumnName());
                sb.append("]=?");

                // Grab the value of the parameter
                Object val = key.get(i);
                parms.add(val);
            }
            sb.append(")\n");
        }
        String sql = String.format("select\n\t*\nfrom [%s]\nwhere %s", table.getName(), sb.toString());
        PreparedStatement stmt = con.prepareStatement(sql);
        for (int i = 0; i < parms.size(); i++) {
            stmt.setObject(i + 1, parms.get(i));
        }
        return stmt;
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

    public static ChangeType detectChange(ResultSet srs, ResultSet drs) throws Exception {
        byte[] shash = getHash(srs);
        byte[] dhash = getHash(drs);
        if (shash == null && dhash == null) {
            throw new RuntimeException("Both rows are null!");
        }
        if (shash == null) {
            return ChangeType.DELETE;
        }
        if (dhash == null) {
            return ChangeType.INSERT;
        }
        if (shash.equals(dhash)) {
            return ChangeType.NONE;
        }
        return ChangeType.UPDATE;
    }

    /**
     * Get the Hash from a ResultSet, or returns null if the ResultSet is exhausted
     *
     * @param rs The ResultSet
     * @return The Hash, or null
     */
    private static byte[] getHash(ResultSet rs) throws Exception {
        if (rs == null || rs.isBeforeFirst() || rs.isAfterLast() || rs.getRow() == 0) {
            return null;
        }
        return rs.getBytes("Hash");
    }

}
