package com.zenplanner.sql;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class DbComparator {

    private static final String filterCol = "partitionId"; // TODO: Parameterize
    private static final int maxKeys = 1999; // jtds driver limit

    public enum ChangeType {
        INSERT, UPDATE, DELETE, NONE
    }

    public DbComparator() {

    }

    /**
     * Takes connections to two databases, compares deltas, and upserts appropriate data to get them in sync
     *
     * @param scon The source connection
     * @param dcon The destination connection
     * @param filterValue A value with which to filter partition data
     */
    public static void Syncronize(Connection scon, Connection dcon, String filterValue) {
        try {
            Map<String, Table> srcTables = filterTables(getTables(scon));
            Map<String, Table> dstTables = getTables(dcon);
            try {
                setConstraints(dcon, dstTables.values(), false);
                for (Table srcTable : srcTables.values()) {
                    if (!dstTables.containsKey(srcTable.getName())) {
                        continue;
                    }
                    Table dstTable = dstTables.get(srcTable.getName());
                    System.out.println("Comparing table: " + srcTable.getName());
                    syncTables(scon, dcon, srcTable, dstTable, filterValue);
                }
            } catch (Exception ex) {
                throw ex;
            } finally {
                setConstraints(dcon, dstTables.values(), true);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error comparing databases!", ex);
        }
    }

    /**
     * Turns all constraints on or off
     *
     * @param con The connection on which to enable or disable constraints
     * @param tables A collection of tables on which to operate
     * @param enabled A flag indicating if constraints should be enabled or disabled
     */
    private static void setConstraints(Connection con, Collection<Table> tables, boolean enabled) {
        for (Table table : tables) {
            table.setConstraints(con, enabled);
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
        String sql = lcd.writeHashedQuery(filterCol);
        //int i = 0; // TODO: Threading and progress indicator
        try (PreparedStatement stmt = scon.prepareStatement(sql); PreparedStatement dtmt = dcon.prepareStatement(sql)) {
            if (lcd.hasColumn(filterCol)) {
                stmt.setObject(1, filterValue);
                dtmt.setObject(1, filterValue);
            }
            try (ResultSet srs = stmt.executeQuery(); ResultSet drs = dtmt.executeQuery()) {
                srs.next();
                drs.next();
                Map<ChangeType, Set<Key>> changes = new HashMap<>();
                changes.put(ChangeType.INSERT, new HashSet<>());
                changes.put(ChangeType.UPDATE, new HashSet<>());
                changes.put(ChangeType.DELETE, new HashSet<>());
                while (srs.getRow() > 0 || drs.getRow() > 0) {
                    ChangeType change = detectChange(lcd, srs, drs);
                    Key key = getPk(lcd, srs, drs);
                    Set<Key> changeset = changes.get(change);
                    if (changeset != null) {
                        changeset.add(key);
                    }
                    advance(srcTable, dstTable, srs, drs);
                }
                insertRows(scon, dcon, lcd, changes.get(ChangeType.INSERT));
                updateRows(scon, dcon, lcd, changes.get(ChangeType.UPDATE));
                deleteRows(dcon, lcd, changes.get(ChangeType.DELETE));
            } catch (Exception ex) {
                throw new RuntimeException("Error selecting hashed rows!", ex);
            }
        }
    }

    private static void deleteRows(Connection dcon, Table table, Set<Key> keys) throws Exception {
        List<Column> pk = table.getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Deleting " + count + " rows from " + table.getName());
            try (PreparedStatement selectStmt = table.createDeleteQuery(dcon, keys, count)) {
                selectStmt.execute();
            }
            rowIndex += count;
        }
    }

    /**
     * Queries the source database for row information on each row who's PK is in the keys array, and inserts those
     * rows into the destination connection.
     *
     * @param scon The source connection
     * @param dcon The destination connection
     * @param table The table definition
     * @param keys The keys of the rows for which to query
     * @throws Exception
     */
    private static void insertRows(Connection scon, Connection dcon, Table table, Set<Key> keys) throws Exception {
        if (keys.size() <= 0) {
            return;
        }

        table.setIdentityInsert(dcon, true);
        List<Column> pk = table.getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Inserting " + count + " rows into " + table.getName());
            try (PreparedStatement selectStmt = table.createSelectQuery(scon, keys, count)) {
                try (ResultSet rs = selectStmt.executeQuery()) {
                    String sql = table.writeInsertQuery();
                    try (PreparedStatement insertStmt = dcon.prepareStatement(sql)) {
                        while (rs.next()) {
                            insertRow(insertStmt, table, rs);
                        }
                        try {
                            insertStmt.executeBatch();
                        } catch (Exception ex) {
                            throw new RuntimeException("Error inserting rows!", ex);
                        }
                    }
                }
            }
            rowIndex += count;
        }
    }

    private static void updateRows(Connection scon, Connection dcon, Table table, Set<Key> keys) throws Exception {
        if (keys.size() <= 0) {
            return;
        }
        List<Column> pk = table.getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Updating " + count + " rows in " + table.getName());
            try (PreparedStatement selectStmt = table.createSelectQuery(scon, keys, count)) {
                try (ResultSet rs = selectStmt.executeQuery()) {
                    String sql = table.writeUpdateQuery();
                    try (PreparedStatement updateStmt = dcon.prepareStatement(sql)) {
                        while (rs.next()) {
                            updateRow(updateStmt, table, rs);
                        }
                        try {
                            updateStmt.executeBatch();
                        } catch (Exception ex) {
                            throw new RuntimeException("Error updating rows!", ex);
                        }
                    }
                }
            }
            rowIndex += count;
        }
    }

    /**
     * Adds the values from a given row to the PreparedStatement
     *
     * @param stmt  The PreparedStatement to help prepare
     * @param table The table definition
     * @param rs    The ResultSet to pull values from
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

    private static void updateRow(PreparedStatement stmt, Table table, ResultSet rs) throws Exception {
        stmt.clearParameters();
        int i = 0;
        List<Column> pk = table.getPk();
        for (Column col : table.values()) {
            if(pk.contains(col)) {
                continue; // TODO: Cache non-update columns for speed
            }
            String colName = col.getColumnName();
            Object val = rs.getObject(colName);
            stmt.setObject(++i, val);
        }
        for(Column col : pk) {
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
        ChangeType change = detectChange(table, srs, drs);
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
     * Filters a map of database tables and returns only the ones that are sync-able
     *
     * @param in The map to filter
     * @return The filtered map
     */
    private static Map<String, Table> filterTables(Map<String, Table> in) {
        Map<String, Table> out = new HashMap<>();
        for (Map.Entry<String, Table> entry : in.entrySet()) {
            String name = entry.getKey();
            Table table = entry.getValue();
            if (table.getPk().size() > 0) {
                out.put(name, table);
            }
        }
        return out;
    }

    /**
     * Basically this is the join logic. It compares the two rows presently under the cursors, and returns an action
     * that needs to be taken based on whether the row is in left but not right, right but not left, or in both but
     * changes are present. As usual for join code, this method assumes that the ResultSets are ordered, and the
     * Key.compare() method exhibits the same ordering as the database engine.
     *
     * @param table The table definition
     * @param srs   The source RecordSet
     * @param drs   The destination RecordSet
     * @return A ChangeType indicating what action should be taken to sync the two databases
     * @throws Exception
     */
    public static ChangeType detectChange(Table table, ResultSet srs, ResultSet drs) throws Exception {
        // Verify we're on the same row
        Key srcPk = table.getPk(srs);
        Key dstPk = table.getPk(drs);
        int eq = Key.compare(srcPk, dstPk);

/*
Left		Right
ACD			BDE

A			B			Left < right, insert A into right
C			B			Left > right, delete B from right
D			D			Left = right, update D in right
null		E			Left > right, delete E from right
*/
        if (eq < 0) {
            // Left < right, insert
            return ChangeType.INSERT;
        }
        if (eq > 0) {
            // Left > right, delete
            return ChangeType.DELETE;
        }

        // Keys match, check hashes
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
        if (Arrays.equals(shash, dhash)) {
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
