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
        StringBuilder sb = new StringBuilder(); // Debugging
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
                Key lastSrcPk = new Key();
                Key lastDstPk = new Key();
                while (srs.getRow() > 0 || drs.getRow() > 0) {
                    // Debugging
                    Key srcPk = lcd.getPk(srs); // Debugging
                    Key dstPk = lcd.getPk(drs); // Debugging
                    ChangeType change = lcd.detectChange(srs, drs);
                    sb.append("" + srcPk + "-" + dstPk + " " + change + "\n"); // Debugging
                    if (Key.compare(lastSrcPk, srcPk) > 0) { // Debugging
                        int eq = Key.compare(lastSrcPk, srcPk); // Debugging
                        throw new RuntimeException("Invalid sort order on source query!"); // Debugging
                    }
                    if (Key.compare(lastDstPk, dstPk) > 0) { // Debugging
                        int eq = Key.compare(lastDstPk, dstPk); // Debugging
                        throw new RuntimeException("Invalid sort order on dest query!"); // Debugging
                    }

                    Key key = lcd.getPk(srs, drs);
                    Set<Key> changeset = changes.get(change);
                    if (changeset != null) {
                        changeset.add(key);
                    }
                    advance(srcTable, dstTable, srs, drs);

                    // Debugging
                    //insertRows(scon, dcon, lcd, changes.get(ChangeType.INSERT)); // Debugging
                    //changes.get(ChangeType.INSERT).clear(); // Debugging
                    lastSrcPk = srcPk; // Debugging
                    lastDstPk = dstPk; // Debugging
                }
                lcd.insertRows(scon, dcon, changes.get(ChangeType.INSERT));
                lcd.updateRows(scon, dcon, changes.get(ChangeType.UPDATE));
                lcd.deleteRows(dcon, changes.get(ChangeType.DELETE));
            } catch (Exception ex) {
                throw new RuntimeException("Error selecting hashed rows!", ex);
            }
        }
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

}
