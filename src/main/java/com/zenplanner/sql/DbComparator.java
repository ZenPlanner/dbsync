package com.zenplanner.sql;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DbComparator {

    private String currentTableName;
    private final AtomicInteger rowCount = new AtomicInteger();
    private final AtomicInteger tableCount = new AtomicInteger();
    private final AtomicInteger currentTable = new AtomicInteger();
    private final AtomicInteger currentRow = new AtomicInteger();
    private final Set<ActionListener> listeners = Collections.synchronizedSet(new HashSet<ActionListener>());

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
    public void synchronize(Connection scon, Connection dcon, Map<String,Object> filters, List<String> ignoreTables) {
        try {
            // Get the intersection of the tables
            Map<String, Table> srcTables = filterTables(getTables(scon));
            Map<String, Table> dstTables = getTables(dcon);
            Set<String> tableNames = new HashSet<>();
            tableNames.addAll(srcTables.keySet());
            tableNames.retainAll(dstTables.keySet());
            tableNames.removeAll(ignoreTables);
            tableCount.set(tableNames.size());
            rowCount.set(countRows(scon, srcTables.values(), filters));
            currentTable.set(0);
            currentRow.set(0);

            // Synchronize them
            try {
                setConstraints(dcon, dstTables.values(), false);
                for(String tableName : tableNames) {
                    setCurrentTableName(tableName);
                    Table srcTable = srcTables.get(tableName);
                    Table dstTable = dstTables.get(tableName);
                    System.out.println("Comparing table: " + srcTable.getName());
                    syncTable(scon, dcon, srcTable, dstTable, filters);
                    currentTable.incrementAndGet();
                    fireProgress();
                }
            } catch (Exception ex) {
                throw ex;
            } finally {
                setConstraints(dcon, dstTables.values(), true);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Error comparing databases!", ex);
        }
        currentTable.incrementAndGet();
        fireProgress();
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

    private static int countRows(Connection con, Collection<Table> tables, Map<String,Object> filters) throws Exception {
        int count = 0;
        for(Table table : tables) {
            String sql = table.writeCountQuery(filters);
            try (PreparedStatement stmt = con.prepareStatement(sql)) {
                if(table.hasAllColumns(filters.keySet())) {
                    setFilterParams(stmt, filters);
                }
                if(stmt.execute()) {
                    do {
                        try (ResultSet rs = stmt.getResultSet()) {
                            while (rs.next()) {
                                count += rs.getInt(1);
                            }
                        }
                    } while(stmt.getMoreResults());
                }
            }
        }
        return count;
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
                    String colName = rs.getString("column_name").toLowerCase();
                    col.setColumnName(colName);
                    col.setDataType(rs.getString("data_type"));
                    col.setPrimaryKey(rs.getBoolean("primary_key"));
                    table.put(colName, col);
                }
            }
        }

        for(Table table : tables.values()) {
            if(table.size() == 0) {
                throw new IllegalStateException("Table has no columns: " + table.getName());
            }
        }

        return tables;
    }

    private static void setFilterParams(PreparedStatement stmt, Map<String, Object> filters) throws Exception {
        int i = 1;
        for(Object val : filters.values()) {
            stmt.setObject(i, val);
            i++;
        }
    }

    /**
     * Compares two tables and syncronizes the results
     *
     * @param scon        The source connection
     * @param dcon        The destination connection
     * @param srcTable    The source table
     * @param dstTable    The destination table
     * @throws Exception
     */
    private void syncTable(Connection scon, Connection dcon, Table srcTable, Table dstTable,
                                  Map<String, Object> filters) throws Exception {
        Table lcd = findLcd(srcTable, dstTable);
        String sql = lcd.writeHashedQuery(filters);
        //int i = 0; // TODO: Threading and progress indicator
        try (PreparedStatement stmt = scon.prepareStatement(sql); PreparedStatement dtmt = dcon.prepareStatement(sql)) {
            // Set filter parameters
            if(lcd.hasAllColumns(filters.keySet())) {
                setFilterParams(stmt, filters);
                setFilterParams(dtmt, filters);
            }

            // Make changes
            try (ResultSet srs = stmt.executeQuery(); ResultSet drs = dtmt.executeQuery()) {
                srs.next();
                drs.next();
                Map<ChangeType, Set<Key>> changes = new HashMap<>();
                changes.put(ChangeType.INSERT, new HashSet<>());
                changes.put(ChangeType.UPDATE, new HashSet<>());
                changes.put(ChangeType.DELETE, new HashSet<>());
                while (srs.getRow() > 0 || drs.getRow() > 0) {
                    ChangeType change = lcd.detectChange(srs, drs);

                    Key key = lcd.getPk(srs, drs);
                    Set<Key> changeset = changes.get(change);
                    if (changeset != null) {
                        changeset.add(key);
                    }
                    advance(srcTable, dstTable, srs, drs);
                    currentRow.incrementAndGet();
                }
                lcd.insertRows(scon, dcon, changes.get(ChangeType.INSERT));
                lcd.updateRows(scon, dcon, changes.get(ChangeType.UPDATE));
                lcd.deleteRows(dcon, changes.get(ChangeType.DELETE));
            } catch (Exception ex) {
                throw new RuntimeException("Error selecting hashed rows: " + sql, ex);
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

        if(table.size() == 0) {
            throw new IllegalStateException("Table has no columns: " + table.getName());
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

    public int getCurrentRow() {
        return currentRow.get();
    }

    public int getRowCount() {
        return rowCount.get();
    }

    public synchronized void setCurrentTableName(String name) {
        currentTableName = name;
    }

    public synchronized String getCurrentTableName() {
        return currentTableName;
    }

    public int getCurrentTable() {
        return currentTable.get();
    }

    public int getTableCount() {
        return tableCount.get();
    }

    private void fireProgress() {
        ActionEvent ae = new ActionEvent(this, getCurrentTable(), null);
        for(ActionListener pl : listeners) {
            pl.actionPerformed(ae);
        }
    }

    public void addListener(ActionListener val) {
        listeners.add(val);
    }

}
