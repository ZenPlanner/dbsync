package com.zenplanner.sql;

import com.google.common.base.Joiner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class Table extends TreeMap<String, Column> {
    private final String name;
    private List<Column> pk;
    private static final int maxKeys = 1999; // jtds driver limit

    public Table(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof Table)) {
            return false;
        }
        return name.equalsIgnoreCase(((Table) obj).getName());
    }

    /**
     * @return A list of the columns that constitute the primary key
     */
    public List<Column> getPk() {
        if(pk != null) {
            return pk;
        }
        synchronized (this) {
            List<Column> pk = new ArrayList<>();
            for(Column col : values()) {
                if(col.isPrimaryKey()) {
                    pk.add(col);
                }
            }
            this.pk = pk;
            return pk;
        }
    }

    public boolean hasColumn(String name) {
        return containsKey(name);
    }

    /**
     * @return the insert SQL for this table
     */
    public String writeInsertQuery() {
        List<String> colNames = new ArrayList<>();
        List<String> valueNames = new ArrayList<>();
        for(Column col : values()) {
            String colName = col.getColumnName();
            colNames.add("\n\t[" + colName + "]");
            valueNames.add("?");
        }
        String nameClause = Joiner.on(", ").join(colNames);
        String valueClause = Joiner.on(", ").join(valueNames);
        String sql = String.format("insert into [%s] (%s\n) values (%s)", getName(), nameClause, valueClause);
        return sql;
    }

    public String writeUpdateQuery() {
        List<String> updateCols = new ArrayList<>();
        List<Column> pk = getPk();
        for(Column col : values()) {
            if(pk.contains(col)) {
                continue; // TODO: Cache non-update columns for speed
            }
            String colName = col.getColumnName();
            updateCols.add(String.format("\t[%s]=?", colName));
        }
        List<String> whereCols = new ArrayList<>();
        for(Column col : pk) {
            String colName = col.getColumnName();
            whereCols.add(String.format("[%s]=?", colName));
        }
        String updateClause = Joiner.on(",\n").join(updateCols);
        String whereClause = Joiner.on("\n\tand ").join(whereCols);
        String sql = String.format("update [%s] set\n%s\nwhere %s", getName(), updateClause, whereClause);
        return sql;
    }

    /**
     * @return A magical query that returns the primary key and a hash of the row
     */
    public String writeHashedQuery(Map<String,Object> filters) {
        List<String> colNames = new ArrayList<>();
        List<String> pk = new ArrayList<>();
        for(Column col : values()) {
            colNames.add(col.getSelect());
        }
        for (Column col : getPk()) {
            pk.add(String.format("[%s]", col.getColumnName()));
        }
        String hashNames = Joiner.on("+\n\t\t").join(colNames);
        String orderClause = Joiner.on(",").join(pk);
        String selectClause = orderClause + ",\n\tHASHBYTES('md5',\n\t\t" + hashNames + "\n\t) as [Hash]";
        String sql = String.format("select\n\t%s\nfrom [%s]\n", selectClause, getName());

        // Filter
        if(hasAllColumns(filters.keySet())) {
            sql += "where [" + Joiner.on("]=?\n\tand [").join(filters.keySet()) + "]=?";
        }

        sql += String.format("order by %s", orderClause);
        return sql;
    }

    public String writeCountQuery(Map<String,Object> filters) {
        String sql = String.format("select\n\tcount(*)\nfrom [%s]\n", getName());
        if(hasAllColumns(filters.keySet())) {
            sql += "where [" + Joiner.on("]=?\n\tand [").join(filters.keySet()) + "]=?;\n";
        }
        return sql;
    }

    public boolean hasAllColumns(Set<String> colNames) {
        Set<String> filterCols = new HashSet<>();
        filterCols.addAll(keySet());
        filterCols.retainAll(colNames);
        return filterCols.size() == colNames.size();
    }

    /**
     * Pulls an array of objects that represents the PK from a row
     *
     * @param rs  A ResultSet to check
     * @return A List representing the PK
     * @throws Exception
     */
    public Key getPk(ResultSet rs) throws Exception {
        Key key = new Key();
        if (rs.isClosed() || rs.isBeforeFirst() || rs.isAfterLast() || rs.getRow() == 0) {
            return null;
        }
        for (Column col : values()) {
            if (col.isPrimaryKey()) {
                Comparable<?> val = col.getValue(rs);
                key.add(val);
            }
        }
        return key;
    }

    public void setIdentityInsert(Connection con, boolean enabled) {
        try (Statement stmt = con.createStatement()) {
            String state = enabled ? "ON" : "OFF";
            stmt.executeUpdate(String.format("SET IDENTITY_INSERT [%s] %s;", getName(), state));
        } catch (Exception ex) {
            // TODO: Nicer solution for tables that don't have an identity
        }
    }

    public PreparedStatement createSelectQuery(Connection con, Set<Key> keys, int count) {
        return createQuery("select *", con, keys, count);
    }

    public PreparedStatement createDeleteQuery(Connection con, Set<Key> keys, int count) {
        return createQuery("delete", con, keys, count);
    }

    // TODO: Break this monster out into separate methods for SQL and values
    private PreparedStatement createQuery(String prefix, Connection con, Set<Key> keys, int count) {
        List<Object> parms = new ArrayList<>();
        List<Column> pk = getPk();
        StringBuilder sb = new StringBuilder();
        int rowIndex = 0;
        for (Key key : new HashSet<>(keys)) {
            keys.remove(key); // Remove as we go
            if (sb.length() > 0) {
                sb.append("\tor ");
            }
            sb.append("(");
            for (int pkIdx = 0; pkIdx < pk.size(); pkIdx++) {
                if (pkIdx > 0) {
                    sb.append(" and ");
                }
                Column col = pk.get(pkIdx);
                sb.append("[");
                sb.append(col.getColumnName());
                sb.append("]=?");

                // Grab the value of the parameter
                Object val = key.get(pkIdx);
                parms.add(val);
            }
            sb.append(")\n");
            if (rowIndex++ >= count) {
                break;
            }
        }
        String sql = String.format("%s\nfrom [%s]\nwhere %s", prefix, getName(), sb.toString());
        try {
            PreparedStatement stmt = con.prepareStatement(sql);
            for (int i = 0; i < parms.size(); i++) {
                Object javaVal = parms.get(i);
                Object sqlVal = javaToSql(javaVal);
                stmt.setObject(i + 1, sqlVal);
            }
            return stmt;
        } catch (Exception ex) {
            throw new RuntimeException("Error creating select query!", ex);
        }
    }
    public static Object javaToSql(Object val) {
        if(val == null) {
            return null;
        }
        if(val instanceof UUID) {
            return UuidUtil.uuidToByteArray(((UUID)val));
        }
        if(val instanceof String) {
            return val;
        }
        if(val instanceof Long) {
            return val;
        }
        throw new RuntimeException("Unknown type: " + val.getClass().getName());
    }


    public void deleteRows(Connection dcon, Set<Key> keys) throws Exception {
        List<Column> pk = getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Deleting " + count + " rows from " + getName());
            try (PreparedStatement selectStmt = createDeleteQuery(dcon, keys, count)) {
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
     * @param keys The keys of the rows for which to query
     * @throws Exception
     */
    public void insertRows(Connection scon, Connection dcon, Set<Key> keys) throws Exception {
        if (keys.size() <= 0) {
            return;
        }

        setIdentityInsert(dcon, true);
        List<Column> pk = getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Inserting " + count + " rows into " + getName());
            try (PreparedStatement selectStmt = createSelectQuery(scon, keys, count)) {
                try (ResultSet rs = selectStmt.executeQuery()) {
                    String sql = writeInsertQuery();
                    try (PreparedStatement insertStmt = dcon.prepareStatement(sql)) {
                        while (rs.next()) {
                            insertRow(insertStmt, rs);
                        }
                        try {
                            insertStmt.executeBatch();
                        } catch (Exception ex) {
                            throw new RuntimeException("Error inserting rows: " + sql, ex);
                        }
                    }
                }
            }
            rowIndex += count;
        }
    }

    public void updateRows(Connection scon, Connection dcon, Set<Key> keys) throws Exception {
        if (keys.size() <= 0) {
            return;
        }
        List<Column> pk = getPk();
        int rowLimit = (int) Math.floor(maxKeys / pk.size());
        for (int rowIndex = 0; rowIndex < keys.size(); ) {
            int count = Math.min(keys.size() - rowIndex, rowLimit);
            System.out.println("Updating " + count + " rows in " + getName());
            try (PreparedStatement selectStmt = createSelectQuery(scon, keys, count)) {
                try (ResultSet rs = selectStmt.executeQuery()) {
                    String sql = writeUpdateQuery();
                    try (PreparedStatement updateStmt = dcon.prepareStatement(sql)) {
                        while (rs.next()) {
                            updateRow(updateStmt, rs);
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
     * @param rs    The ResultSet to pull values from
     * @throws Exception
     */
    private void insertRow(PreparedStatement stmt, ResultSet rs) throws Exception {
        stmt.clearParameters();
        int i = 0;
        for (Column col : values()) {
            String colName = col.getColumnName();
            Object val = rs.getObject(colName);
            stmt.setObject(++i, val);
        }
        stmt.addBatch();
    }

    private void updateRow(PreparedStatement stmt, ResultSet rs) throws Exception {
        stmt.clearParameters();
        int i = 0;
        List<Column> pk = getPk();
        for (Column col : values()) {
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
     * Gets the primary key from whichever row exists
     *
     * @param srs   The source RecordSet
     * @param drs   The destination RecordSet
     * @return The primary key of the row
     * @throws Exception
     */
    public Key getPk(ResultSet srs, ResultSet drs) throws Exception {
        DbComparator.ChangeType change = detectChange(srs, drs);
        if (change == DbComparator.ChangeType.DELETE) {
            return getPk(drs);
        }
        return getPk(srs);
    }

    /**
     * Basically this is the join logic. It compares the two rows presently under the cursors, and returns an action
     * that needs to be taken based on whether the row is in left but not right, right but not left, or in both but
     * changes are present. As usual for join code, this method assumes that the ResultSets are ordered, and the
     * Key.compare() method exhibits the same ordering as the database engine.
     *
     * @param srs   The source RecordSet
     * @param drs   The destination RecordSet
     * @return A ChangeType indicating what action should be taken to sync the two databases
     * @throws Exception
     */
    public DbComparator.ChangeType detectChange(ResultSet srs, ResultSet drs) throws Exception {
        // Verify we're on the same row
        Key srcPk = getPk(srs);
        Key dstPk = getPk(drs);
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
            return DbComparator.ChangeType.INSERT;
        }
        if (eq > 0) {
            // Left > right, delete
            return DbComparator.ChangeType.DELETE;
        }

        // Keys match, check hashes
        byte[] shash = getHash(srs);
        byte[] dhash = getHash(drs);
        if (shash == null && dhash == null) {
            throw new RuntimeException("Both rows are null!");
        }
        if (shash == null) {
            return DbComparator.ChangeType.DELETE;
        }
        if (dhash == null) {
            return DbComparator.ChangeType.INSERT;
        }
        if (Arrays.equals(shash, dhash)) {
            return DbComparator.ChangeType.NONE;
        }
        return DbComparator.ChangeType.UPDATE;
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
