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
    public String writeHashedQuery(String filterCol) {
        List<String> colNames = new ArrayList<>();
        List<String> selectPk = new ArrayList<>();
        List<String> sortPk = new ArrayList<>();
        for (Column col : values()) {
            if (col.isPrimaryKey()) {
                // TODO: Lexagraphical sorting is a horrible performance killer! Figure out UUID sort order on SQL server and match it in Key.compare()
                selectPk.add(String.format("convert(varchar(max),[%s]) as [%s]", col.getColumnName(), col.getColumnName()));
                sortPk.add(String.format("convert(varchar(max),[%s])", col.getColumnName()));
            }
            colNames.add(col.getSelect());
        }
        String hashNames = Joiner.on("+\n\t\t").join(colNames);
        String selectPkClause = Joiner.on(",").join(selectPk);
        String orderClause = Joiner.on(",").join(sortPk);
        String selectClause = selectPkClause + ",\n\tHASHBYTES('md5',\n\t\t" + hashNames + "\n\t) as [Hash]";
        String sql = String.format("select\n\t%s\nfrom [%s]\n", selectClause, getName());
        if(hasColumn(filterCol)) {
            sql += String.format("where [%s]=?\n", filterCol);
        }
        sql += String.format("order by %s", orderClause);
        return sql;
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
                Comparable<?> val = rs.getString(col.getColumnName());
                key.add(val);
            }
        }
        return key;
    }

    /**
     * Enables or disables constraints for this table
     * @param con The connection to use
     * @param enabled
     */
    public void setConstraints(Connection con, boolean enabled) {
        try (Statement stmt = con.createStatement()) {
            String state = enabled ? "CHECK" : "NOCHECK";
            stmt.executeUpdate(String.format("ALTER TABLE [%s] %s CONSTRAINT all;", getName(), state));
        } catch (Exception ex) {
            throw new RuntimeException("Error setting constraints enabled: " + enabled, ex);
        }
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
                stmt.setObject(i + 1, parms.get(i));
            }
            return stmt;
        } catch (Exception ex) {
            throw new RuntimeException("Error creating select query!", ex);
        }
    }

}
