package com.zenplanner.sql;

import com.google.common.base.Joiner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class Table extends TreeMap<String, Column> {
    private final String name;

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
        List<Column> pk = new ArrayList<>();
        for(Column col : values()) {
            if(col.isPrimaryKey()) {
                pk.add(col);
            }
        }
        return pk;
    }


    /**
     * @return the insert SQL for this table
     */
    public String writeInsertQuery() {
        List<String> colNames = new ArrayList<>();
        List<String> valueNames = new ArrayList<>();
        for(Column col : values()) {
            String colName = col.getColumnName();
            colNames.add("[" + colName + "]");
            valueNames.add("?");
        }
        String nameClause = Joiner.on(", ").join(colNames);
        String valueClause = Joiner.on(", ").join(valueNames);
        String sql = String.format("insert into [%s] (%s) values (%s)", getName(), nameClause, valueClause);
        return sql;
    }

    /**
     * @return A magical query that returns the primary key and a hash of the row
     */
    public String writeHashedQuery(String filterCol) {
        List<String> colNames = new ArrayList<>();
        List<String> pk = new ArrayList<>();
        for (Column col : values()) {
            if (col.isPrimaryKey()) {
                pk.add("[" + col.getColumnName() + "]");
            }
            colNames.add(col.getSelect());
        }
        String selectClause = Joiner.on("+\n\t\t").join(colNames);
        String orderClause = Joiner.on(",").join(pk);
        selectClause = orderClause + ",\n\tHASHBYTES('md5',\n\t\t" + selectClause + "\n\t) as [Hash]";
        String sql = String.format("select\n\t%s\nfrom [%s]\n", selectClause, getName());
        if(filterCol != null) {
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
                Comparable<?> val = (Comparable<?>) rs.getObject(col.getColumnName());
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

}
