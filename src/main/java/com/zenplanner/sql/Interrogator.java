package com.zenplanner.sql;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Interrogator {

    public static String getConstraints(Connection con) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (Statement stmt = con.createStatement()) {
            String sql = Resources.toString(Resources.getResource("GetConstraints.sql"), Charsets.UTF_8);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                Map<UniqueConstraint, List<String>> uks = getUniqueConstraints(rs);
                sb.append(writeUnique(uks));
            }
            stmt.getMoreResults();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                Map<ForeignKey, List<FkCol>> fks = getForeignKeys(rs);
                sb.append(writeForeign(fks));
            }
        }
        return sb.toString();
    }

    private static String writeForeign(Map<ForeignKey, List<FkCol>> fks) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<ForeignKey, List<FkCol>> entry : fks.entrySet()) {
            ForeignKey cnst = entry.getKey();
            List<FkCol> cols = entry.getValue();
            sb.append(String.format("ALTER TABLE [%s].[%s] %s CONSTRAINT [%s];",
                    cnst.getSchemaName(),
                    cnst.getTableName(),
                    cnst.isDisabled() ? "NOCHECK" : "CHECK",
                    cnst.getConstraintName()
            ));
        }
        return sb.toString();
    }

    private static String writeUnique(Map<UniqueConstraint, List<String>> constraints) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<UniqueConstraint, List<String>> entry : constraints.entrySet()) {
            UniqueConstraint cnst = entry.getKey();
            List<String> cols = entry.getValue();
            sb.append(String.format("ALTER TABLE [%s].[%s] ADD CONSTRAINT [%s] UNIQUE ([%s]); ",
                    cnst.getSchemaName(),
                    cnst.getTableName(),
                    cnst.getConstraintName(),
                    Joiner.on("],[").join(cols)
            ));
        }
        return sb.toString();
    }

    private static Map<ForeignKey, List<FkCol>> getForeignKeys(ResultSet rs) throws SQLException {
        Map<ForeignKey, List<FkCol>> fks = new HashMap<>();
        while (rs.next()) {
            ForeignKey fk = new ForeignKey(
                    rs.getString("schema_name"),
                    rs.getString("FK_NAME"),
                    rs.getString("table"),
                    rs.getBoolean("disabled")
            );
            if (!fks.containsKey(fk)) {
                fks.put(fk, new ArrayList<>());
            }
            List<FkCol> cols = fks.get(fk);
            cols.add(new FkCol(rs.getString("referenced_table"), rs.getString("referenced_column")));
        }
        return fks;
    }

    private static Map<UniqueConstraint, List<String>> getUniqueConstraints(ResultSet rs) throws SQLException {
        Map<UniqueConstraint, List<String>> uniqueConstraints = new HashMap<>();
        while (rs.next()) {
            UniqueConstraint uk = new UniqueConstraint(
                    rs.getString("schema_name"),
                    rs.getString("table_name"),
                    rs.getString("constraint_name")
            );
            if (!uniqueConstraints.containsKey(uk)) {
                uniqueConstraints.put(uk, new ArrayList<>());
            }
            List<String> columns = uniqueConstraints.get(uk);
            columns.add(rs.getString("COLUMN_NAME"));
        }
        return uniqueConstraints;
    }

    // ----------------------------------------------- FkCol -----------------------------------------------------------
    private static class FkCol {
        private String referencedTable;
        private String referencedColumn;

        private FkCol(String referencedTable, String referencedColumn) {
            this.referencedTable = referencedTable;
            this.referencedColumn = referencedColumn;
        }

        public String getReferencedColumn() {
            return referencedColumn;
        }

        public String getReferencedTable() {
            return referencedTable;
        }
    }

    // ----------------------------------------------- ForeignKey ------------------------------------------------------
    private static class ForeignKey {
        private String schemaName;
        private String constraintName;
        private String tableName;
        private boolean disabled;

        private ForeignKey(String schemaName, String constraintName, String tableName, boolean disabled) {
            this.schemaName = schemaName;
            this.constraintName = constraintName;
            this.tableName = tableName;
            this.disabled = disabled;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean isDisabled() {
            return disabled;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ForeignKey that = (ForeignKey) o;

            if (disabled != that.disabled) return false;
            if (constraintName != null ? !constraintName.equals(that.constraintName) : that.constraintName != null)
                return false;
            if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
            if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = schemaName != null ? schemaName.hashCode() : 0;
            result = 31 * result + (constraintName != null ? constraintName.hashCode() : 0);
            result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
            result = 31 * result + (disabled ? 1 : 0);
            return result;
        }
    }

    // ----------------------------------------- UniqueConstraint ------------------------------------------------------
    private static class UniqueConstraint {
        private String schemaName;
        private String tableName;
        private String constraintName;

        public UniqueConstraint(String schemaName, String tableName, String constraintName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.constraintName = constraintName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getConstraintName() {
            return constraintName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UniqueConstraint that = (UniqueConstraint) o;

            if (constraintName != null ? !constraintName.equals(that.constraintName) : that.constraintName != null)
                return false;
            if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
            if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = schemaName != null ? schemaName.hashCode() : 0;
            result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
            result = 31 * result + (constraintName != null ? constraintName.hashCode() : 0);
            return result;
        }
    }
}
