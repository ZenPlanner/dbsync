package com.zenplanner.sql;

import java.util.HashMap;

public class Table extends HashMap<String, Column> {
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
}
