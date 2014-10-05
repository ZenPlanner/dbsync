package com.zenplanner.sql;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UuidTest extends TestCase {

    private static final int count = 100;

    public UuidTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(UuidTest.class);
    }

    public void testApp() throws Exception {
        // Get 100 random UUIDs sorted by SQL
        List<Object> sqlList = new ArrayList<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        String conStr = "jdbc:jtds:sqlserver://localhost:1433/ZenPlanner-Development;user=zenwebdev;password=Enterprise!";
        try (Connection con = DriverManager.getConnection(conStr)) {
            try(Statement stmt = con.createStatement()) {
                String sql = String.format("select top %s NEWID() as UUID from sys.sysobjects order by UUID;", count);
                try(ResultSet rs = stmt.executeQuery(sql)) {
                    while(rs.next()) {
                        sqlList.add(rs.getObject("UUID"));
                    }
                }
            }
        }

        // Clone the list and sort with Java
        List<Object> javaList = sqlList.stream().sorted().collect(Collectors.toList());

        boolean eq = Arrays.equals(sqlList.toArray(), javaList.toArray());
        Assert.assertTrue(eq);
    }
}
