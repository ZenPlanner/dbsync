package com.zenplanner.sql;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
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
        List<Comparable<?>> sqlList = new ArrayList<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        String conStr = "jdbc:jtds:sqlserver://localhost:1433/ZenPlanner-Development;user=zenwebdev;password=Enterprise!";
        try (Connection con = DriverManager.getConnection(conStr)) {
            try(Statement stmt = con.createStatement()) {
                String sql = String.format("select top %s NEWID() as UUID from sys.sysobjects order by UUID;", count);
                try(ResultSet rs = stmt.executeQuery(sql)) {
                    while(rs.next()) {
                        byte[] bytes = rs.getBytes(1);
                        UUID uuid = byteArrayToUuid(bytes);
                        sqlList.add(uuid);
                    }
                }
            }
        }

        // Clone the list and sort with Java
        List<Comparable<?>> javaList = sqlList.stream().sorted().collect(Collectors.toList());

        // Test for correct order
        boolean eq = true;
        for(int i = 0; i < javaList.size(); i++) {
            Comparable sqlId = sqlList.get(i);
            Comparable javaId = javaList.get(i);
            if(sqlId.compareTo(javaId) != 0) {
                eq = false;
            }
        }
        Assert.assertTrue(eq);
    }

    private UUID byteArrayToUuid(byte[] bytes) {
        if(bytes.length != 16) {
            throw new RuntimeException("Invalid UUID bytes!");
        }
        byte[] time_low = Arrays.copyOfRange(bytes, 0, 4);
        ArrayUtils.reverse(time_low);
        byte[] time_mid = Arrays.copyOfRange(bytes, 4, 6);
        ArrayUtils.reverse(time_mid);
        byte[] time_hi = Arrays.copyOfRange(bytes, 6, 8); // actually time_hi + version
        ArrayUtils.reverse(time_hi);
        byte[] node = Arrays.copyOfRange(bytes, 8, 16); // actually variant + clock_seq + node

        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.put(time_low);
        bb.put(time_mid);
        bb.put(time_hi);
        bb.put(node);

        bb.rewind();
        long hi = bb.getLong();
        long low = bb.getLong();

        UUID uuid = new UUID(hi, low);
        return uuid;
    }

    private String addDashes(String hex) {
        return String.format("%s-%s-%s-%s-%s",
                hex.substring(0,8),
                hex.substring(8,12),
                hex.substring(12,16),
                hex.substring(16,20),
                hex.substring(20,32));
    }
}
