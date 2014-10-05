package com.zenplanner.sql;

import com.sun.org.apache.xerces.internal.impl.dv.util.HexBin;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
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
                    String type = rs.getMetaData().getColumnTypeName(1);
                    while(rs.next()) {
                        String str = rs.getString(1);
                        byte[] bytes = rs.getBytes(1);
                        String text = HexBin.encode(bytes);
                        UUID byteUuid = UUID.nameUUIDFromBytes(bytes);
                        UUID strUuid = UUID.fromString(str);

                        ByteBuffer bb = ByteBuffer.wrap(bytes);
                        long firstLong = bb.getLong();
                        long secondLong = bb.getLong();
                        UUID bbUuid = new UUID(firstLong, secondLong);

                        byte[] buff = ByteBuffer.allocate(16)
                                .putLong(strUuid.getMostSignificantBits())
                                .putLong(strUuid.getLeastSignificantBits())
                                .array();
                        String hexStr = addDashes(HexBin.encode(buff));

                        Assert.assertEquals(byteUuid, strUuid);
                        sqlList.add(byteUuid);
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

    private String addDashes(String hex) {
        return String.format("%s-%s-%s-%s-%s",
                hex.substring(0,8),
                hex.substring(8,12),
                hex.substring(12,16),
                hex.substring(16,20),
                hex.substring(20,32));
    }
}
