package com.zenplanner.sql;

import com.google.common.base.Joiner;
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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class UuidTest extends TestCase {

    public UuidTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(UuidTest.class);
    }

    public void testApp() throws Exception {
        // Generate interesting UUIDs
        List<UUID> testUuids = new ArrayList<>();
        for(int i = 0; i < 16; i++) {
            byte[] bytes = new byte[16];
            for(int b = 0; b < 256; b += 64) {
                bytes[i] = (byte)b;
                UUID original = UuidUtil.byteArrayToUuid(bytes);
                testUuids.add(original);
            }
        }

        // Create select statement
        String valueClause = Joiner.on("')),\n(CONVERT(uniqueidentifier, '").join(testUuids);
        String sql = String.format("SELECT \n\t*\nFROM (\n\tVALUES (CONVERT(uniqueidentifier, '%s'))\n\t) tg(UUID)\norder by UUID", valueClause);

        // Get 100 random UUIDs sorted by SQL
        List<UUID> sqlList = new ArrayList<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        String conStr = "jdbc:jtds:sqlserver://qasqlcluster1.office.zen.corp:1433/ZenPlanner-QA1;user=zenwebqa;password=Intrepid!";
        try (Connection con = DriverManager.getConnection(conStr)) {
            try (Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        byte[] bytes = rs.getBytes(1);
                        String str = rs.getString(1);
                        UUID strUuid = UUID.fromString(str);
                        UUID binUuid = UuidUtil.byteArrayToUuid(bytes);
                        Assert.assertEquals(strUuid, binUuid);

                        byte[] out = UuidUtil.uuidToByteArray(binUuid);
                        Assert.assertTrue(Arrays.equals(bytes, out));

                        sqlList.add(binUuid);
                    }
                }
            }
        }

        // Clone the list and sort with Java
        List<UUID> javaList = sqlList.stream().sorted(UuidUtil::sqlUuidCompare).collect(Collectors.toList());

        // Test for correct order
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < javaList.size(); i++) {
            Comparable sqlUuid = sqlList.get(i);
            Comparable javaUuid = javaList.get(i);
            Assert.assertEquals(sqlUuid, javaUuid);
            sb.append(String.format("%s %s\n", sqlUuid, javaUuid));
        }
        String res = sb.toString();
        System.out.println(res);
    }

}
