package com.zenplanner.sql;

import com.sun.javafx.image.ByteToBytePixelConverter;
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

    private static final int count = 10000;

    public UuidTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(UuidTest.class);
    }

    public void testApp() throws Exception {
        for(int i = 0; i < 16; i++) {
            byte[] bytes = new byte[16];
            for(int b = 0; b < 256; b++) {
                bytes[i] = (byte)b;
                UUID original = byteArrayToUuid(bytes);
                System.out.println(original);
            }
        }

        // Get 100 random UUIDs sorted by SQL
        List<UUID> sqlList = new ArrayList<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        String conStr = "jdbc:jtds:sqlserver://localhost:1433/ZenPlanner-Development;user=zenwebdev;password=Enterprise!";
        try (Connection con = DriverManager.getConnection(conStr)) {
            try (Statement stmt = con.createStatement()) {
                String sql = String.format("select top %s NEWID() as UUID from sys.sysobjects order by UUID;", count);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        byte[] bytes = rs.getBytes(1);
                        String str = rs.getString(1);
                        UUID strUuid = UUID.fromString(str);
                        UUID binUuid = byteArrayToUuid(bytes);
                        Assert.assertEquals(strUuid, binUuid);

                        byte[] out = uuidToByteArray(binUuid);
                        Assert.assertTrue(Arrays.equals(bytes, out));

                        sqlList.add(binUuid);
                    }
                }
            }
        }

        // Clone the list and sort with Java
        List<UUID> javaList = sqlList.stream().sorted(UuidTest::sqlUuidCompare).collect(Collectors.toList());

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

    private static byte[] uuidToByteArray(UUID uuid) {

        // Turn into byte array
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        bb.rewind();
        byte[] bytes = new byte[16];
        bb.get(bytes);

        // Transform
        bb = transformUuid(bytes);

        // Turn into byte array
        bb.get(bytes);

        return bytes;
    }

    private static UUID byteArrayToUuid(byte[] bytes) {
        ByteBuffer bb = transformUuid(bytes);
        long hi = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(hi, low);
        return uuid;
    }

    private static int sqlUuidCompare(UUID leftUuid, UUID rightUuid) {
        byte[] leftBytes = uuidToByteArray(leftUuid);
        byte[] rightBytes = uuidToByteArray(rightUuid);

        // Compare node
        byte[] leftNode = Arrays.copyOfRange(leftBytes, 10, 16); // node
        byte[] rightNode = Arrays.copyOfRange(rightBytes, 10, 16); // node
        for(int i = 10; i < 16; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        throw new RuntimeException("Comparison beyond node not implemented!");
    }

    private static ByteBuffer transformUuid(byte[] bytes) {
        if (bytes.length != 16) {
            throw new RuntimeException("Invalid UUID bytes!");
        }

        // Get hi
        byte[] time_low = Arrays.copyOfRange(bytes, 0, 4);
        byte[] time_mid = Arrays.copyOfRange(bytes, 4, 6);
        byte[] time_hi = Arrays.copyOfRange(bytes, 6, 8); // actually time_hi + version

        // Get low
        byte[] clock_seq = Arrays.copyOfRange(bytes, 8, 10); // actually variant + clock_seq
        byte[] node = Arrays.copyOfRange(bytes, 10, 16); // node

        // Transform
        ArrayUtils.reverse(time_low);
        ArrayUtils.reverse(time_mid);
        ArrayUtils.reverse(time_hi);

        // Rebuild
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.put(time_low);
        bb.put(time_mid);
        bb.put(time_hi);
        bb.put(clock_seq);
        bb.put(node);

        // Grab longs
        bb.rewind();
        return bb;
    }

    private static String addDashes(String hex) {
        return String.format("%s-%s-%s-%s-%s",
                hex.substring(0, 8),
                hex.substring(8, 12),
                hex.substring(12, 16),
                hex.substring(16, 20),
                hex.substring(20, 32));
    }
}
