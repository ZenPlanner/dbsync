package com.zenplanner.sql;

import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class UuidUtil {

    public static byte[] uuidToByteArray(UUID uuid) {

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

    public static UUID byteArrayToUuid(byte[] bytes) {
        ByteBuffer bb = transformUuid(bytes);
        long hi = bb.getLong();
        long low = bb.getLong();
        UUID uuid = new UUID(hi, low);
        return uuid;
    }

    public static int sqlUuidCompare(UUID leftUuid, UUID rightUuid) {
        byte[] leftBytes = uuidToByteArray(leftUuid);
        byte[] rightBytes = uuidToByteArray(rightUuid);

        // Compare node
        for(int i = 10; i < 16; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        // Compare clock_seq
        for(int i = 8; i < 10; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        // Compare time_hi
        for(int i = 6; i < 8; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        // Compare time_mid
        for(int i = 4; i < 6; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        // Compare time_low
        for(int i = 0; i < 4; i++) {
            int val = (leftBytes[i] & 0xFF) - (rightBytes[i] & 0xFF);
            if(val != 0) {
                return val;
            }
        }

        return 0; // They are equal!
    }

    public static ByteBuffer transformUuid(byte[] bytes) {
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

    public static String addDashes(String hex) {
        return String.format("%s-%s-%s-%s-%s",
                hex.substring(0, 8),
                hex.substring(8, 12),
                hex.substring(12, 16),
                hex.substring(16, 20),
                hex.substring(20, 32));
    }
}
