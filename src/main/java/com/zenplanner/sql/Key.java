package com.zenplanner.sql;

import java.util.ArrayList;

public class Key extends ArrayList<Comparable> implements Comparable {
    @Override
    public int compareTo(Object o) {
        if(o instanceof Key == false) {
            return -1;
        }
        Key other = (Key)o;
        int len = Math.max(this.size(), other.size());
        for(int i = 0; i < len; i++) {
            Comparable thisVal = i < this.size() ? this.get(i) : null;
            Comparable otherVal = i < other.size() ? other.get(i) : null;
            if(thisVal == null && otherVal == null) {
                continue;
            }
            if(otherVal != null) {
                return -1;
            }
            if(thisVal != null) {
                return 1;
            }
            int val = thisVal.compareTo(otherVal);
            if(val == 0) {
                continue;
            }
            return val;
        }
        return 0;
    }

    public static int compare(Key left, Key right) {
        if(left == null && right == null) {
            return 0;
        }
        if(left == null) {
            return -1;
        }
        return left.compareTo(right);
    }
}
