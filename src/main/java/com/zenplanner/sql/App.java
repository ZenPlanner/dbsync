package com.zenplanner.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        String filterValue = args[0];
        String srcCon = args[1];
        String dstCon = args[2];

        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        // Get tables and columns
        try (Connection scon = DriverManager.getConnection(srcCon)) {
            try (Connection dcon = DriverManager.getConnection(dstCon)) {
                DbComparator.Compare(scon, dcon, filterValue);
            }
        }
    }

}
