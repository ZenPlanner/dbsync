package com.zenplanner.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        Class.forName("net.sourceforge.jtds.jdbc.Driver");

        if(args.length < 3) {
            FormMain frm = new FormMain();
            frm.setVisible(true);
        } else {
            sync(args[0], args[1], args[2]);
        }
    }

    private static void sync(String srcCon, String dstCon, String filterValue) throws Exception {
        try (Connection scon = DriverManager.getConnection(srcCon)) {
            try (Connection dcon = DriverManager.getConnection(dstCon)) {
                DbComparator.Syncronize(scon, dcon, filterValue);
            }
        }
    }

}
