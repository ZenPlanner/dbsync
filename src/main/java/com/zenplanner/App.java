package com.zenplanner;

import java.sql.Connection;
import java.sql.DriverManager;

public class App {
    public static void main(String[] args) throws Exception {
        String sourceCon = args[0];
        //String destCon = args[1];

        //Class.forName("org.postgresql.Driver");
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        try(Connection connection = DriverManager.getConnection(sourceCon)) {

        }
    }
}
