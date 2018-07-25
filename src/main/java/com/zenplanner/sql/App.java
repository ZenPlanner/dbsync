package com.zenplanner.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private static final String conTemplate = "jdbc:jtds:sqlserver://%s:1433/%s;user=%s;password=%s";

    private static final DbComparator comp = new DbComparator();

    public static void main(String[] args) throws Exception {
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        try {
            if (args.length == 0) {
                FormMain frm = new FormMain();
                frm.setVisible(true);
            } else {
                Properties properties = comp.loadProps();

                String sourceServer = properties.getProperty("SourceServer");
                String sourceDb = properties.getProperty("SourceDb");
                String sourceUsername = properties.getProperty("SourceUsername");
                String sourcePassword = properties.getProperty("SourcePassword");
                String sourceCon = String.format(
                        conTemplate,
                        sourceServer,
                        sourceDb,
                        sourceUsername,
                        sourcePassword);

                String destServer = properties.getProperty("DestServer");
                String destDb = properties.getProperty("DestDb");
                String destUsername = properties.getProperty("DestUsername");
                String destPassword = properties.getProperty("DestPassword");
                String destCon = String.format(
                        conTemplate,
                        destServer,
                        destDb,
                        destUsername,
                        destPassword);

                Map<String, List<Object>> filters = new HashMap<>();
                List<Object> filterColumns = new ArrayList<Object>();
                // Filter column names must be lowercase to match table columns
                if (args[0].startsWith("-PartitionId")) {
                    String[] partitionIdArg = args[0].split("=");
                    filters.put("partitionid", Arrays.asList(partitionIdArg[1]));
                } else {
                    String filterValueList = properties.getProperty("FilterValue");
                    String[] filterValuesArray = filterValueList.split(",");
                    List<Object> filterValues = new ArrayList<Object>();
                    for (int i = 0; i < filterValuesArray.length; i++) {
                        filterValues.add("[" + filterValuesArray[i] + "]");
                    }
                    filters.put("partitionid", filterValues);
                }

                String ignoreTablesList = properties.getProperty("IgnoreTables");
                String[] ignoreTablesArray = ignoreTablesList.split(",");
                List<String> ignoreTables = Arrays.asList(ignoreTablesArray);

                boolean delete = (properties.getProperty("Delete")).equalsIgnoreCase("true");

                try (Connection scon = DriverManager.getConnection(sourceCon)) {
                    System.out.println("Established source connection");
                    scon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    try (Connection dcon = DriverManager.getConnection(destCon)) {
                        System.out.println("Established destination connection");
                        System.out.println("Filter Column");
                        Set<String> filterColumnNames = filters.keySet();
                        for (String filterColumnName : filterColumnNames) {
                            System.out.println("- " + filterColumnName);
                            List<Object> filterColumnValues = filters.get(filterColumnName);
                            for (Object filterColumnValue : filterColumnValues) {
                                System.out.println("-- " + filterColumnValue);
                            }
                        }
                        System.out.println("Ignore Tables");
                        for (String ignoreTable : ignoreTables) {
                            System.out.println("- " + ignoreTable);
                        }
                        System.out.println("Delete=" + delete);
                        comp.synchronize(scon, dcon, filters, ignoreTables, delete);
                    }
                }
            }
        } catch (Exception e) {
            comp.loadProps();
        }
        System.out.println("Done.");
    }
}
