package com.zenplanner.sql;

import javax.swing.*;
import javax.swing.Timer;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class FormMain extends JFrame {
    private JPanel panel1;
    private JTextField tbSrcServer;
    private JTextField tbSrcDb;
    private JTextField tbSrcUsername;
    private JPasswordField tbSrcPassword;
    private JTextField tbDstServer;
    private JTextField tbDstDb;
    private JTextField tbDstUsername;
    private JProgressBar pbMain;
    private JButton btnGo;
    private JPasswordField tbDstPassword;
    private JTextField tbFilterColumn;
    private JTextField tbFilterValue;
    private JTextArea tbIgnore;
    private JLabel lblCurrentTable;
    private JLabel lblCurrentRow;

    private static final String conTemplate = "jdbc:jtds:sqlserver://%s:1433/%s;user=%s;password=%s";
    private final DbComparator comp = new DbComparator();

    public FormMain() {
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        add(panel1);
        setSize(800, 600);
        setVisible(true);
        pack();

        new Timer(100, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                pbMain.setMaximum(comp.getRowCount());
                pbMain.setValue(comp.getCurrentRow());
                lblCurrentTable.setText(comp.getCurrentTableName());
                lblCurrentRow.setText("" + comp.getCurrentRow() + " / " + comp.getRowCount());
            }
        }).start();

        comp.addListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        if(comp.getCurrentTable() > comp.getTableCount()) {
                            JOptionPane.showMessageDialog(null, "Synchronization complete!");
                            btnGo.setEnabled(true);
                            pbMain.setValue(0);
                        }
                    }
                });
            }
        });

        btnGo.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                btnGo.setEnabled(false);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            saveProps();
                            sync();
                        } catch (Exception ex) {
                            throw new RuntimeException("Error syncing DBs!", ex); // TODO: Pop-up
                        }
                    }
                }).start();
            }
        });

        loadProps();
    }

    private void sync() throws Exception {
        Map<String,Object> filters = new HashMap<String,Object>();
        filters.put(tbFilterColumn.getText().toLowerCase(), tbFilterValue.getText());
        String srcCon = String.format(conTemplate, tbSrcServer.getText(), tbSrcDb.getText(),
                tbSrcUsername.getText(), tbSrcPassword.getText());
        String dstCon = String.format(conTemplate, tbDstServer.getText(), tbDstDb.getText(),
                tbDstUsername.getText(), tbDstPassword.getText());
        java.util.List<String> ignoreTables = Arrays.asList(tbIgnore.getText().split(","));
        try (Connection scon = DriverManager.getConnection(srcCon)) {
            try (Connection dcon = DriverManager.getConnection(dstCon)) {
                comp.synchronize(scon, dcon, filters, ignoreTables);
            }
        }
    }

    private File getPropFile() {
        File dir = new File(FormMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        if("classes".equals(dir.getName())) {
            dir = dir.getParentFile();
        }
        File f = new File(dir, "dbsync.properties");
        return f;
    }

    private void loadProps() {
        try {
            Properties props = new Properties();
            try(InputStream is = new FileInputStream( getPropFile() )) {
                props.load( is );
                tbSrcServer.setText(props.getProperty("SourceServer"));
                tbSrcDb.setText(props.getProperty("SourceDb"));
                tbSrcUsername.setText(props.getProperty("SourceUsername"));
                tbSrcPassword.setText(props.getProperty("SourcePassword"));

                tbDstServer.setText(props.getProperty("DestServer"));
                tbDstDb.setText(props.getProperty("DestDb"));
                tbDstUsername.setText(props.getProperty("DestUsername"));
                tbDstPassword.setText(props.getProperty("DestPassword"));

                tbFilterColumn.setText(props.getProperty("FilterColumn"));
                tbFilterValue.setText(props.getProperty("FilterValue"));

                tbIgnore.setText(props.getProperty("IgnoreTables"));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            // If there's an error, they just don't get pre-loaded properties
        }
     }

    private void saveProps() throws Exception {
        Properties props = new Properties();
        props.setProperty("SourceServer", tbSrcServer.getText());
        props.setProperty("SourceDb", tbSrcDb.getText());
        props.setProperty("SourceUsername", tbSrcUsername.getText());
        props.setProperty("SourcePassword", tbSrcPassword.getText());

        props.setProperty("DestServer", tbDstServer.getText());
        props.setProperty("DestDb", tbDstDb.getText());
        props.setProperty("DestUsername", tbDstUsername.getText());
        props.setProperty("DestPassword", tbDstPassword.getText());

        props.setProperty("FilterColumn", tbFilterColumn.getText());
        props.setProperty("FilterValue", tbFilterValue.getText());

        props.setProperty("IgnoreTables", tbIgnore.getText());

        try(OutputStream out = new FileOutputStream( getPropFile() )) {
            props.store(out, "dbsync properties");
        }
    }


}
