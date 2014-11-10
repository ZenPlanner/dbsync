package com.zenplanner.sql;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import javax.swing.*;
import javax.swing.Timer;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.List;

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
    private JCheckBox cbDelete;
    private JProgressBar pbRecord;

    PeriodFormatter timeFormatter = new PeriodFormatterBuilder()
            .printZeroAlways()
            .appendMinutes()
            .appendSeparator(":")
            .appendSeconds()
            .toFormatter();

    private static final String conTemplate = "jdbc:jtds:sqlserver://%s:1433/%s;user=%s;password=%s";
    private final DbComparator comp = new DbComparator();
    private Instant startTime;

    public FormMain() {
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        add(panel1);
        setSize(800, 600);
        setVisible(true);
        pack();
        loadProps();

        // Restore constraints if the app crashed
        Map<String, List<String>> constraints = comp.loadConstraints();
        if(constraints != null) {
            int res = JOptionPane.showConfirmDialog(this,
                    "Abnormal termination detected, would you like to restore constraints from backup file?",
                    "Warning", JOptionPane.YES_NO_OPTION);
            if(res == JOptionPane.YES_OPTION){
                String dstCon = getDstCon();
                try (Connection dcon = DriverManager.getConnection(dstCon)) {
                    comp.setConstraints(dcon, constraints, true);
                } catch (Exception ex) {
                    throw new RuntimeException("Error restoring constraints!", ex);
                }
                comp.unloadConstraints();
            }
        }

        Timer timer = new Timer(100, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                pbMain.setMaximum(comp.getRowCount());
                pbMain.setValue(comp.getCurrentRow());
                pbRecord.setMaximum(comp.getModCount());
                pbRecord.setValue(comp.getCurrentMod());
                lblCurrentTable.setText(comp.getCurrentTableName());
                lblCurrentRow.setText("" + comp.getCurrentRow() + " / " + comp.getRowCount());
            }
        });

        comp.addListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                SwingUtilities.invokeLater(new Runnable() {
                    @Override
                    public void run() {
                        if(comp.getCurrentTable() > comp.getTableCount()) {
                            timer.stop();
                            lblCurrentTable.setText("");
                            lblCurrentRow.setText("");
                            btnGo.setEnabled(true);
                            pbMain.setValue(0);
                            Duration delta = new Duration(startTime, Instant.now());
                            String text = timeFormatter.print(delta.toPeriod());
                            JOptionPane.showMessageDialog(FormMain.this, "Synchronized in " + text);
                        }
                    }
                });
            }
        });

        btnGo.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                startTime = Instant.now();
                btnGo.setEnabled(false);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            timer.start();
                            saveProps();
                            sync();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            throw new RuntimeException("Error syncing DBs!", ex); // TODO: Pop-up
                        }
                    }
                }).start();
            }
        });

    }

    private String getDstCon() {
        String dstCon = String.format(conTemplate, tbDstServer.getText(), tbDstDb.getText(),
                tbDstUsername.getText(), tbDstPassword.getText());
        return dstCon;
    }

    private void sync() throws Exception {
        Map<String,List<Object>> filters = new HashMap<String,List<Object>>();
        List<Object> vals = Arrays.asList(tbFilterValue.getText().split(","));
        filters.put(tbFilterColumn.getText().toLowerCase(), vals);
        java.util.List<String> ignoreTables = Arrays.asList(tbIgnore.getText().split(","));
        boolean delete = cbDelete.isSelected();

        String srcCon = String.format(conTemplate, tbSrcServer.getText(), tbSrcDb.getText(),
                tbSrcUsername.getText(), tbSrcPassword.getText());
        String dstCon = getDstCon();
        try (Connection scon = DriverManager.getConnection(srcCon)) {
            scon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            try (Connection dcon = DriverManager.getConnection(dstCon)) {
                comp.synchronize(scon, dcon, filters, ignoreTables, delete);
            }
        }
    }

    private void loadProps() {
        try {
            Properties props = comp.loadProps();
            tbSrcServer.setText(props.getProperty("SourceServer"));
            tbSrcDb.setText(props.getProperty("SourceDb"));
            tbSrcUsername.setText(props.getProperty("SourceUsername"));
            tbSrcPassword.setText(props.getProperty("SourcePassword"));

            tbDstServer.setText(props.getProperty("DestServer"));
            tbDstDb.setText(props.getProperty("DestDb"));
            tbDstUsername.setText(props.getProperty("DestUsername"));
            tbDstPassword.setText(props.getProperty("DestPassword"));

            try {
                cbDelete.setSelected(Boolean.parseBoolean(props.getProperty("Delete")));
            } catch (Exception ex) {
                // Ignore parse errors
            }
            tbFilterColumn.setText(props.getProperty("FilterColumn"));
            tbFilterValue.setText(props.getProperty("FilterValue"));

            tbIgnore.setText(props.getProperty("IgnoreTables"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
     }

    private void saveProps() throws Exception {
        Properties props = comp.loadProps();

        props.setProperty("SourceServer", tbSrcServer.getText());
        props.setProperty("SourceDb", tbSrcDb.getText());
        props.setProperty("SourceUsername", tbSrcUsername.getText());
        props.setProperty("SourcePassword", tbSrcPassword.getText());

        props.setProperty("DestServer", tbDstServer.getText());
        props.setProperty("DestDb", tbDstDb.getText());
        props.setProperty("DestUsername", tbDstUsername.getText());
        props.setProperty("DestPassword", tbDstPassword.getText());

        props.setProperty("Delete", "" + cbDelete.isSelected());
        props.setProperty("FilterColumn", tbFilterColumn.getText());
        props.setProperty("FilterValue", tbFilterValue.getText());

        props.setProperty("IgnoreTables", tbIgnore.getText());

        comp.saveProps(props);
    }


}
