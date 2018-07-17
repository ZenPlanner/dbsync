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
        if (constraints != null) {
            int res = JOptionPane.showConfirmDialog(this,
                    "Abnormal termination detected, would you like to restore constraints from backup file?",
                    "Warning", JOptionPane.YES_NO_OPTION);
            if (res == JOptionPane.YES_OPTION) {
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
                        if (comp.getCurrentTable() > comp.getTableCount()) {
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
        Map<String, List<Object>> filters = new HashMap<String, List<Object>>();
        List<Object> vals = Arrays.asList(tbFilterValue.getText().split(","));
        filters.put(tbFilterColumn.getText().toLowerCase(), vals);
        List<String> ignoreTables = Arrays.asList(tbIgnore.getText().split(","));
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


    {
// GUI initializer generated by IntelliJ IDEA GUI Designer
// >>> IMPORTANT!! <<<
// DO NOT EDIT OR ADD ANY CODE HERE!
        $$$setupUI$$$();
    }

    /**
     * Method generated by IntelliJ IDEA GUI Designer
     * >>> IMPORTANT!! <<<
     * DO NOT edit this method OR call it in your code!
     *
     * @noinspection ALL
     */
    private void $$$setupUI$$$() {
        panel1 = new JPanel();
        panel1.setLayout(new GridBagLayout());
        final JLabel label1 = new JLabel();
        label1.setText("Server");
        GridBagConstraints gbc;
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(2, 2, 2, 2);
        panel1.add(label1, gbc);
        tbSrcServer = new JTextField();
        tbSrcServer.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 1;
        gbc.weightx = 1.0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbSrcServer, gbc);
        final JLabel label2 = new JLabel();
        label2.setText("Database");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(2, 2, 2, 2);
        panel1.add(label2, gbc);
        tbSrcDb = new JTextField();
        tbSrcDb.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 2;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbSrcDb, gbc);
        final JLabel label3 = new JLabel();
        Font label3Font = this.$$$getFont$$$(null, Font.BOLD, -1, label3.getFont());
        if (label3Font != null) label3.setFont(label3Font);
        label3.setHorizontalAlignment(0);
        label3.setHorizontalTextPosition(0);
        label3.setText("Source");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 0;
        gbc.weightx = 1.0;
        panel1.add(label3, gbc);
        final JLabel label4 = new JLabel();
        label4.setText("Username");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(2, 2, 2, 2);
        panel1.add(label4, gbc);
        tbSrcUsername = new JTextField();
        tbSrcUsername.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 3;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbSrcUsername, gbc);
        final JPanel spacer1 = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 5;
        gbc.fill = GridBagConstraints.VERTICAL;
        panel1.add(spacer1, gbc);
        final JLabel label5 = new JLabel();
        Font label5Font = this.$$$getFont$$$(null, Font.BOLD, -1, label5.getFont());
        if (label5Font != null) label5.setFont(label5Font);
        label5.setHorizontalAlignment(0);
        label5.setHorizontalTextPosition(0);
        label5.setText("Destination");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 6;
        panel1.add(label5, gbc);
        final JLabel label6 = new JLabel();
        label6.setText("Server");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 7;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 2, 0, 0);
        panel1.add(label6, gbc);
        final JLabel label7 = new JLabel();
        label7.setText("Database");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 8;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 2, 0, 0);
        panel1.add(label7, gbc);
        final JLabel label8 = new JLabel();
        label8.setText("Username");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 9;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 2, 0, 0);
        panel1.add(label8, gbc);
        final JLabel label9 = new JLabel();
        label9.setText("Password");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 10;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 2, 0, 0);
        panel1.add(label9, gbc);
        final JLabel label10 = new JLabel();
        label10.setText("Password");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 2, 0, 0);
        panel1.add(label10, gbc);
        tbDstServer = new JTextField();
        tbDstServer.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 7;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbDstServer, gbc);
        tbDstDb = new JTextField();
        tbDstDb.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 8;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbDstDb, gbc);
        tbDstUsername = new JTextField();
        tbDstUsername.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 9;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbDstUsername, gbc);
        final JPanel spacer2 = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 12;
        gbc.fill = GridBagConstraints.VERTICAL;
        panel1.add(spacer2, gbc);
        pbMain = new JProgressBar();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 20;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(4, 4, 4, 4);
        panel1.add(pbMain, gbc);
        btnGo = new JButton();
        btnGo.setText("Synchronize");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 22;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(20, 100, 20, 100);
        panel1.add(btnGo, gbc);
        tbDstPassword = new JPasswordField();
        tbDstPassword.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 10;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbDstPassword, gbc);
        tbSrcPassword = new JPasswordField();
        tbSrcPassword.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 4;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbSrcPassword, gbc);
        final JPanel spacer3 = new JPanel();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 17;
        gbc.fill = GridBagConstraints.VERTICAL;
        panel1.add(spacer3, gbc);
        final JLabel label11 = new JLabel();
        label11.setHorizontalAlignment(0);
        label11.setHorizontalTextPosition(0);
        label11.setText("Filter");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 13;
        panel1.add(label11, gbc);
        final JLabel label12 = new JLabel();
        label12.setText("Filter Column");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 14;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label12, gbc);
        final JLabel label13 = new JLabel();
        label13.setText("Filter Value");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 15;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label13, gbc);
        tbFilterColumn = new JTextField();
        tbFilterColumn.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 14;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbFilterColumn, gbc);
        tbFilterValue = new JTextField();
        tbFilterValue.setText("");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 15;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel1.add(tbFilterValue, gbc);
        final JLabel label14 = new JLabel();
        label14.setText("Ignore Tables");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 16;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label14, gbc);
        tbIgnore = new JTextArea();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 16;
        gbc.fill = GridBagConstraints.BOTH;
        panel1.add(tbIgnore, gbc);
        final JLabel label15 = new JLabel();
        label15.setText("Current table");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 18;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label15, gbc);
        lblCurrentTable = new JLabel();
        lblCurrentTable.setText("None");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 18;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(lblCurrentTable, gbc);
        final JLabel label16 = new JLabel();
        label16.setText("Current row");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 19;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label16, gbc);
        lblCurrentRow = new JLabel();
        lblCurrentRow.setText("None");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 19;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(lblCurrentRow, gbc);
        final JLabel label17 = new JLabel();
        label17.setText("Delete");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 11;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label17, gbc);
        cbDelete = new JCheckBox();
        cbDelete.setText("Delete records not in source");
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 11;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(cbDelete, gbc);
        final JLabel label18 = new JLabel();
        label18.setText("Table");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 20;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label18, gbc);
        final JLabel label19 = new JLabel();
        label19.setText("Record");
        gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.gridy = 21;
        gbc.anchor = GridBagConstraints.WEST;
        panel1.add(label19, gbc);
        pbRecord = new JProgressBar();
        gbc = new GridBagConstraints();
        gbc.gridx = 1;
        gbc.gridy = 21;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(4, 4, 4, 4);
        panel1.add(pbRecord, gbc);
    }

    /**
     * @noinspection ALL
     */
    private Font $$$getFont$$$(String fontName, int style, int size, Font currentFont) {
        if (currentFont == null) return null;
        String resultName;
        if (fontName == null) {
            resultName = currentFont.getName();
        } else {
            Font testFont = new Font(fontName, Font.PLAIN, 10);
            if (testFont.canDisplay('a') && testFont.canDisplay('1')) {
                resultName = fontName;
            } else {
                resultName = currentFont.getName();
            }
        }
        return new Font(resultName, style >= 0 ? style : currentFont.getStyle(), size >= 0 ? size : currentFont.getSize());
    }

    /**
     * @noinspection ALL
     */
    public JComponent $$$getRootComponent$$$() {
        return panel1;
    }
}
