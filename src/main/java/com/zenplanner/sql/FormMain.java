package com.zenplanner.sql;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;

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
    private JTextField tbPartitionId;

    private static final String conTemplate = "jdbc:jtds:sqlserver://%s:1433/%s;user=%s;password=%s";

    public FormMain() {
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        add(panel1);
        setSize(800, 600);
        setVisible(true);
        pack();

        btnGo.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                btnGo.setEnabled(false);
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String srcCon = String.format(conTemplate, tbSrcServer.getText(), tbSrcDb.getText(),
                                    tbSrcUsername.getText(), tbSrcPassword.getText());
                            String dstCon = String.format(conTemplate, tbDstServer.getText(), tbDstDb.getText(),
                                    tbDstUsername.getText(), tbDstPassword.getText());
                            try (Connection scon = DriverManager.getConnection(srcCon)) {
                                try (Connection dcon = DriverManager.getConnection(dstCon)) {
                                    DbComparator comp = new DbComparator();
                                    comp.synchronize(scon, dcon, tbPartitionId.getText());
                                }
                            }
                        } catch (Exception ex) {
                            throw new RuntimeException("Error syncing DBs!", ex); // TODO: Pop-up
                        }
                    }
                }).start();
            }
        });
    }
}
