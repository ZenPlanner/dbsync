package com.zenplanner.sql;

import javax.swing.*;

public class FormMain extends JFrame {
    private JPanel panel1;
    private JTextField tbSrcServer;
    private JTextField tbSrcDb;
    private JTextField tbSrcUsername;
    private JTextField tbSrcPassword;
    private JTextField textField1;
    private JTextField tbDstServer;
    private JTextField tbDstDb;
    private JTextField tbDstUsername;
    private JTextField tbDstPassword;
    private JProgressBar pbMain;
    private JButton tbnGo;

    public FormMain() {
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        add(panel1);
        setSize(800, 600);
        setVisible(true);
        pack();
    }
}
