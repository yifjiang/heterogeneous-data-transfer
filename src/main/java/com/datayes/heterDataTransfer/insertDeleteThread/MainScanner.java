package com.datayes.heterDataTransfer.insertDeleteThread;
import com.datayes.heterDataTransfer.insertDeleteThread.Config;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class MainScanner extends Thread{

    Connection con;

    public MainScanner() throws ClassNotFoundException, SQLException {
        con = DriverManager.getConnection(Config.sqlConnectionUrl);
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        try {
            while (true) {
                System.out.println("hello world");
                Thread.currentThread().sleep(1000);
            }
        } catch(InterruptedException ex) {
            System.out.println("Interrupt");
        }

    }


}
