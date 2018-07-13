package com.datayes.heterDataTransfer.scanner;

import com.datayes.heterDataTransfer.server.ServerConfig;

import java.sql.*;
import java.util.HashSet;

/**
 * Created by lyhdk7 on 7/7/18.
 */
public class MainScanner extends Thread {

    Connection con;

    HashSet<String> tableSet;

    public MainScanner() throws ClassNotFoundException, SQLException {
        con = DriverManager.getConnection(ServerConfig.sqlConnectionUrl);
        tableSet = new HashSet<>();
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        try {
            while(true) {
                DatabaseMetaData tableMetaData =  con.getMetaData();
                ResultSet rst = tableMetaData.getTables(null, "dbo", "%", null);
                while(rst.next()) {
                    String currentTableName = rst.getString("TABLE_NAME");
                    if (!tableSet.contains(currentTableName)) {
                        tableSet.add(currentTableName);
                        InsertUpdateScanner insertUpdateScanner = new InsertUpdateScanner(currentTableName);
                        insertUpdateScanner.start();

                        DeleteScanner deleteScanner = new DeleteScanner(currentTableName);
                        deleteScanner.start();
                    }
                }




            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }
}
