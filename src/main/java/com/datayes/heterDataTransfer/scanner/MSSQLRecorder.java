package com.datayes.heterDataTransfer.scanner;

import com.datayes.heterDataTransfer.server.ServerConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MSSQLRecorder {

    Connection con;

    public MSSQLRecorder(Connection inCon) {con = inCon;}

    void createTableIfNotExists() throws SQLException {
        if (ServerConfig.doMonitor){
            String[] createIfNotExists = {
                    "if not exists (select * from sysobjects where name='"+ServerConfig.capturedTableName+"' and xtype='U') CREATE TABLE capturedCount(changeID BIGINT IDENTITY(1,1) PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT GETDATE())",
                    "if not exists (select * from sysobjects where name='sentCount' and xtype='U') CREATE TABLE sentCount(changeID BIGINT IDENTITY(1,1) PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT GETDATE())",
            };
            Statement stmtTmp = con.createStatement();
            for (String q : createIfNotExists) {
                stmtTmp.execute(q);
            }
        }
    }

    void record(String op, int numChange, String tableName) throws SQLException{
        String q = String.format(
                "INSERT INTO "+tableName+"(changeType, count) VALUES (\'%s\',%s)", op, numChange
        );
        System.out.println(q);
        Statement stmt = con.createStatement();
        stmt.execute(q);
        stmt.close();
    }
}
