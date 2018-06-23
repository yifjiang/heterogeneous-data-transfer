package com.datayes.heterDataTransfer.insertDeleteThread;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class Config {
    static String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;" +
            "databaseName=testDB;" +
            "user=sa;password=Vm450Group7;";
    static String sqlClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static String hostAddress = "localhost";
    static int hostPort = 8001;

    static String topicName = "Changes";
}
