package com.datayes.heterDataTransfer.sync;

public class ServerConfig {
    String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;" +
            "databaseName=testMSSQL;" +
            "user=sa;password=Vm450Group7;";
    String sqlClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    String hostAddress = "localhost";
    int hostPort = 8001;
    String getSqlConnectionUrl(){
        return sqlConnectionUrl;
    }
    String getSqlClass(){
        return sqlClass;
    }
    String getHostAddress(){
        return hostAddress;
    }
    int getHostPort(){
        return hostPort;
    }
}
