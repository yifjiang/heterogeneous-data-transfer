package com.datayes.heterDataTransfer.sync;

public class ClientConfig {
    long partitionSize = 5000;
    String sqlConnectionUrl = "jdbc:mysql://localhost/test?" +
            "user=root&password=root";
    String sqlClass = "com.mysql.cj.jdbc.Driver";
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
    long getPartitionSize(){ return partitionSize;}
}
