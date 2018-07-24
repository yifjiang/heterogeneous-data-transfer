package com.datayes.heterDataTransfer.sync;

public class ClientConfig {
    long partitionSize = 5000;
    String sqlConnectionUrl = "jdbc:mysql://localhost/testMSSQL?" +
            "user=root&password=Vm450Group7";
    String sqlClass = "com.mysql.cj.jdbc.Driver";
    String hostAddress = "localhost";
    String[] tableToSynchronize = {"test"};
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
    String[] getTableToSynchronize(){ return tableToSynchronize; }
    int getHostPort(){
        return hostPort;
    }
    long getPartitionSize(){ return partitionSize;}
}
