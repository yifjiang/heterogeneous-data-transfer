package com.datayes.heterDataTransfer.sync;

import java.io.IOException;
import java.sql.SQLException;
import java.util.BitSet;

public class FullSyncClient {
    ClientServerSocket socket;
    ClientConfig clientConfig;
    MySQLConnector connector;

    FullSyncClient(){
        clientConfig = new ClientConfig();
        socket = new ClientServerSocket(
                clientConfig.getHostAddress(), clientConfig.getHostPort());
        socket.startClient();
    }

    void synchronize(String tablename) throws IOException, SQLException {
        socket.sendString(tablename);
        long minTmstamp = connector.getMinTimestamp(tablename);
        long maxTmstamp = connector.getMaxTimestamp(tablename);
        socket.sendLong(minTmstamp);
        socket.sendLong(maxTmstamp);
        long partitionSize = clientConfig.getPartitionSize();
        long partitionNum = (maxTmstamp-minTmstamp+1+partitionSize-1)/partitionSize;//ceil
        socket.sendLong(partitionNum);
        for (
                long begin = minTmstamp, end = begin+partitionSize;
                begin <= maxTmstamp;
                begin = end, end = begin+partitionSize)
        {
            BitSet timeStamps = connector.getTimeStamps(tablename, begin, end);
//        System.out.println(timeStamps);
            socket.sendLong(begin);
            socket.sendLong(end);
            socket.sendBitSet(timeStamps);
            DataSet dataSet = new DataSet();
            dataSet.constructFrom(socket);
            System.out.println(begin);
            connector.updateTable(tablename, dataSet, begin);
        }
        DataSet tmpDataSet = new DataSet();
        tmpDataSet.constructFrom(socket);
        connector.updateTable(tablename, tmpDataSet, 0);
        tmpDataSet = new DataSet();
        tmpDataSet.constructFrom(socket);
        connector.updateTable(tablename, tmpDataSet, maxTmstamp+1);
    }

    public void run(){
        //protocol
        try {
//            clientServerSocket.sendString("hello I'm client");
            connector = new MySQLConnector();
            socket.sendInt(1);
            synchronize("test1");
            socket.sendString("FIN ACK");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args){
        new FullSyncClient().run();
    }
}
