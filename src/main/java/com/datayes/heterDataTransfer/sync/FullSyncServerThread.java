package com.datayes.heterDataTransfer.sync;

import java.util.BitSet;

public class FullSyncServerThread extends Thread {
    ClientServerSocket socket;
    SQLServerComparater sqlServerComparater;
    FullSyncServerThread(ClientServerSocket inClientServerSocket){
        socket = inClientServerSocket;
    }

    @Override
    public void run() {
        super.run();
        //start protocol
        try {
//            System.out.println(socket.recvString());
            sqlServerComparater = new SQLServerComparater();
            int numTable = socket.recvInt();
            System.out.println("numTable:"+numTable);
            for (int i = 0; i < numTable; i++) {
                String tableName = socket.recvString();
                long minTmstamp = sqlServerComparater.getMinTimestamp(tableName);
                long maxTmstamp = sqlServerComparater.getMaxTimestamp(tableName);
                socket.sendLong(minTmstamp);
                socket.sendLong(maxTmstamp);
                long partitionNum = socket.recvLong();
                for (int p = 0; p < partitionNum; ++p){
                    long begin = socket.recvLong();
                    long end = socket.recvLong();
                    BitSet clientTimeStamps = socket.recvBitSet();
//                System.out.println(clientTimeStamps);
                    DataSet dataSet = sqlServerComparater.calculateDifference(
                            tableName,
                            begin,
                            end,
                            clientTimeStamps
                    );
//                System.out.println(dataSet.residue);
                    dataSet.send(socket);
                }
                System.out.println("Table "+tableName+" synchronized");
            }
            System.out.println(socket.recvString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        socket.close();
    }
}
