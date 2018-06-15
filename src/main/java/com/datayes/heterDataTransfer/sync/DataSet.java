package com.datayes.heterDataTransfer.sync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;

public class DataSet {
    int numCol;
    int numRow;
    ArrayList<ArrayList<byte[]>> data;
    ArrayList<Integer> byteNums;
    ArrayList<String> columnNames;
    ArrayList<Integer> columnTypes;
    BitSet residue;

    public String constructFrom(
            ResultSet rst,
            BitSet filter,
            long offset)
            throws SQLException
    {

        ResultSetMetaData metaData = rst.getMetaData();
        numCol = metaData.getColumnCount();
        data = new ArrayList<ArrayList<byte[]>>();
        columnNames = new ArrayList<>(numCol);
        columnTypes = new ArrayList<>(numCol);
        byteNums = new ArrayList<>(numCol);

        StringBuilder debugMessage = new StringBuilder();
        for (int j = 1; j <= numCol; ++j){
            debugMessage.append(String.format("%s %s, ",
                    metaData.getColumnName(j),
                    metaData.getColumnTypeName(j)));
            columnNames.add(metaData.getColumnName(j));
            columnTypes.add(metaData.getColumnType(j));
        }
        debugMessage.append("\n");

        while (rst.next()) {
            byte[] timeBytes = rst.getBytes("TMSTAMP");
            ByteBuffer wrapped = ByteBuffer.wrap(timeBytes);
            long timeStamp = wrapped.getLong();
//            System.out.println(timeStamp);
            assert timeStamp-offset <= Integer.MAX_VALUE;
            if (filter.get((int) (timeStamp - offset))) {
                filter.clear((int) (timeStamp - offset));
                continue;
            }
            data.add(new ArrayList<byte[]>(numCol));
            ArrayList<byte[]> row = data.get(data.size()-1);
            for (int j = 1; j<= numCol; j++){
                byte[] block = rst.getBytes(j);
                if (j > byteNums.size()){
                    byteNums.add(block.length);
                }
                row.add(block);
                debugMessage.append(rst.getString(j) + ", ");
            }
            data.set(data.size()-1, row);
            debugMessage.append("\n");
        }
        numRow = data.size();
        residue = filter;
        assert numCol == byteNums.size();

        return debugMessage.toString();
    }

    public void send(ClientServerSocket socket) throws IOException {
        socket.sendInt(numCol);
        for (String name:columnNames) socket.sendString(name);
        for (int type:columnTypes) socket.sendInt(type);
        socket.sendInt(numRow);
        if (numRow > 0) {
            for (int byteNum : byteNums
                    ) {
                socket.sendInt(byteNum);
            }
        }
        for (ArrayList<byte[]> row:data
             ) {
            for (byte[] block:row
                 ) {
                socket.sendByteAry(block);
            }
        }
        socket.sendBitSet(residue);
    }

    public void constructFrom(ClientServerSocket socket) throws IOException {
        numCol = socket.recvInt();
        byteNums = new ArrayList<>(numCol);
        columnNames = new ArrayList<>(numCol);
        columnTypes = new ArrayList<>(numCol);
        byteNums = new ArrayList<>(numCol);
        for (int i = 0; i < numCol; ++i){
            columnNames.add(socket.recvString());
        }
        for (int i = 0; i < numCol; ++i){
            columnTypes.add(socket.recvInt());
        }
        numRow = socket.recvInt();
        if (numRow > 0) {//TODO
            for (int i = 0; i < numCol; ++i) {
                byteNums.add(socket.recvInt());
            }
        }
        data = new ArrayList<>(numRow);
        for (int i = 0; i < numRow; ++i){
            ArrayList<byte[]> row = new ArrayList<byte[]>(numCol);
            for (int j = 0; j < numCol; ++j){
                row.add(socket.recvByteAry(byteNums.get(j)));
            }
            data.add(row);
        }
        residue = socket.recvBitSet();
    }
}
