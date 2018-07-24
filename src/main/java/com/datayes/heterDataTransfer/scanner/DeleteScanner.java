package com.datayes.heterDataTransfer.scanner;

import com.datayes.heterDataTransfer.server.ServerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class DeleteScanner extends Thread{

    Connection con;
    Connection monitorCon;
    String currentTable;
    private final Producer<String, byte[]> producer = new KafkaProducer<>(ServerConfig.kafkaProps);
    final int fetchSize = 1000;
    BufferedReader br;

    public DeleteScanner(String tableName) throws ClassNotFoundException, SQLException {
        con = null;
        currentTable = tableName;
        br = null;
        if (ServerConfig.doMonitor){
            monitorCon = DriverManager.getConnection(ServerConfig.monitorDBURL);
        }
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        Statement stmt = null;
        ResultSet rst = null;
        try {
            con = DriverManager.getConnection(ServerConfig.sqlConnectionUrl);

            MSSQLRecorder mssqlRecorder = new MSSQLRecorder(monitorCon);

            if (ServerConfig.doMonitor) {
                mssqlRecorder.createTableIfNotExists();//monitor
            }

            String fileName = "./backup_"+currentTable+".txt";

            stmt = con.createStatement();


            while (true) {
                int maxID = 0;
                int lowerBound = 0;
                int upperBound = fetchSize;
                File readFile = new File(fileName);
                StringBuilder content = new StringBuilder();

                if (!readFile.exists()) {
                    readFile.createNewFile();
                }
                br = new BufferedReader(new FileReader(readFile));

                //Execute the query
                rst = stmt.executeQuery("SELECT MAX(ID) FROM " + currentTable);
                if(rst.next()){
                    maxID = rst.getInt(1);
                }

                //Read by partition
                List<Long> preIdSet = new ArrayList<>();
                List<Long> curIdSet = new ArrayList<>();
                int prePtr = 0;
                int curPtr = 0;
                List<Long> deletedIds = new ArrayList<>();

                while (true){
                    if (prePtr >= preIdSet.size()){
                        preIdSet = readIdListByPartition(br, fetchSize);
                        prePtr = 0;
                    }
                    if (curPtr >= curIdSet.size()){
                        rst = stmt.executeQuery("SELECT ID FROM " + currentTable + " " +
                                    "WHERE ID >= " + Integer.toString(lowerBound) + " and ID < " + Integer.toString(upperBound));
                        upperBound += fetchSize;
                        lowerBound += fetchSize;
                        curIdSet = readResultSet(rst);
                        content = fileWriter(content, curIdSet);
                        curPtr = 0;
                    }
                    while (prePtr < preIdSet.size() && curPtr < curIdSet.size()){
                        long curId = curIdSet.get(curPtr);
                        long preId = preIdSet.get(prePtr);
                        if (curId > preId){

                            Map<String, String> tempMap = new HashMap<>();
                            tempMap.put("OPERATION", "DELETE");
                            tempMap.put("ID", Long.toString(preId));

                            deletedIds.add(preId);

                            prePtr += 1;
                        } else if (curId == preId){
                            prePtr += 1;
                            curPtr += 1;
                        } else{
                            curPtr += 1;
                        }
                    }
                    if ((lowerBound > maxID && curIdSet.size() == 0) || preIdSet.size() == 0){
                        while (preIdSet.size() > 0){
                            while(prePtr < preIdSet.size()){
                                Map<String, String> tempMap = new HashMap<>();
                                tempMap.put("OPERATION", "DELETE");
                                tempMap.put("ID", Long.toString(preIdSet.get(prePtr)));
                                /*producer.send(new ProducerRecord<String, String>(currentTable,
                                        null, tempMap.toString()));*/

                                //System.out.println("new delete id: " + preIdSet.get(prePtr));
                                deletedIds.add(preIdSet.get(prePtr));
                                prePtr += 1;
                            }
                            preIdSet = readIdListByPartition(br, fetchSize);
                            prePtr = 0;
                        }
                        while(lowerBound <= maxID){
                            rst = stmt.executeQuery("SELECT ID FROM " + currentTable + " " +
                                    "WHERE ID >= " + Integer.toString(lowerBound) + " and ID < " + Integer.toString(upperBound));
                            upperBound += fetchSize;
                            lowerBound += fetchSize;
                            curIdSet = readResultSet(rst);
                            content = fileWriter(content, curIdSet);
                        }
                        break;
                    }
                }

                if (!deletedIds.isEmpty()) {

                    IncrementMessageProtos.IncrementMessage message = IncrementMessageProtos.IncrementMessage.newBuilder()
                            .setType(2)
                            .addAllDeleteIds(deletedIds)
                            .build();
                    producer.send(new ProducerRecord<String, byte[]>(currentTable,
                            null, message.toByteArray()));
                    System.out.println("Delete: \n" + message.toString());

                    if (ServerConfig.doMonitor){
                        mssqlRecorder.record("DELETE", deletedIds.size(), ServerConfig.capturedTableName);
                    }

                }

                BufferedWriter out = new BufferedWriter(new FileWriter(fileName, false));

                out.write(content.toString());
                out.close();
                br.close();

            }
//        } catch(InterruptedException ex) {
//            System.out.println("Interrupt");
        } catch(IOException e) {
            System.out.println("create file fail!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        finally {
            closeQuietly(con, stmt, rst);

            producer.close();
        }

    }

    private static List<Long> readIdListByPartition(BufferedReader br, int size) throws IOException {
        List<Long> result = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null && size-- > 0) {
            result.add(Long.parseLong(line));
        }
        return result;
    }

    private static List<Long> readResultSet(ResultSet rst) throws SQLException {
        List<Long> result = new ArrayList<>();
        while(rst.next()){
            result.add(rst.getLong("ID"));
        }
        return result;
    }

    private static StringBuilder fileWriter(StringBuilder content, List<Long> curIdSet){
        for (int i = 0; i < curIdSet.size(); i += 1){
            content.append(String.valueOf(curIdSet.get(i)) + "\n");
        }
        return content;
    }

    private void closeQuietly(AutoCloseable... args) {
        try {
            for (AutoCloseable closeable : args) {
                if (closeable != null) {
                    closeable.close();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


}
