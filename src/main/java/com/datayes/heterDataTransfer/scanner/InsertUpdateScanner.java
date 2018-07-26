package com.datayes.heterDataTransfer.scanner;


import com.datayes.heterDataTransfer.server.ServerConfig;


import com.datayes.heterDataTransfer.util.Helper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lyhdk7 on 2018/6/18.
 */
public class InsertUpdateScanner extends Thread{

    Connection con;
    Connection monitorCon;
    String currentTable;
    private final Producer<String, byte[]> producer = new KafkaProducer<>(ServerConfig.kafkaProps);

    public InsertUpdateScanner(String tableName) throws ClassNotFoundException, SQLException {
        con = null;
        currentTable = tableName;
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
            stmt = con.createStatement();
            MSSQLRecorder mssqlRecorder = new MSSQLRecorder(monitorCon);

            if (ServerConfig.doMonitor) {
                mssqlRecorder.createTableIfNotExists();//monitor
            }

            String fileName = "./a_"+currentTable+".txt";




            while (true) {
                File readFile = new File(fileName);
                long newestId = 0;
                long newestTMP = 0;
                if (!readFile.exists()) {
                    readFile.createNewFile();
                    //Statement curstmt = con.createStatement();
                    rst = stmt.executeQuery(String.format("SELECT MAX(ID) FROM %s;", currentTable));
                    long largestID = 0;
                    while(rst.next()) {
                        largestID = rst.getLong(1);
                    }
                    rst = stmt.executeQuery(String.format("SELECT MAX(TMSTAMP) FROM %s;", currentTable));
                    long largestTMP = 0;
                    while(rst.next()) {

                        largestTMP = bytesToLong(rst.getBytes(1));
                        //System.out.println(largestTMP);
                    }
                    newestId = largestID;
                    newestTMP = largestTMP;


                }

                List<String> getLines = readFile2(readFile);

                if (getLines.size() != 0) {
                    newestId = Long.parseLong(getLines.get(0));
                    newestTMP = Long.parseLong(getLines.get(1));
                }

                rst = stmt.executeQuery(String.format("SELECT * FROM %s WHERE TMSTAMP > %d;",
                        currentTable,
                        newestTMP));

                ResultSetMetaData metaData = rst.getMetaData();
                int numCol = metaData.getColumnCount();
                ArrayList<String> columnNames = new ArrayList<>(numCol);
                ArrayList<Integer> columnTypes = new ArrayList<>(numCol);

                for (int j = 1; j <= numCol; ++j){
                    columnNames.add(metaData.getColumnName(j));
                    columnTypes.add(metaData.getColumnType(j));
                }



                long largestID = newestId;
                long largestTMP = newestTMP;
                List<IncrementMessageProtos.InsertUpdateContent> insertContents = new ArrayList<>();
                List<IncrementMessageProtos.InsertUpdateContent> updateContents = new ArrayList<>();

                while (rst.next()){
                    long curId = rst.getLong("ID");
                    long curTMP = bytesToLong(rst.getBytes("TMSTAMP"));
                    if (curId > largestID) largestID = curId;
                    if (curTMP > largestTMP) largestTMP = curTMP;


                    Map<String, String> tempMap = new HashMap<>();

                    if (curId > newestId) {
                        tempMap.put("OPERATION", "INSERT");

                    } else {
                        tempMap.put("OPERATION", "UPDATE");
                    }

                    List<String> contents = new ArrayList<>();

                    for (int j = 1; j <= numCol; ++j) {
                        if (columnTypes.get(j-1) == Types.DATE) {
                            final String str = rst.getDate(j).toString();
                            tempMap.put(columnNames.get(j-1), str);
                            contents.add(str);
                        } else if (columnTypes.get(j-1) == Types.DECIMAL) {
                            final String str = rst.getBigDecimal(j).toString();
                            tempMap.put(columnNames.get(j-1), str);
                            contents.add(str);
                        }
                        else {
                            byte[] toProcess = rst.getBytes(j);

                            final String str = helpToString(columnTypes.get(j-1), toProcess);
                            tempMap.put(columnNames.get(j-1), str);

                            contents.add(str);
                        }



                    }

                    IncrementMessageProtos.InsertUpdateContent insertUpdateContent =
                            IncrementMessageProtos.InsertUpdateContent.newBuilder()
                            .addAllValues(contents)
                            .build();

                    if (curId > newestId) {
                        insertContents.add(insertUpdateContent);
                    }
                    else {
                        updateContents.add(insertUpdateContent);
                    }


                    //System.out.println(tempMap.toString());

                }

                if (!insertContents.isEmpty()) {
                    IncrementMessageProtos.IncrementMessage message =
                            IncrementMessageProtos.IncrementMessage.newBuilder()
                            .setType(0)
                            .addAllFields(columnNames)
                            .addAllInsertUpdateContents(insertContents)
                            .build();
                    producer.send(new ProducerRecord<String, byte[]>(currentTable,
                            null, message.toByteArray()));

                    System.out.println("Insert: \n" + message.toString());

                    if (ServerConfig.doMonitor){
                        mssqlRecorder.record("INSERT", insertContents.size(), ServerConfig.capturedTableName);
                    }

                }

                if (!updateContents.isEmpty()) {
                    IncrementMessageProtos.IncrementMessage message =
                            IncrementMessageProtos.IncrementMessage.newBuilder()
                                    .setType(1)
                                    .addAllFields(columnNames)
                                    .addAllInsertUpdateContents(updateContents)
                                    .build();
                    producer.send(new ProducerRecord<String, byte[]>(currentTable,
                            null, message.toByteArray()));

                    System.out.println("Update: \n" + message.toString());

                    if (ServerConfig.doMonitor){
                        mssqlRecorder.record("UPDATE", updateContents.size(), ServerConfig.capturedTableName);
                    }

                }

                BufferedWriter out = new BufferedWriter(new FileWriter(fileName));

                out.write(largestID + "\n");
                out.write(largestTMP + "\n");
                out.close();





                //Thread.currentThread().sleep(2000);
            }
        /*} catch(InterruptedException ex) {
            System.out.println("Interrupt");*/
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        finally {
            closeQuietly(con, stmt, rst);
            producer.close();
        }

    }

    private static List<String> readFile2(File fin) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fin));
        List<String> result = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            result.add(line);
        }

        br.close();
        return result;
    }

    private static long bytesToLong(byte[] b) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    private static String helpToString(Integer type, byte[] toProcess) {
        return Helper.toString(type, toProcess);
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
