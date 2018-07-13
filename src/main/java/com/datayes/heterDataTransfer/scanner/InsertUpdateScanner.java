package com.datayes.heterDataTransfer.scanner;


import com.datayes.heterDataTransfer.server.ServerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.ByteBuffer;
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
    String currentTable;
    private final Producer<String, String> producer = new KafkaProducer<>(ServerConfig.kafkaProps);

    public InsertUpdateScanner(String tableName) throws ClassNotFoundException, SQLException {
        con = DriverManager.getConnection(ServerConfig.sqlConnectionUrl);
        currentTable = tableName;
    }

    Connection getConnection(){
        return con;
    }

    public void run() {
        try {

            while (true) {
                //System.out.println("hello world");

                File readFile = new File("./a.txt");
                if (!readFile.exists()) {
                    readFile.createNewFile();
                }
                List<String> getLines = readFile2(readFile);
                long newestId = 0;
                long newestTMP = 0;
                if (getLines.size() != 0) {
                    newestId = Long.parseLong(getLines.get(0));
                    newestTMP = Long.parseLong(getLines.get(1));
                }

                Statement stmt = con.createStatement();
                ResultSet rst = stmt.executeQuery(String.format("SELECT * FROM %s WHERE TMSTAMP > %d;",
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

                    for (int j = 1; j <= numCol; ++j) {
                        byte[] toProcess = rst.getBytes(j);
                        ByteBuffer wrapped = ByteBuffer.wrap(toProcess);

                        tempMap.put(columnNames.get(j-1), helpToString(columnTypes.get(j-1), toProcess));



                    }
                    producer.send(new ProducerRecord<String, String>(currentTable,
                            null, tempMap.toString()));

                    System.out.println("Message sent successfully");
                    System.out.println(tempMap.toString());

                }

                BufferedWriter out = new BufferedWriter(new FileWriter("./a.txt"));

                out.write(largestID + "\n");
                out.write(largestTMP + "\n");
                out.close();



                //Thread.currentThread().sleep(2000);
            }
        /*} catch(InterruptedException ex) {
            System.out.println("Interrupt");*/
        } catch(IOException e) {
            System.out.println("create file fail!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        finally {
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
        ByteBuffer wrapped = ByteBuffer.wrap(toProcess);
        switch (type) {
            case Types.TIMESTAMP:
                return Long.toString(wrapped.getLong());
            case Types.BIGINT:
                return Long.toString(wrapped.getLong());
            case Types.BINARY:
                return Long.toString(wrapped.getLong());
            case Types.BOOLEAN:
                return Boolean.toString(toProcess[0] != 0);
            case Types.CHAR:
                return "\'" + new String(toProcess) + "\'";
            case Types.DOUBLE:
                return Double.toString(wrapped.getDouble());
            case Types.FLOAT:
                return Double.toString(wrapped.getDouble());
            case Types.VARCHAR:
                return "\'" + new String(toProcess) + "\'";
            case Types.LONGNVARCHAR:
                return new String(toProcess);
            case Types.BIT:
                return Boolean.toString(toProcess[0] != 0);
            case Types.TINYINT:
                return Integer.toString((int) toProcess[0]);
            case Types.SMALLINT:
                return Short.toString(wrapped.getShort());
            case Types.INTEGER:
                return Integer.toString(wrapped.getInt());
            case Types.REAL:
                return Float.toString(wrapped.getFloat());
            default:
                return "unhandled type:" + Integer.toString(type);
            //TODO: unhandled data types and testing
        }
    }


}
