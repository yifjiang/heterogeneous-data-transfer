package com.datayes.heterDataTransfer.client;

import com.datayes.heterDataTransfer.scanner.IncrementMessageProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatabaseClient implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(ClientConfig.kafkaProps);
    private Connection con;
    private Connection monitorConnection;
    private final String currentTable;

    DatabaseClient(String table) {
        currentTable = table;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(ClientConfig.sqlConnectionUrl);
            if (ClientConfig.doMonitor) {
                monitorConnection = DriverManager.getConnection(ClientConfig.monitorDBURL);
            }
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    private void record(String op, int numChange, String tableName) throws SQLException{
        String q = String.format(
                "INSERT INTO "+tableName+"(changeType, count) VALUES (\'%s\',%s)", op, numChange
        );
        Statement stmt = monitorConnection.createStatement();
        stmt.execute(q);
        stmt.close();
    }

    private void recordReceived(String op, int numChange) throws SQLException{
        record(op, numChange, "receiveCount");
    }

    private void recordApplied(String op, int numChange) throws SQLException{
        record(op, numChange, "appliedCount");
    }

    public void run() {

        try {

            if (ClientConfig.doMonitor) {
                String[] createIfNotExists = {
                        "CREATE TABLE IF NOT EXISTS receiveCount(changeID BIGINT AUTO_INCREMENT PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT NOW())",
                        "CREATE TABLE IF NOT EXISTS appliedCount(changeID BIGINT AUTO_INCREMENT PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT NOW())",
                };
                Statement stmtTmp = monitorConnection.createStatement();
                for (String q : createIfNotExists) {
                    stmtTmp.execute(q);
                }
            }

//            if (ClientConfig.doMonitor) {
//            SystemMonitor systemMonitor = new SystemMonitor(monitorConnection);
//            systemMonitor.run();
//            }

            consumer.subscribe(Arrays.asList(currentTable));

            System.out.println("Subscribed to topic " + currentTable);


            while (!closed.get()) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    for (ConsumerRecord<String, byte[]> record : records) {

                        System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n",
                                record.partition(), record.offset(), record.key(), record.value());

                        IncrementMessageProtos.IncrementMessage message =
                                IncrementMessageProtos.IncrementMessage.parseFrom(record.value());

                        String query;

                        if (message.getType() == 0) {

                            if (ClientConfig.doMonitor) {
                                recordReceived("INSERT", message.getInsertUpdateContentsCount());
                            }

                            StringBuilder fieldsStrBuilder = new StringBuilder();
                            StringBuilder valuesStrBuilder = new StringBuilder();

                            for (String field : message.getFieldsList()) {
                                fieldsStrBuilder.append(field);
                                fieldsStrBuilder.append(",");
                            }
                            fieldsStrBuilder.setLength(fieldsStrBuilder.length() - 1);
                            String fieldStr = fieldsStrBuilder.toString();

                            for (IncrementMessageProtos.InsertUpdateContent content : message.getInsertUpdateContentsList()) {
                                valuesStrBuilder.append("(");
                                for (String value : content.getValuesList()) {
                                    if (value.isEmpty()) {
                                        valuesStrBuilder.append("NULL");
                                    }
                                    else {
                                        valuesStrBuilder.append(value);
                                    }
                                    valuesStrBuilder.append(",");

                                }
                                valuesStrBuilder.setLength(valuesStrBuilder.length() - 1);
                                valuesStrBuilder.append("),");
                            }
                            valuesStrBuilder.setLength(valuesStrBuilder.length() - 1);
                            query = "INSERT INTO " + currentTable + " (" + fieldStr + ") VALUES " +
                                    valuesStrBuilder.toString() + ";";
                            System.out.println(query);
                            Statement stmt = con.createStatement();
                            stmt.execute(query);

                            if (ClientConfig.doMonitor){
                                recordApplied("INSERT", message.getInsertUpdateContentsCount());
                            }




                        } else if (message.getType() == 1) {

                            if (ClientConfig.doMonitor) {
                                recordReceived("UPDATE", message.getInsertUpdateContentsCount());
                            }

                            List<String> fields = message.getFieldsList();

                            for (IncrementMessageProtos.InsertUpdateContent content: message.getInsertUpdateContentsList()) {
                                StringBuilder updateStrBuilder = new StringBuilder();
                                List<String> values = content.getValuesList();
                                String id = "";
                                for (int i = 0; i < values.size(); i++) {
                                    if (fields.get(i).equals("ID")) {
                                        id = values.get(i);
                                    }
                                    else if (!values.get(i).isEmpty()){
                                        updateStrBuilder.append(fields.get(i));
                                        updateStrBuilder.append("=");
                                        updateStrBuilder.append(values.get(i));
                                        updateStrBuilder.append(",");
                                    }
                                }
                                updateStrBuilder.setLength(updateStrBuilder.length() - 1);
                                query = "UPDATE " + currentTable + " SET " + updateStrBuilder.toString() + " WHERE ID = " +
                                        id + ";";
                                System.out.println(query);
                                Statement stmt = con.createStatement();
                                stmt.execute(query);

                                if (ClientConfig.doMonitor) {
                                    recordApplied("UPDATE", message.getInsertUpdateContentsCount());
                                }
                            }


                        } else if (message.getType() == 2) {
                            if (ClientConfig.doMonitor) {
                                recordReceived("DELETE", message.getDeleteIdsCount());
                            }

                            StringBuilder idStrBuilder = new StringBuilder();

                            for (Long id: message.getDeleteIdsList()) {
                                idStrBuilder.append(Long.toString(id));
                                idStrBuilder.append(",");
                            }

                            idStrBuilder.setLength(idStrBuilder.length() - 1);

                            query = "DELETE FROM " + currentTable + " WHERE id IN (" + idStrBuilder.toString() + ");";

                            System.out.println(query);
                            Statement stmt = con.createStatement();
                            stmt.execute(query);

                            if (ClientConfig.doMonitor) {
                                recordApplied("DELETE", message.getDeleteIdsCount());
                            }

                        } else {
                            throw new RuntimeException("Unknown operation");
                        }

                    }
                }
                catch (SQLException | RuntimeException | InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        }
        catch (WakeupException e) {
            if (!closed.get()) throw e;
        }
        catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            consumer.close();
        }

    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) {
        Thread thread = new Thread(new DatabaseClient(ClientConfig.tableName));
        thread.start();
    }
}
