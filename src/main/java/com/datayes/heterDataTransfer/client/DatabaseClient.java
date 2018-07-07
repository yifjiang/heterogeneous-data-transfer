package com.datayes.heterDataTransfer.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatabaseClient implements Runnable {

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ClientConfig.kafkaProps);
    private Connection con;
    private Connection monitorConnection;

    DatabaseClient() {
        try {
            con = DriverManager.getConnection(ClientConfig.sqlConnectionUrl);
            monitorConnection = DriverManager.getConnection(ClientConfig.monitorDBURL);
        }
        catch (SQLException e) {
            System.out.println("Database connection failed");
        }
    }

    private int numChangeRows(Map<String, String> changeContent){
        return 1;//TODO: implement this according to number of rows changed.
    }

    void record(Map<String, String> changeContent, String tableName) throws SQLException{
        int numChange = numChangeRows(changeContent);
        String q = String.format(
                "INSERT INTO "+tableName+"(changeType, count) VALUES (\'%s\',%s)",
                changeContent.get("OPERATION"),
                Integer.toString(numChange)
        );
        Statement stmt = monitorConnection.createStatement();
        stmt.execute(q);
        stmt.close();
    }

    void recordReceived(Map<String, String> changeContent) throws SQLException{
        record(changeContent, "receiveCount");
    }

    void recordApplied(Map<String, String> changeContent) throws SQLException{
        record(changeContent, "appliedCount");
    }

    public void run() {

        try {

            String[] createIfNotExists = {
                    "CREATE TABLE IF NOT EXISTS receiveCount(changeID BIGINT AUTO_INCREMENT PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT NOW())",
                    "CREATE TABLE IF NOT EXISTS appliedCount(changeID BIGINT AUTO_INCREMENT PRIMARY KEY, changeType CHAR(10), count INT, dateAndTime DATETIME DEFAULT NOW())",
            };
            Statement stmtTmp = monitorConnection.createStatement();
            for (String q: createIfNotExists) {
                stmtTmp.execute(q);
            }

//            SystemMonitor systemMonitor = new SystemMonitor(monitorConnection);
//            systemMonitor.run();

            consumer.subscribe(Arrays.asList(ClientConfig.topicName));

            System.out.println("Subscribed to topic " + ClientConfig.topicName);

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {

                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n",
                            record.partition(), record.offset(), record.key(), record.value());


                    Map<String, String> changeContent = decode(record.value());
                    recordReceived(changeContent);
                    final String query;


                    if (changeContent.get("OPERATION").equals("INSERT")) {


                        StringBuilder fieldsStr = new StringBuilder();
                        StringBuilder valuesStr = new StringBuilder();

                        for (Map.Entry<String, String> entry : changeContent.entrySet()) {
                            String field = entry.getKey();
                            if (field.equals("OPERATION")) continue;
                            fieldsStr.append(field);
                            fieldsStr.append(",");
                            valuesStr.append(entry.getValue());
                            valuesStr.append(",");
                        }

                        fieldsStr.setLength(fieldsStr.length() - 1);
                        valuesStr.setLength(valuesStr.length() - 1);

                        query = "INSERT INTO " + ClientConfig.tableName + " (" + fieldsStr.toString() +
                                ") VALUES (" + valuesStr.toString() + ");";

                    }
                    else if (changeContent.get("OPERATION").equals("UPDATE")) {

                        StringBuilder updateStr = new StringBuilder();

                        for (Map.Entry<String, String> entry : changeContent.entrySet()) {
                            String field = entry.getKey();
                            if (field.equals("OPERATION") || field.equals("ID")) continue;
                            updateStr.append(field);
                            updateStr.append("=");
                            updateStr.append(entry.getValue());
                            updateStr.append(",");
                        }

                        updateStr.setLength(updateStr.length() - 1);

                        query = "UPDATE "+ ClientConfig.tableName + " SET " + updateStr.toString() + " WHERE ID = " +
                                changeContent.get("ID") + ";";

                    }
                    else if (changeContent.get("OPERATION").equals("DELETE")) {

                        query = "DELETE FROM " + ClientConfig.tableName + " WHERE id = " + changeContent.get("ID") + ";";

                    }
                    else {
                        throw new RuntimeException("Unknown operation");
                    }

                    System.out.println(query);
                    Statement stmt = con.createStatement();
                    stmt.execute(query);
                    recordApplied(changeContent);
                }
            }
        }
        catch (WakeupException e) {
            if (!closed.get()) throw e;
        }
        catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            consumer.close();
        }

    }

    private Map<String, String> decode(String message) {

        Map<String, String> ret = new HashMap<>();

        try {
            Properties prop = new Properties();
            prop.load(new StringReader(message.substring(1, message.length() - 1).replace(", ", "\n")));

            for (Map.Entry<Object, Object> e : prop.entrySet()) {
                ret.put((String) e.getKey(), (String) e.getValue());
            }

        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return ret;

    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public static void main(String[] args) {
        new DatabaseClient().run();
    }
}
