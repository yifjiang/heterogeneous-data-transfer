package com.datayes.heterDataTransfer.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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

public class DatabaseClient implements Runnable {

    private Connection con;
    private String currentTable;

    DatabaseClient() {
        try {
            con = DriverManager.getConnection(ClientConfig.sqlConnectionUrl);
        }
        catch (SQLException e) {
            System.out.println("Database connection failed");
        }
        currentTable = ClientConfig.tableName;
    }

    public void run() {


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ClientConfig.kafkaProps);

        consumer.subscribe(Arrays.asList(ClientConfig.topicName));

        System.out.println("Subscribed to topic " + ClientConfig.topicName);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n",
                        record.partition(), record.offset(), record.key(), record.value());

                try {

                    Map<String, String> changeContent = decode(record.value());
                    final String query;


                    if (changeContent.get("OPERATION").equals("INSERT")) {


                        StringBuilder fieldsStr = new StringBuilder();
                        StringBuilder valuesStr = new StringBuilder();

                        for (Map.Entry<String, String> entry : changeContent.entrySet()) {
                            String field = entry.getKey();
                            if (field.equals("OPERATION") || field.equals("TMSTAMP")) continue;
                            fieldsStr.append(field);
                            fieldsStr.append(",");
                            valuesStr.append(entry.getValue());
                            valuesStr.append(",");
                        }

                        fieldsStr.setLength(fieldsStr.length() - 1);
                        valuesStr.setLength(valuesStr.length() - 1);

                        query = "INSERT INTO " + currentTable + " (" + fieldsStr.toString() +
                                ") VALUES (" + valuesStr.toString() + ");";

                    }
                    else if (changeContent.get("OPERATION").equals("UPDATE")) {

                        StringBuilder updateStr = new StringBuilder();

                        for (Map.Entry<String, String> entry : changeContent.entrySet()) {
                            String field = entry.getKey();
                            if (field.equals("OPERATION") || field.equals("TMSTAMP") || field.equals("ID")) continue;
                            updateStr.append(field);
                            updateStr.append("=");
                            updateStr.append(entry.getValue());
                            updateStr.append(",");
                        }

                        updateStr.setLength(updateStr.length() - 1);

                        query = "UPDATE "+ currentTable + " SET " + updateStr.toString() + " WHERE ID = " +
                                changeContent.get("ID") + ";";

                    }
                    else if (changeContent.get("OPERATION").equals("DELETE")) {

                        query = "DELETE FROM " + currentTable + " WHERE id = " + changeContent.get("ID") + ";";

                    }
                    else {
                        throw new RuntimeException("Unknown operation");
                    }

                    System.out.println(query);
                    Statement stmt = con.createStatement();
                    stmt.execute(query);


                }
                catch (Exception e) {
                    System.out.println(e.getMessage());
                }

            }

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

    public static void main(String[] args) {
        new DatabaseClient().run();
    }
}
