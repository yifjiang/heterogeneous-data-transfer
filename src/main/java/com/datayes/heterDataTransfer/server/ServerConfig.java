package com.datayes.heterDataTransfer.server;

import java.util.Properties;

public class ServerConfig {

    public static final String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;" +
            "databaseName=testMSSQL;" +
            "user=sa;password=Vm450Group7;";

    public static final String tableName = "test";

    public static final String topicName = "Changes";

    public static final Properties kafkaProps = new Properties();

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "test");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "1000");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }
}
