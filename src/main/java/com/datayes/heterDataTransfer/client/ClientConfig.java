package com.datayes.heterDataTransfer.client;

import java.util.Properties;

class ClientConfig {


    static final String sqlConnectionUrl = "jdbc:mysql://localhost/testMSSQL?" +
            "user=root&password=Vm450Group7&serverTimezone=UTC";

    static final String monitorDBURL = "jdbc:mysql://localhost/testMonitor?" +
            "user=root&password=Vm450Group7&serverTimezone=UTC";

    static final Boolean doMonitor = true;

    static final Properties kafkaProps = new Properties();

    static {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("group.id", "test");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "1000");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }


    /* ********************************test configuration******************************** */

    /*static final String sqlConnectionUrl = "jdbc:mysql://localhost/testMSSQL?" +
            "user=root&password=admin&serverTimezone=UTC";

    static final String monitorDBURL = "jdbc:mysql://localhost/testMonitor?" +
            "user=root&password=admin&serverTimezone=UTC";

    static final Boolean doMonitor = true;

    static final Properties kafkaProps = new Properties();

    static {
        kafkaProps.put("bootstrap.servers", "106.75.231.251:9092");
        kafkaProps.put("group.id", "test");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "1000");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }*/

}
