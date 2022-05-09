package com.daimler.tt.kafka;

public class KafkaConfig {

    final static String producerID = "producer-1";
    final static String consumerID = "consumer-1";

    //Giving 2 servers for fault tolerance
    final static String bootstrapServerList = "localhost:9092,localhost:9093";

    final static String topicName = "myTopic17";

}
