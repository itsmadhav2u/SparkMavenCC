package com.daimler.tt.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

/*
Sample messages we are handling in this consumer logic
1,2013-07-25 00:00:00.0,12111,COMPLETE
2,2013-07-25 00:00:00.0,8827,CLOSED
*/

public class MyConsumer {
    public static void main(String[] args) {

        //Creating a consumer object
        Properties consumerPros = new Properties();
        //Mandatory
        consumerPros.put(ConsumerConfig.CLIENT_ID_CONFIG, KafkaConfig.consumerID);
        consumerPros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServerList);
        consumerPros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerPros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Optional
        consumerPros.put(ConsumerConfig.GROUP_ID_CONFIG, "CONSUMER_GROUP1");
        consumerPros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //default is latest

        KafkaConsumer<Integer, String> myConsumer = new KafkaConsumer<Integer, String>(consumerPros);

        //Subscribing to a kafka topic
        myConsumer.subscribe(Arrays.asList("all_orders"));

        //Creating a producer object
        Properties producerPros = new Properties();
        producerPros.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.producerID);
        producerPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServerList);
        producerPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> myProducer = new KafkaProducer<Integer, String>(producerPros);


        while (true){

            ConsumerRecords<Integer, String> records = myConsumer.poll(100); //consume data for every 100 milli seconds

            for( ConsumerRecord<Integer, String> record : records){
                if (record.value().split(",")[3].equals("COMPLETE")){
                    myProducer.send(new ProducerRecord<Integer, String>("closed_orders", record.key(), record.value()));
                }
                else{
                    myProducer.send(new ProducerRecord<Integer, String>("completed_orders", record.key(), record.value()));
                }
            }
        }

    }
}
