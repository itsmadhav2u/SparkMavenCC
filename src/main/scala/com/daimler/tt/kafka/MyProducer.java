package com.daimler.tt.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {

        /* 1. Setting Properties
        1. ClientId/ProducerID  2. Bootstrap server list    3. Key Serializer   4. Value Serializer
        */
        Properties producerPros = new Properties();
        producerPros.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.producerID);
        producerPros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServerList);
        producerPros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerPros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //2. Creating Kafka Producer object
        KafkaProducer<Integer, String> myProducer = new KafkaProducer<Integer, String>(producerPros);

        //3. Sending messages to kafka by send method on this producer object
        for(int i=1; i<=17; i++){

            //For ProducerRecord - topicName and message value is mandatory
            //Optional - message key (optional but imp we give, it helps in grouping, join etc), target partition, timestamp
            myProducer.send(new ProducerRecord<Integer, String>(KafkaConfig.topicName, i, "My Message " + i));

        }

        /*
         -> Every Record goes through Serialization, partitioning and then kept in buffer this is done by send()
                For each partition we will have different buffers to store data in packets by default buffer size is 32 mb (configurable)
                    It helps in network optimization
                For partitioning we have 2 options
                1. Default system partitioner (hash key if key available / round robin if no key available)
                2. Custom partitioner class

         -> We have 2 types if timestamps for evey record
            1. (default) create time or event time (time at which message is produced)
            2. log append time (time when broker receives message)
            If we want to change we can use below parameter
            message.timestamp.time = 0 (default - create time)
            message.timestamp.time = 1 (log append time)

          -> After Serialization, partitioning and buffer in send() an 'IO Thread' will take the data and send to broker
                and then broker will send an acknowledgement to 'Io Thread'
         */

        //4. Close the producer object
        myProducer.close();

    }
}
