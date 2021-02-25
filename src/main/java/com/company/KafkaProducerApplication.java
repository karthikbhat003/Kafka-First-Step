package com.company;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerApplication {
    public static void main(String[] args) {

        String bootstrapServers="127.0.0.1:5181";

        //Get the properties from jdk
        Properties properties=new Properties();

        // store the key value pairs in property
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        // Address of the kafka server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //Serialise the key to be sent to kafka broker
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Serialise the value to be sent to kafka broker
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        // produce the kafka producer with properties
        KafkaProducer<String,String> first_producer=new KafkaProducer<String, String>(properties);


        // Create a producer record for kafka
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("My_first","Hi Kafka");

        first_producer.send(record);
        first_producer.flush();
        first_producer.close();
    }
}
