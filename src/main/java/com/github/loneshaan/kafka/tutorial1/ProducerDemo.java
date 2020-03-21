package com.github.loneshaan.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // create a producerRecord
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first-topic" , "Hello World");

        // Then Send Data - async
        kafkaProducer.send(producerRecord);

        // flush
        kafkaProducer.flush();

        // close
        kafkaProducer.close();
    }
}
