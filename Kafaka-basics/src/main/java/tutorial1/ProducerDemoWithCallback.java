package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
        public static void main(String[] args) {
            final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // every time gets executed when data is send || exception is thrown
                    if(e == null){
                        logger.info("Received New Meta Data \n"
                        + "Topic "+recordMetadata.topic()+" \n"
                        + "Partition "+recordMetadata.partition()+" \n"
                        + "Timestamp "+recordMetadata.timestamp());
                    }else{
                        e.printStackTrace();
                        logger.error("Error while producing to producer ",e);
                    }
                }
            });

            // flush
            kafkaProducer.flush();

            // close
            kafkaProducer.close();
        }
    }

