package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private KafkaConsumer<String,String> consumer;
    private CountDownLatch latch;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

    public ConsumerThread(CountDownLatch latch,String topic,String bootStrapServers,String groupIp){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupIp);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");

        this.latch = latch;
        consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
      try{
          while (true){
              logger.info("In while");
              ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));
              for(ConsumerRecord<String,String> record : records){
                  logger.info("Key "+record.key() +" value "+record.value());
                  logger.info("Partition "+record.partition() +" Offset: "+ record.offset());
              }
          }
      }catch (WakeupException e){
          logger.info("Received Shutdown Signal");
      }finally {
          consumer.close();
          // tell our main code we are done with consumer
          latch.countDown();
      }
    }

    public void shutdown(){
        // the wakeup method is a special method to interrupt consumer.poll();
        // it will throw the exception WakeUpException
        consumer.wakeup();
    }

}
