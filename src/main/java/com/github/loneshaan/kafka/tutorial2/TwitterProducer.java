package com.github.loneshaan.kafka.tutorial2;

import com.github.loneshaan.kafka.tutorial1.ProducerDemo;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey = null;
    private String consumerSecret = null;
    private String token = null;
    private String secret = null;

    public TwitterProducer() {
        try (InputStream input = new FileInputStream("src/main/resources/application.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            consumerKey = prop.getProperty("twitter.consumer_key");
            consumerSecret = prop.getProperty("twitter.consumer_secret");
            token = prop.getProperty("twitter.token");
            secret = prop.getProperty("twitter.secret");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Error while Reading Twitter Properties");
        }
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create kafka producer
        KafkaProducer<String, String> producer = this.createKafkaProducer();

        // Add the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application....");
            client.stop();
            logger.info("Closing Producer");
            producer.close();
            logger.info("Done");
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Something bad happened ", e);
                    }
                });
            }
        }
        logger.info("End Of Application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the host u want to connect to
        Hosts host = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint filterEndpoint = new StatusesFilterEndpoint();

        //List<Long> followings = Lists.newArrayList(1234L , 566788L);
        //filterEndpoint.followings(followings);

        List<String> terms = Lists.newArrayList("Coronavirus");
        filterEndpoint.trackTerms(terms);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder clientBuilder = new ClientBuilder()
                .name("TwitterLearning-Client") // Optional Mainly For Logs
                .hosts(host)
                .authentication(auth)
                .endpoint(filterEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return clientBuilder.build();
    }

    public static void main(String[] args) {
        // create twitter clients
        new TwitterProducer().run();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); //
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG , Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION , "5");

        // Create Producer
        return new KafkaProducer<>(properties);
    }
}
