package com.github.loneshaan.kafka.tutorial3;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static String hostname = null;
    private static String username = null;
    private static String password = null;

    public ElasticSearchConsumer() {
        try {
            Properties prop = new Properties();
            InputStream input = new FileInputStream("KafkaConsumerElasticSearch/src/main/resources/application.properties");
            prop.load(input);
            hostname = prop.getProperty("elasticsearch.hostname");
            username = prop.getProperty("elasticsearch.username");
            password = prop.getProperty("elasticsearch.password");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static RestHighLevelClient createClient() {
        // don't do if u run your local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")
        ).setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootStrapServers = "localhost:9092";
        String groupIp = "kafka_demo_elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIp);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        //"earliest/latest/none"
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private static String extractIdFromTweet(String tweetJson) {
        JsonObject jsonObject = (JsonObject) JsonParser.parseString(tweetJson);
        return jsonObject.get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException {
        new ElasticSearchConsumer();
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

        // poll the new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String, String> record : records) {
                // Kafka Generic Id
                // String id = record.topic()+"_"+record.partition()+"_"+record.offset(); //  this is when u will not find any possible id

                // twitter feed specific id
                try {
                    String tweetId = extractIdFromTweet(record.value()); // Used to make it idempotent
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", tweetId).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.error("Skipping the bad data " + record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                consumer.commitSync();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
//        client.close();
    }
}
