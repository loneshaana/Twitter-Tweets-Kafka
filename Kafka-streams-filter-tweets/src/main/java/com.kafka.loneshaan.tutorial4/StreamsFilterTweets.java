package com.kafka.loneshaan.tutorial4;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static Integer extractUserFollowers(String tweetJson) {
        JsonObject jsonObject = (JsonObject) JsonParser.parseString(tweetJson);
        try {
            return jsonObject.get("user").getAsJsonObject().get("followers_count").getAsInt();
        } catch (NullPointerException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        try {
            KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {
                // filter for tweets which has user of over 10000 followers
                return extractUserFollowers(jsonTweet) > 10000;
            });

            filteredStream.to("important_tweets");
        } catch (NullPointerException e) {

        }

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build() , properties);
        // start streams application
        kafkaStreams.start();
    }
}
