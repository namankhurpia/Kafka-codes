package com.namankhurpia.kafka.twitterAppWithKafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.HosebirdMessageProcessor;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String apikey = "wrufKr3va3mGapKgkCOu71ofd";
    String apikeysecet = "RvJd56NZc25MPCUwvYt4gWT8gQELGWJ5a2EoqYCmaVlL593MYH";
    //String bearertoken = "AAAAAAAAAAAAAAAAAAAAACsDZwEAAAAApiGmt90nlqlt2PijSwzyFc8qJZE%3DQDB1rIu4gpW6wBmAkxAeQuHpoWL4C2svbmwOogZBCqoTir81xV";
    String accesstoken = "924379622816518145-yLszpFO9P9DIobtzSHeDGZL4efGNkV1";
    String accesstokensecret = "YHSSTf2Al2RuafotVLrP8Q06TaOy8i5U6NOIctB2EMrqw";

    String clientId = "X0tNMjU3WjBMLXdENmFrQk9lQlk6MTpjaQ";
    String clientsecret = "7gjPSSQbQ1tlDU385pPEPLCuAwL_vOTfy7t4WpEp2UqKGY8-0b";

    public static void main(String[] args) {
        TwitterProducer obj = new TwitterProducer();
        obj.run();
    }

    public void run()
    {
        logger.info("setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //create twitter client
        Client client = CreateTwitterClient(msgQueue);
        client.connect();

        //create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        // add a shutdown condition
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Closing app...");
            client.stop();
            producer.close();
        }));

        //loop to send tweets to kafka

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg !=null)
            {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null)
                        {
                            logger.info("something bad happened",e);
                        }
                    }
                });
            }


        }
        logger.info("End of app");

    }

    public Client CreateTwitterClient(BlockingQueue<String> msgQueue)
    {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("russia");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(apikey,apikeysecet,accesstoken,accesstokensecret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-MyClient-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;

    }

    public KafkaProducer createKafkaProducer()
    {
        String bootstrapserver = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);
        return kafkaProducer;
    }
}
