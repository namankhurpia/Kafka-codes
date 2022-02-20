package com.namankhurpia.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

        String bootstrapserver = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);

        //create producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first-topic","VALUE FROM JAVA");


        //send data - asynchronous
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null)
                {
                    logger.info("Received new metadata:"+"topic:" +recordMetadata.topic()
                            +"offset:"+ recordMetadata.hasOffset()
                            +"timestamp:" +recordMetadata.hasTimestamp()
                            +"partition:"+ recordMetadata.partition());
                }
                else
                {
                    logger.error("ERROR",e);
                }
            }
        });

        //flush data
        kafkaProducer.flush();

        //close and flush data
        kafkaProducer.close();
        System.out.println("done");

    }
}
