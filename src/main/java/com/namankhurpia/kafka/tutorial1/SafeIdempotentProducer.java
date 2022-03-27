package com.namankhurpia.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SafeIdempotentProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //same keys goes to same partitions
        //if you dont set a key it will go in round robin fashion to any partition

        Logger logger = LoggerFactory.getLogger(SafeIdempotentProducer.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //creating safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //kafka greater than 2.0 so keep 5 otherwise keep 1


        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(properties);

        for(int i=0;i<10;i++)
        {

            //create producer record
            String topic = "first-topic";
            String value = "value:"+i;
            String key = "key:"+i;
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key,value);
            logger.info("key:"+key);

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
            }).get(); //blocks the send to make it synchronous


        }

        //flush data
        kafkaProducer.flush();

        //close and flush data
        kafkaProducer.close();
        System.out.println("done");

    }

}
