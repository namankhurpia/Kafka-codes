package com.namankhurpia.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithGroupsAssignAndSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGroupsAssignAndSeek.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        String groupid = "my-first-group";
        String topic = "first-topic";

        //creating properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupid);  -- no groupids
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  //latest, earliest,

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscibe to our consumer topic
        //consumer.subscribe(Arrays.asList("first-topic"));  - do not subscribe

        //assign and seek mostly used to replay data or fetch a specific message

        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0); //we have set out topic and partition
        long offsetToReadFrom = 10l; // we started from offset 10
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numberOfMesagestoRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //poll for new data
        while (keepOnReading)
        {
            ConsumerRecords<String,String> record = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> cr:record)
            {
                logger.info("key:"+cr.key() +"    value:"+cr.value()+ "    partition:"+cr.partition() + "    offset:" +cr.offset());
                numberOfMessagesReadSoFar +=1;
                if(numberOfMessagesReadSoFar>=numberOfMesagestoRead)
                {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("DONE");

    }
}
