package com.namankhurpia.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithGroups {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithGroups.class.getName());

        String bootstrapserver = "127.0.0.1:9092";
        String groupid = "my-first-group";

        //creating properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG , groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");  //latest, earliest,

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscibe to our consumer topic
        consumer.subscribe(Arrays.asList("first-topic"));

        //poll for new data
        while (true)
        {
            ConsumerRecords<String,String> record = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> cr:record)
            {
                logger.info("key:"+cr.key() +"    value:"+cr.value()+ "    partition:"+cr.partition() + "    offset:" +cr.offset());

            }
        }


    }
}
