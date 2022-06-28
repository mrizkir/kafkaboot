package com.yacanet.kafkaboot.services;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaSensorConsumer {
    Logger logger = LoggerFactory.getLogger(KafkaSensorConsumer.class);   
    /**
     * digunakan untuk mendapatka message terakhir
     * @param sensorId
     * @param prop
     * @return void
     */
    public String latestMessage(String sensorId, Properties prop)
    {  
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList("sensor-" + sensorId));
        
        boolean flag = true;
        String msg = "";
        while (flag)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("is empty: " + records.isEmpty());
            logger.info("jumlah record: " + records.count());
            if (records.isEmpty())
            {
                for(ConsumerRecord<String, String> record: records)
                {
                    msg = record.value();
                    logger.info("latest message from topic: " + msg);
                    long offset = record.offset();
                    logger.info("offset value is: " + offset);
                    if (offset > 0)
                    {
                        flag = false;
                        consumer.close();
                        break;
                    }
                }   
            }
            else
            {
                flag = false;
                consumer.close(); 
                break;
            }            
        }         
        return msg;
    }
}
