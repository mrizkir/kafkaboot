package com.yacanet.kafkaboot.controllers;


import com.yacanet.kafkaboot.models.StringResponse;
import com.yacanet.kafkaboot.models.sensor.SensorRequestModel;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/sensor")
public class SensorController {
    Logger logger = LoggerFactory.getLogger(SensorController.class);
    
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public String index()
    {        
        return "akan mengirim ke kafka producer";
    }
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StringResponse> store(@RequestBody SensorRequestModel sensor)
    {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.yacanet.com:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());        
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); 
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        
        logger.info("sensor nomor " + sensor.getId());
        
        ProducerRecord<String, String> record = new ProducerRecord<>("sensor-" + sensor.getId(), sensor.getData());
        producer.send(record);
        
        StringResponse response = new StringResponse();
        response.setStatus("000");
        response.setMessage("DATA SENSOR: " + sensor.getData() + " BERHASIL DISIMPAN PADA TOPIC: sensor-" + sensor.getId());
        
        producer.close();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
