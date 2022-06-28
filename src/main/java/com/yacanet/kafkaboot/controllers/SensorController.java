package com.yacanet.kafkaboot.controllers;


import com.yacanet.kafkaboot.services.PayloadResponse;
import com.yacanet.kafkaboot.models.sensor.SensorRequestModel;
import com.yacanet.kafkaboot.services.KafkaConfig;
import com.yacanet.kafkaboot.services.KafkaSensorConsumer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    
    @Autowired
    private KafkaConfig configKafka;
    
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public String index()
    {        
        return "akan mengirim ke kafka producer";
    }
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PayloadResponse> store(@RequestBody SensorRequestModel sensor)
    { 
        KafkaProducer<String, String> producer = new KafkaProducer<>(configKafka.getPropProducers());
        
        ProducerRecord<String, String> record = new ProducerRecord<>("sensor-" + sensor.getSensorId(), sensor.getData());
        producer.send(record);
        
        PayloadResponse response = new PayloadResponse();
        response.setStatus("000");
        response.setMessage("DATA SENSOR: " + sensor.getData() + " BERHASIL DISIMPAN PADA TOPIC: sensor-" + sensor.getSensorId());
        
        producer.close();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
    @GetMapping(value="/statuslatest", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PayloadResponse> latest()
    {
        configKafka.setAutoOffsetReset("latest");
        PayloadResponse response = new PayloadResponse();
        Properties prop = configKafka.getPropConsumers();
        
        KafkaSensorConsumer sensor = new KafkaSensorConsumer();
        response.setStatus("000");
        response.setMessage("status latest");
        response.setData(sensor.latestMessage("1", prop));  

        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
