package com.kspendli.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class ReportKafkaListener {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ReportService reportService;

    private ObjectMapper mapper=new ObjectMapper();

    @Value("${kafka.output-topic}")
    private String outputTopic;

    public ReportKafkaListener(KafkaTemplate<String, Object> kafkaTemplate, ReportService reportService) {
        this.kafkaTemplate = kafkaTemplate;
        this.reportService = reportService;
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        log.info("Received the message");
        var message= (String) record.value();
        if (isValidCountry(message)) {
            log.info("Valid Message");
            reportService.processReport(message);

            // Generic publish
            kafkaTemplate.send(outputTopic, message);
        }

        ack.acknowledge();
    }

    @KafkaListener(topics = "${kafka.output-topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void consumer(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        log.info("Received the CN country message");

        ack.acknowledge();
    }

    private boolean isValidCountry(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            return node.has("country")
                    && "CN".equalsIgnoreCase(node.get("country").asText());
        } catch (Exception e) {
            log.warn("Invalid JSON: {}", json);
            return false;
        }
    }

}
