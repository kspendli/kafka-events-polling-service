package com.kspendli.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportKafkaListener {

    private final ReportService reportService;

    public ReportKafkaListener(ReportService reportService) {
        this.reportService = reportService;
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Object event, Acknowledgment ack) {
        log.info("Received the message");
        reportService.processReport(event);

        ack.acknowledge();
    }
}
