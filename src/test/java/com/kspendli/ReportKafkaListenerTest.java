package com.kspendli;

import com.kspendli.service.ReportService;
import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;


@SpringBootTest(
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
)
@EmbeddedKafka(partitions = 1, topics = "report-topic")
class ReportKafkaListenerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @PostConstruct
    void logBroker() {
        System.out.println("Embedded Kafka running at: " + embeddedKafka.getBrokersAsString());
    }

    @Test
    void shouldConsumeJsonMessageSuccessfully() throws InterruptedException {

        kafkaTemplate.send("test-topic", "{\"country\":\"CN\"}");

        Thread.sleep(30000);
    }
}
