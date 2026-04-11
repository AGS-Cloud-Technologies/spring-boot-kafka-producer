package com.agstech.kafka.producer.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "my-topic" })
class KafkaProducerImplTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Test
    void sendMessage() {
        // given
        String message = "Hello, Kafka!";

        // when & then
        assertDoesNotThrow(() -> kafkaProducer.sendMessage(message));
    }
}
