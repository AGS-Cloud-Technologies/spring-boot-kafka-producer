package com.agstech.kafka.producer.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Factory for creating generic Kafka Producer instances
 * Supports creating producers for any type of message payload
 */
@Component
public class KafkaProducerFactory {

    /**
     * Create a typed Kafka producer
     * @param kafkaTemplate The KafkaTemplate for the specific type
     * @param defaultTopic The default topic name
     * @param <T> The message type
     * @return A configured KafkaProducer instance
     */
    public <T> KafkaProducer<T> createProducer(KafkaTemplate<String, T> kafkaTemplate, String defaultTopic) {
        return new KafkaProducerImpl<>(kafkaTemplate, defaultTopic);
    }
}

