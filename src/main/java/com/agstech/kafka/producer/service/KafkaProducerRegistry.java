package com.agstech.kafka.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for managing multiple typed Kafka producers
 * Allows for production-ready management of different message types
 */
@Component
public class KafkaProducerRegistry {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerRegistry.class);

    private final Map<String, KafkaProducer<?>> producers = new HashMap<>();
    private final KafkaProducerFactory producerFactory;

    public KafkaProducerRegistry(KafkaProducerFactory producerFactory) {
        this.producerFactory = producerFactory;
    }

    /**
     * Register a typed producer for a specific type
     * @param producerKey The unique key for this producer
     * @param kafkaTemplate The KafkaTemplate for the specific type
     * @param defaultTopic The default topic for this producer
     * @param <T> The message type
     */
    public <T> void registerProducer(String producerKey, KafkaTemplate<String, T> kafkaTemplate, String defaultTopic) {
        KafkaProducer<T> producer = producerFactory.createProducer(kafkaTemplate, defaultTopic);
        producers.put(producerKey, producer);
        log.info("Registered Kafka producer with key: {}", producerKey);
    }

    /**
     * Get a registered producer by key
     * @param producerKey The producer key
     * @param <T> The message type
     * @return The KafkaProducer instance
     * @throws IllegalArgumentException if producer not found
     */
    @SuppressWarnings("unchecked")
    public <T> KafkaProducer<T> getProducer(String producerKey) {
        KafkaProducer<?> producer = producers.get(producerKey);
        if (producer == null) {
            throw new IllegalArgumentException("Producer not found for key: " + producerKey);
        }
        return (KafkaProducer<T>) producer;
    }

    /**
     * Check if a producer is registered
     * @param producerKey The producer key
     * @return true if registered, false otherwise
     */
    public boolean hasProducer(String producerKey) {
        return producers.containsKey(producerKey);
    }
}



