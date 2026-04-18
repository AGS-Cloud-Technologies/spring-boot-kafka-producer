package com.agstech.kafka.producer.service;

/**
 * Custom exception for Kafka Producer operations
 */
public class KafkaProducerException extends RuntimeException {

    public KafkaProducerException(String message) {
        super(message);
    }

    public KafkaProducerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaProducerException(Throwable cause) {
        super(cause);
    }
}

