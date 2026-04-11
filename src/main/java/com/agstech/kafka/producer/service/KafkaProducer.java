package com.agstech.kafka.producer.service;

public interface KafkaProducer {
    void sendMessage(String message);
}
