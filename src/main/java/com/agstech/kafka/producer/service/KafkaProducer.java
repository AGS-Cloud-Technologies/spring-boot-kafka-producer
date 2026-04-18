package com.agstech.kafka.producer.service;

import java.util.concurrent.CompletableFuture;

/**
 * Generic Kafka Producer interface
 * @param <T> The type of message payload
 */
public interface KafkaProducer<T> {

    /**
     * Send a message to Kafka topic
     * @param topic The topic name
     * @param message The message payload
     */
    void sendMessage(String topic, T message);

    /**
     * Send a message to Kafka topic with async callback
     * @param topic The topic name
     * @param message The message payload
     * @return CompletableFuture for async processing
     */
    CompletableFuture<Void> sendMessageAsync(String topic, T message);

    /**
     * Send a message to Kafka topic with a key
     * @param topic The topic name
     * @param key The partition key
     * @param message The message payload
     */
    void sendMessage(String topic, String key, T message);

    /**
     * Send a message to Kafka topic with a key and async callback
     * @param topic The topic name
     * @param key The partition key
     * @param message The message payload
     * @return CompletableFuture for async processing
     */
    CompletableFuture<Void> sendMessageAsync(String topic, String key, T message);
}


