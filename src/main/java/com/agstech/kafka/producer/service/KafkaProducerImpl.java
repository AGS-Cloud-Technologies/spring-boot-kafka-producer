package com.agstech.kafka.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * Generic Kafka Producer Implementation
 * Handles any type of message payload with production-ready features
 * @param <T> The type of message payload
 */
public class KafkaProducerImpl<T> implements KafkaProducer<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerImpl.class);

    private final KafkaTemplate<String, T> kafkaTemplate;
    private final String defaultTopic;

    public KafkaProducerImpl(KafkaTemplate<String, T> kafkaTemplate, String defaultTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.defaultTopic = defaultTopic;
    }

    @Override
    public void sendMessage(String topic, T message) {
        try {
            log.debug("Sending message to topic: {}", topic);
            kafkaTemplate.send(topic, message);
            log.info("Message sent successfully to topic: {}", topic);
        } catch (Exception e) {
            log.error("Failed to send message to topic: {}", topic, e);
            throw new KafkaProducerException("Failed to send message to topic: " + topic, e);
        }
    }

    @Override
    public CompletableFuture<Void> sendMessageAsync(String topic, T message) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            log.debug("Sending async message to topic: {}", topic);
            kafkaTemplate.send(topic, message).whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Async message send failed for topic: {}", topic, exception);
                    future.completeExceptionally(exception);
                } else {
                    log.info("Async message sent successfully to topic: {}", topic);
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send async message to topic: {}", topic, e);
            future.completeExceptionally(new KafkaProducerException("Failed to send async message to topic: " + topic, e));
        }
        return future;
    }

    @Override
    public void sendMessage(String topic, String key, T message) {
        try {
            log.debug("Sending message to topic: {} with key: {}", topic, key);
            Message<T> kafkaMessage = MessageBuilder
                    .withPayload(message)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader("kafka_messageKey", key)
                    .build();
            kafkaTemplate.send(kafkaMessage);
            log.info("Message sent successfully to topic: {} with key: {}", topic, key);
        } catch (Exception e) {
            log.error("Failed to send message to topic: {} with key: {}", topic, key, e);
            throw new KafkaProducerException("Failed to send message to topic: " + topic + " with key: " + key, e);
        }
    }

    @Override
    public CompletableFuture<Void> sendMessageAsync(String topic, String key, T message) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            log.debug("Sending async message to topic: {} with key: {}", topic, key);
            Message<T> kafkaMessage = MessageBuilder
                    .withPayload(message)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader("kafka_messageKey", key)
                    .build();
            kafkaTemplate.send(kafkaMessage).whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Async message send failed for topic: {} with key: {}", topic, key, exception);
                    future.completeExceptionally(exception);
                } else {
                    log.info("Async message sent successfully to topic: {} with key: {}", topic, key);
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            log.error("Failed to send async message to topic: {} with key: {}", topic, key, e);
            future.completeExceptionally(new KafkaProducerException("Failed to send async message to topic: " + topic + " with key: " + key, e));
        }
        return future;
    }
}




