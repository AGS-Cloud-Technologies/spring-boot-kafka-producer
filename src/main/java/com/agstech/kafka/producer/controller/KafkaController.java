package com.agstech.kafka.producer.controller;

import com.agstech.kafka.producer.model.KafkaEvent;
import com.agstech.kafka.producer.model.User;
import com.agstech.kafka.producer.service.KafkaProducer;
import com.agstech.kafka.producer.service.KafkaProducerException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic REST Controller for Kafka message publishing
 * Demonstrates production-ready generic producer usage
 */
@RestController
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    private static final Logger log = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    @Qualifier("kafkaTemplate")
    private KafkaTemplate<String, String> stringKafkaTemplate;

    @Value("${kafka.producer.topic:my-topic}")
    private String defaultTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Send a generic string message
     * @param message The message to send
     * @return ResponseEntity with status
     */
    @PostMapping("/publish/string")
    public ResponseEntity<Map<String, Object>> publishString(@RequestBody String message) {
        try {
            log.info("Publishing string message: {}", message);
            KafkaProducer<String> producer = new com.agstech.kafka.producer.service.KafkaProducerImpl<>(stringKafkaTemplate, defaultTopic);
            producer.sendMessage(defaultTopic, message);

            return ResponseEntity.ok(createResponse("Message published successfully", "success", 200));
        } catch (KafkaProducerException e) {
            log.error("Error publishing string message", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createResponse("Failed to publish message: " + e.getMessage(), "error", 500));
        }
    }

    /**
     * Send a generic string message with a key
     * @param key The partition key
     * @param message The message to send
     * @return ResponseEntity with status
     */
    @PostMapping("/publish/string/{key}")
    public ResponseEntity<Map<String, Object>> publishStringWithKey(
            @PathVariable String key,
            @RequestBody String message) {
        try {
            log.info("Publishing string message with key: {}", key);
            KafkaProducer<String> producer = new com.agstech.kafka.producer.service.KafkaProducerImpl<>(stringKafkaTemplate, defaultTopic);
            producer.sendMessage(defaultTopic, key, message);

            return ResponseEntity.ok(createResponse("Message with key published successfully", "success", 200));
        } catch (KafkaProducerException e) {
            log.error("Error publishing string message with key", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createResponse("Failed to publish message: " + e.getMessage(), "error", 500));
        }
    }

    /**
     * Send a generic JSON payload (any type)
     * @param topic The topic to send to
     * @param payload The JSON payload
     * @return ResponseEntity with status
     */
    @PostMapping("/publish/json/{topic}")
    public ResponseEntity<Map<String, Object>> publishJson(
            @PathVariable String topic,
            @RequestBody Object payload) {
        try {
            log.info("Publishing JSON payload to topic: {}", topic);
            KafkaProducer<Object> producer = new com.agstech.kafka.producer.service.KafkaProducerImpl<>(
                    new KafkaTemplate<>(new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(getProducerConfig())),
                    topic
            );
            producer.sendMessage(topic, payload);

            return ResponseEntity.ok(createResponse("JSON payload published successfully", "success", 200));
        } catch (Exception e) {
            log.error("Error publishing JSON payload", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createResponse("Failed to publish message: " + e.getMessage(), "error", 500));
        }
    }

    /**
     * Send a User object (typed message)
     * @param user The User object to send
     * @return ResponseEntity with status
     */
    @PostMapping("/publish/user")
    public ResponseEntity<Map<String, Object>> publishUser(@RequestBody User user) {
        try {
            log.info("Publishing user event: {}", user.userId);
            KafkaTemplate<String, User> userKafkaTemplate = new KafkaTemplate<>(
                    new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(getProducerConfig())
            );

            KafkaProducer<User> producer = new com.agstech.kafka.producer.service.KafkaProducerImpl<>(
                    userKafkaTemplate,
                    "user-events-topic"
            );

            producer.sendMessage("user-events-topic", user.userId, user);

            return ResponseEntity.ok(createResponse("User event published successfully", "success", 200));
        } catch (Exception e) {
            log.error("Error publishing user event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createResponse("Failed to publish user event: " + e.getMessage(), "error", 500));
        }
    }

    /**
     * Send a generic KafkaEvent wrapper (most production use case)
     * @param event The KafkaEvent to send
     * @param <T> The payload type
     * @return ResponseEntity with status
     */
    @PostMapping("/publish/event")
    public <T> ResponseEntity<Map<String, Object>> publishEvent(@RequestBody KafkaEvent<T> event) {
        try {
            log.info("Publishing event: {} of type: {}", event.eventId, event.eventType);

            if (event.eventId == null) {
                event.eventId = UUID.randomUUID().toString();
            }
            if (event.timestamp == null) {
                event.timestamp = LocalDateTime.now();
            }

            KafkaTemplate<String, KafkaEvent<T>> eventKafkaTemplate = new KafkaTemplate<>(
                    new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(getProducerConfig())
            );

            KafkaProducer<KafkaEvent<T>> producer = new com.agstech.kafka.producer.service.KafkaProducerImpl<>(
                    eventKafkaTemplate,
                    defaultTopic
            );

            producer.sendMessage(defaultTopic, event.eventId, event);

            return ResponseEntity.ok(createResponse("Event published successfully with ID: " + event.eventId, "success", 200));
        } catch (Exception e) {
            log.error("Error publishing event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createResponse("Failed to publish event: " + e.getMessage(), "error", 500));
        }
    }

    /**
     * Health check endpoint
     * @return ResponseEntity with status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(createResponse("Kafka Producer Service is healthy", "success", 200));
    }

    /**
     * Helper method to create response map
     */
    private Map<String, Object> createResponse(String message, String status, int code) {
        Map<String, Object> response = new HashMap<>();
        response.put("message", message);
        response.put("status", status);
        response.put("code", code);
        response.put("timestamp", LocalDateTime.now());
        return response;
    }

    /**
     * Helper method to get producer configuration
     */
    private Map<String, Object> getProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JacksonJsonSerializer.class);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        config.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(org.springframework.kafka.support.serializer.JacksonJsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return config;
    }
}


