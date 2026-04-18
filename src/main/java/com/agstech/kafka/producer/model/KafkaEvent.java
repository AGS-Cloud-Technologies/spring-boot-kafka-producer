package com.agstech.kafka.producer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Generic Event model for Kafka messages
 * Implements Serializable for compatibility
 */
public class KafkaEvent<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    public String eventId;
    public String eventType;
    public T payload;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    public LocalDateTime timestamp;
    public String source;
    public String version;

    // Constructors
    public KafkaEvent() {
    }

    public KafkaEvent(String eventId, String eventType, T payload, LocalDateTime timestamp, String source, String version) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.payload = payload;
        this.timestamp = timestamp;
        this.source = source;
        this.version = version;
    }

    // Builder pattern
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private String eventId;
        private String eventType;
        private T payload;
        private LocalDateTime timestamp;
        private String source;
        private String version;

        public Builder<T> eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder<T> eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder<T> payload(T payload) {
            this.payload = payload;
            return this;
        }

        public Builder<T> timestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder<T> source(String source) {
            this.source = source;
            return this;
        }

        public Builder<T> version(String version) {
            this.version = version;
            return this;
        }

        public KafkaEvent<T> build() {
            return new KafkaEvent<>(eventId, eventType, payload, timestamp, source, version);
        }
    }
}


