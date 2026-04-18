package com.agstech.kafka.producer.controller;

import com.agstech.kafka.producer.model.KafkaEvent;
import com.agstech.kafka.producer.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration tests for generic KafkaController
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = {
        "auto.create.topics.enable=true"
}, topics = { "my-topic", "test-topic", "user-events-topic" })
class KafkaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testPublishString() throws Exception {
        // given
        String message = "Hello, Kafka!";

        // when & then
        mockMvc.perform(post("/api/v1/kafka/publish/string")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("\"" + message + "\""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.code").value(200));
    }

    @Test
    void testPublishStringWithKey() throws Exception {
        // given
        String key = "test-key";
        String message = "Hello with key!";

        // when & then
        mockMvc.perform(post("/api/v1/kafka/publish/string/" + key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("\"" + message + "\""))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));
    }

    @Test
    void testPublishUser() throws Exception {
        // given
        User user = User.builder()
                .userId("user-123")
                .firstName("John")
                .lastName("Doe")
                .email("john.doe@example.com")
                .status("ACTIVE")
                .createdAt(LocalDateTime.now())
                .build();

        // when & then
        mockMvc.perform(post("/api/v1/kafka/publish/user")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(user)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));
    }

    @Test
    void testPublishEvent() throws Exception {
        // given
        KafkaEvent<String> event = KafkaEvent.<String>builder()
                .eventId("event-123")
                .eventType("USER_CREATED")
                .payload("Test payload")
                .timestamp(LocalDateTime.now())
                .source("TEST")
                .version("1.0")
                .build();

        // when & then
        mockMvc.perform(post("/api/v1/kafka/publish/event")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(event)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"));
    }

    @Test
    void testHealth() throws Exception {
        // when & then
        mockMvc.perform(get("/api/v1/kafka/health")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.message").value("Kafka Producer Service is healthy"));
    }
}

