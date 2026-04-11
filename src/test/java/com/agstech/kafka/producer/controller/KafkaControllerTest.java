package com.agstech.kafka.producer.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = { "my-topic" })
class KafkaControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void publish() throws Exception {
        // given
        String message = "Hello, Kafka!";

        // when & then
        mockMvc.perform(post("/api/v1/kafka/publish")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(message))
                .andExpect(status().isOk());
    }
}
