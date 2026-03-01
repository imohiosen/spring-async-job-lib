package com.imohiosen.asyncjob.spring.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.imohiosen.asyncjob.port.messaging.JobMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaJobMessageProducerTest {

    @Mock KafkaTemplate<String, String> kafkaTemplate;

    ObjectMapper objectMapper;
    KafkaJobMessageProducer producer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        producer = new KafkaJobMessageProducer(kafkaTemplate, objectMapper);
    }

    @Test
    void publish_sendsJsonToCorrectTopicWithTaskIdAsKey() {
        UUID taskId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        JobMessage message = new JobMessage(taskId, jobId, "ORDER", "{\"id\":1}");

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        producer.publish("order-tasks", message);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), valueCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo("order-tasks");
        assertThat(keyCaptor.getValue()).isEqualTo(taskId.toString());

        // Value should be valid JSON containing our fields
        String json = valueCaptor.getValue();
        assertThat(json).contains(taskId.toString());
        assertThat(json).contains(jobId.toString());
        assertThat(json).contains("ORDER");
    }

    @Test
    void publish_serializesJobMessageToValidJson() throws Exception {
        UUID taskId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        JobMessage message = new JobMessage(taskId, jobId, "INVOICE", "{\"inv\":42}");

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        producer.publish("invoices", message);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("invoices"), eq(taskId.toString()), valueCaptor.capture());

        // Deserialize should round-trip
        JobMessage roundTripped = objectMapper.readValue(valueCaptor.getValue(), JobMessage.class);
        assertThat(roundTripped).isEqualTo(message);
    }

    @Test
    void deserialize_validJson_returnsJobMessage() throws Exception {
        UUID taskId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        JobMessage original = new JobMessage(taskId, jobId, "PAYMENT", "{\"amount\":100}");
        String json = objectMapper.writeValueAsString(original);

        JobMessage result = producer.deserialize(json);

        assertThat(result).isEqualTo(original);
        assertThat(result.taskId()).isEqualTo(taskId);
        assertThat(result.jobId()).isEqualTo(jobId);
        assertThat(result.taskType()).isEqualTo("PAYMENT");
        assertThat(result.payload()).isEqualTo("{\"amount\":100}");
    }

    @Test
    void deserialize_invalidJson_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> producer.deserialize("not valid json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to deserialize JobMessage");
    }

    @Test
    void deserialize_emptyJson_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> producer.deserialize(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publish_serializationFailure_throwsIllegalArgumentException() {
        // Use a mock ObjectMapper that always throws
        ObjectMapper badMapper = mock(ObjectMapper.class);
        KafkaJobMessageProducer badProducer = new KafkaJobMessageProducer(kafkaTemplate, badMapper);

        UUID taskId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        JobMessage message = new JobMessage(taskId, jobId, "TYPE", "payload");

        try {
            when(badMapper.writeValueAsString(any()))
                    .thenThrow(new JsonProcessingException("serialization error") {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        assertThatThrownBy(() -> badProducer.publish("topic", message))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to serialize JobMessage");
    }

    @Test
    void publish_roundTrip_publishAndDeserializeAreSymmetric() throws Exception {
        UUID taskId = UUID.randomUUID();
        UUID jobId = UUID.randomUUID();
        JobMessage original = new JobMessage(taskId, jobId, "EMAIL", "{\"to\":\"test@example.com\"}");

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        producer.publish("emails", original);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("emails"), eq(taskId.toString()), valueCaptor.capture());

        // Simulate consumer deserializing the same JSON
        JobMessage deserialized = producer.deserialize(valueCaptor.getValue());
        assertThat(deserialized).isEqualTo(original);
    }
}
