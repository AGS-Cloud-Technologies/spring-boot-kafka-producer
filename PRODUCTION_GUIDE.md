# Production-Ready Generic Kafka Producer

## Overview

This Spring Boot application provides a **production-ready, generic Kafka producer** that supports sending messages of any type using Java generics (`<T>`). The implementation includes:

- ✅ **Generic Type Support**: Send any object type (String, User, Event, custom objects)
- ✅ **Production Features**: Error handling, retry logic, idempotence, compression
- ✅ **Async Support**: CompletableFuture-based async message publishing
- ✅ **Key-based Partitioning**: Support for message keys for ordered delivery
- ✅ **Comprehensive Logging**: DEBUG and INFO level logging for monitoring
- ✅ **Exception Handling**: Custom KafkaProducerException for error handling
- ✅ **Configuration**: Externalized Kafka configuration with environment-specific profiles
- ✅ **REST API**: Multiple endpoints for different message types
- ✅ **Full Test Coverage**: Integration and unit tests

## Architecture

```
┌─────────────────────────────────────────────────────┐
│           REST Controller                           │
│    (Multiple endpoints for different types)         │
└─────────────┬───────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────────┐
│      KafkaProducerFactory                           │
│   (Creates typed producers on demand)               │
└─────────────┬───────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────────┐
│     KafkaProducerImpl<T>                             │
│  (Generic implementation with production features)  │
└─────────────┬───────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────────┐
│  KafkaTemplate<String, T>                           │
│  (Spring's Kafka template with JSON serialization)  │
└─────────────┬───────────────────────────────────────┘
              │
┌─────────────▼───────────────────────────────────────┐
│     Apache Kafka Broker                             │
│   (Distributed message storage)                     │
└─────────────────────────────────────────────────────┘
```

## Core Components

### 1. Generic Interface: `KafkaProducer<T>`
```java
public interface KafkaProducer<T> {
    void sendMessage(String topic, T message);
    CompletableFuture<Void> sendMessageAsync(String topic, T message);
    void sendMessage(String topic, String key, T message);
    CompletableFuture<Void> sendMessageAsync(String topic, String key, T message);
}
```

### 2. Generic Implementation: `KafkaProducerImpl<T>`
- Supports synchronous and asynchronous message sending
- Includes comprehensive error handling with KafkaProducerException
- Provides detailed logging for production monitoring
- Supports partition keys for ordered message delivery

### 3. Production Configuration
```yaml
kafka:
  producer:
    bootstrap-servers: localhost:9092
    topic: my-topic
    partitions: 1
    replication-factor: 1
    acks: all                    # All replicas must acknowledge
    retries: 3                   # Automatic retry on failure
    batch-size: 16384           # Batch messages for efficiency
    linger-ms: 10               # Wait time before sending batch
    buffer-memory: 33554432     # Total buffer memory
```

Plus in Spring config:
- **Compression**: Snappy compression to reduce bandwidth
- **Idempotence**: Enabled to prevent duplicate messages
- **JSON Serialization**: Automatic JSON serialization for any object type

## REST API Endpoints

### 1. Publish String Message
```bash
POST /api/v1/kafka/publish/string
Content-Type: application/json

"Hello, World!"

Response:
{
  "message": "Message published successfully",
  "status": "success",
  "code": 200,
  "timestamp": "2026-04-12T10:30:00"
}
```

### 2. Publish String with Key (Partition Key)
```bash
POST /api/v1/kafka/publish/string/{key}
Content-Type: application/json

"Hello, World!"
```

### 3. Publish User Object
```bash
POST /api/v1/kafka/publish/user
Content-Type: application/json

{
  "userId": "user-123",
  "firstName": "John",
  "lastName": "Doe",
  "email": "john.doe@example.com",
  "phone": "+1-123-456-7890",
  "status": "ACTIVE",
  "createdAt": "2026-04-12T10:30:00"
}
```

### 4. Publish Generic Event
```bash
POST /api/v1/kafka/publish/event
Content-Type: application/json

{
  "eventId": "evt-123",
  "eventType": "USER_CREATED",
  "payload": {
    "userId": "user-123",
    "firstName": "John"
  },
  "timestamp": "2026-04-12T10:30:00",
  "source": "USER_SERVICE",
  "version": "1.0"
}
```

### 5. Publish Generic JSON
```bash
POST /api/v1/kafka/publish/json/{topic}
Content-Type: application/json

{
  "anyField": "anyValue",
  "nested": {
    "field": "value"
  }
}
```

### 6. Health Check
```bash
GET /api/v1/kafka/health
```

## Usage Examples

### Java Code Examples

#### 1. Sending String Messages
```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void sendString() {
    KafkaProducer<String> producer = 
        new KafkaProducerImpl<>(kafkaTemplate, "my-topic");
    
    // Sync send
    producer.sendMessage("my-topic", "Hello World");
    
    // With key
    producer.sendMessage("my-topic", "user-123", "Hello User");
    
    // Async send
    CompletableFuture<Void> future = 
        producer.sendMessageAsync("my-topic", "Async message");
    future.thenRun(() -> System.out.println("Message sent!"));
}
```

#### 2. Sending Custom Objects
```java
@Autowired
private KafkaTemplate<String, User> userKafkaTemplate;

public void sendUser() {
    User user = User.builder()
        .userId("user-123")
        .firstName("John")
        .email("john@example.com")
        .build();
    
    KafkaProducer<User> producer = 
        new KafkaProducerImpl<>(userKafkaTemplate, "user-events");
    
    // Send with user ID as key for ordering
    producer.sendMessage("user-events", user.getUserId(), user);
}
```

#### 3. Sending Generic Events (Recommended for Production)
```java
@Autowired
private KafkaTemplate<String, KafkaEvent<?>> eventKafkaTemplate;

public void sendEvent() {
    KafkaEvent<User> event = KafkaEvent.<User>builder()
        .eventId(UUID.randomUUID().toString())
        .eventType("USER_CREATED")
        .payload(user)
        .timestamp(LocalDateTime.now())
        .source("USER_SERVICE")
        .version("1.0")
        .build();
    
    KafkaProducer<KafkaEvent<User>> producer = 
        new KafkaProducerImpl<>(eventKafkaTemplate, "events");
    
    producer.sendMessage("events", event.getEventId(), event);
}
```

#### 4. Error Handling
```java
try {
    producer.sendMessage("my-topic", message);
} catch (KafkaProducerException e) {
    logger.error("Failed to send message", e);
    // Handle error: retry, log, alert, etc.
}

// Async error handling
producer.sendMessageAsync("my-topic", message)
    .exceptionally(ex -> {
        logger.error("Async send failed", ex);
        return null;
    });
```

## Building and Running

### Prerequisites
- Java 17+
- Spring Boot 4.0.5+
- Kafka 3.0+
- Maven 3.8+

### Build
```bash
mvn clean package
```

### Run
```bash
# Start Kafka first
docker-compose up -d  # If you have docker-compose.yml

# Run the application
java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar

# Or run with Maven
mvn spring-boot:run
```

### With Different Profiles
```bash
java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar \
    --spring.profiles.active=dev

java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar \
    --spring.profiles.active=prod
```

## Environment-Specific Configuration

### Development (application-dev.yaml)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

kafka:
  producer:
    acks: 1
    retries: 1
```

### Production (application-prod.yaml)
```yaml
spring:
  kafka:
    bootstrap-servers: kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092

kafka:
  producer:
    acks: all
    retries: 5
    buffer-memory: 67108864
```

## Testing

### Run All Tests
```bash
mvn test
```

### Run Specific Test
```bash
mvn test -Dtest=KafkaProducerImplTest
mvn test -Dtest=KafkaControllerTest
```

### Test Coverage
```bash
mvn test jacoco:report
# Report at: target/site/jacoco/index.html
```

## Production Features

### 1. Idempotent Producer
- Prevents duplicate messages in case of retries
- Enabled in configuration: `enable.idempotence: true`

### 2. Compression
- Reduces bandwidth and storage: Snappy compression
- Automatic compression for all messages

### 3. Batching
- Batch size: 16KB
- Linger time: 10ms
- Reduces overhead and increases throughput

### 4. Retry Logic
- Maximum retries: 3 (configurable)
- Automatic exponential backoff
- Handled by Kafka client

### 5. Acknowledgments
- Acks: all (all replicas must acknowledge)
- Ensures durability and no data loss

### 6. Error Handling
- Custom `KafkaProducerException` for application errors
- Comprehensive logging at DEBUG and INFO levels
- Failed messages logged with full stack trace

### 7. Monitoring & Logging
- Application logs at `com.agstech.kafka` package
- Kafka framework logs at INFO level
- Structured logging with timestamps

## Extensibility

### Create Custom Producer for New Type

```java
@Configuration
public class MyTypeKafkaConfig {
    
    @Bean
    public KafkaTemplate<String, MyType> myTypeKafkaTemplate() {
        return new KafkaTemplate<>(myTypeProducerFactory());
    }
    
    @Bean
    public ProducerFactory<String, MyType> myTypeProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }
}

// Usage
@Autowired
private KafkaTemplate<String, MyType> myTypeKafkaTemplate;

public void sendMyType() {
    KafkaProducer<MyType> producer = 
        new KafkaProducerImpl<>(myTypeKafkaTemplate, "my-type-topic");
    producer.sendMessage("my-type-topic", myTypeInstance);
}
```

## Best Practices

1. **Use Keys for Ordering**: If you need ordered delivery within a partition, always use partition keys
2. **Event Wrapper**: Use `KafkaEvent<T>` wrapper for enriched message metadata
3. **Async for High Throughput**: Use async methods when throughput is critical
4. **Error Handling**: Always handle `KafkaProducerException` appropriately
5. **Configuration**: Use environment-specific profiles (dev, stage, prod)
6. **Logging**: Check logs for DEBUG information in development, INFO in production
7. **Topic Management**: Pre-create topics with appropriate partition and replication counts
8. **Monitoring**: Monitor lag, error rates, and throughput in production

## Dependencies

- Spring Boot 4.0.5
- Spring Kafka 3.0+
- Jackson Databind (JSON serialization)
- Lombok (optional, for POJOs)
- JUnit 5 (testing)
- Spring Kafka Test (embedded Kafka for testing)

## License

This project is provided as-is for production use.

## Support

For issues or questions, refer to:
- Spring Kafka Documentation: https://spring.io/projects/spring-kafka
- Apache Kafka Documentation: https://kafka.apache.org/
- JetBrains IDE Documentation

