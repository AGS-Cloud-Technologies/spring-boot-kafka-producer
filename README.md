# Spring Boot Generic Kafka Producer

A **production-ready, enterprise-grade Kafka producer** built with Spring Boot 4.0.5 that supports sending messages of any type using Java generics (`<T>`).

## 🚀 Features

- ✅ **Generic Type Support** - Send any Java object type (String, User, Event, custom classes)
- ✅ **Production Grade** - Idempotence, compression, retries, error handling
- ✅ **Async Support** - CompletableFuture-based asynchronous message publishing
- ✅ **Key-based Partitioning** - Support for message keys for ordered delivery per partition
- ✅ **Comprehensive Logging** - DEBUG and INFO level logging for monitoring
- ✅ **Exception Handling** - Custom `KafkaProducerException` for error handling
- ✅ **Externalized Configuration** - Environment-specific profiles (dev, stage, prod)
- ✅ **REST API** - Multiple endpoints for different message types
- ✅ **Full Test Coverage** - Integration and unit tests with embedded Kafka
- ✅ **JSON Serialization** - Automatic JSON serialization for any object type
- ✅ **Docker Support** - docker-compose.yml for quick setup

## 📋 Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Components](#components)
- [REST API](#rest-api)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Testing](#testing)
- [Deployment](#deployment)
- [Production Features](#production-features)

## 🏗️ Architecture

```
REST Controller (Generic endpoints)
        ↓
KafkaProducerFactory (Type-safe producer creation)
        ↓
KafkaProducerImpl<T> (Generic implementation)
        ↓
KafkaTemplate<String, T> (Spring's Kafka template)
        ↓
Apache Kafka Broker
```

## 🚀 Quick Start

### Prerequisites
- Java 17+
- Maven 3.8+
- Docker & Docker Compose (optional, for Kafka)

### 1. Clone/Setup Project
```bash
cd E:\Spring_Boot\spring-boot-kafka-producer
```

### 2. Start Kafka
```bash
# Using docker-compose
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Build Project
```bash
mvn clean install
```

### 4. Run Application
```bash
# Default (dev profile)
mvn spring-boot:run

# With specific profile
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=dev"
```

### 5. Test API
```bash
# String message
curl -X POST http://localhost:8080/api/v1/kafka/publish/string \
  -H "Content-Type: application/json" \
  -d '"Hello, Kafka!"'

# Health check
curl http://localhost:8080/api/v1/kafka/health
```

## 🧩 Components

### 1. KafkaProducer<T> Interface
Generic interface supporting multiple send methods:
```java
public interface KafkaProducer<T> {
    void sendMessage(String topic, T message);
    CompletableFuture<Void> sendMessageAsync(String topic, T message);
    void sendMessage(String topic, String key, T message);
    CompletableFuture<Void> sendMessageAsync(String topic, String key, T message);
}
```

### 2. KafkaProducerImpl<T>
Generic implementation with:
- Comprehensive error handling
- Detailed logging
- Async support with CompletableFuture
- Key-based partitioning support

### 3. KafkaEvent<T>
Generic event wrapper for production use:
```java
@Data
public class KafkaEvent<T> {
    private String eventId;
    private String eventType;
    private T payload;
    private LocalDateTime timestamp;
    private String source;
    private String version;
}
```

### 4. KafkaProducerFactory
Factory for creating typed producers on demand.

### 5. KafkaProducerRegistry
Registry for managing multiple typed producers (optional, for advanced scenarios).

## 📡 REST API

### 1. Publish String Message
```http
POST /api/v1/kafka/publish/string
Content-Type: application/json

"Hello, World!"
```

### 2. Publish String with Key
```http
POST /api/v1/kafka/publish/string/{key}
Content-Type: application/json

"Hello, World!"
```

### 3. Publish User Object
```http
POST /api/v1/kafka/publish/user
Content-Type: application/json

{
  "userId": "user-123",
  "firstName": "John",
  "lastName": "Doe",
  "email": "john@example.com",
  "status": "ACTIVE"
}
```

### 4. Publish Generic Event (Recommended)
```http
POST /api/v1/kafka/publish/event
Content-Type: application/json

{
  "eventId": "evt-123",
  "eventType": "USER_CREATED",
  "payload": { ... },
  "timestamp": "2026-04-12T10:30:00",
  "source": "USER_SERVICE",
  "version": "1.0"
}
```

### 5. Publish Generic JSON
```http
POST /api/v1/kafka/publish/json/{topic}
Content-Type: application/json

{
  "anyField": "anyValue"
}
```

### 6. Health Check
```http
GET /api/v1/kafka/health
```

## 💡 Usage Examples

### Sending Strings
```java
@Autowired
private KafkaTemplate<String, String> kafkaTemplate;

public void sendString() {
    KafkaProducer<String> producer = 
        new KafkaProducerImpl<>(kafkaTemplate, "my-topic");
    
    // Sync
    producer.sendMessage("my-topic", "Hello");
    
    // With key
    producer.sendMessage("my-topic", "key-1", "Hello");
    
    // Async
    producer.sendMessageAsync("my-topic", "Hello")
        .thenRun(() -> log.info("Sent!"))
        .exceptionally(e -> {
            log.error("Failed", e);
            return null;
        });
}
```

### Sending Custom Objects
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
    
    producer.sendMessage("user-events", user.getUserId(), user);
}
```

### Sending Events (Best Practice)
```java
@Autowired
private KafkaTemplate<String, KafkaEvent<?>> eventKafkaTemplate;

public void sendEvent() {
    KafkaEvent<Order> event = KafkaEvent.<Order>builder()
        .eventId(UUID.randomUUID().toString())
        .eventType("ORDER_PLACED")
        .payload(order)
        .timestamp(LocalDateTime.now())
        .source("ORDER_SERVICE")
        .version("1.0")
        .build();
    
    KafkaProducer<KafkaEvent<Order>> producer = 
        new KafkaProducerImpl<>(eventKafkaTemplate, "events");
    
    producer.sendMessage("events", event.getEventId(), event);
}
```

### Error Handling
```java
try {
    producer.sendMessage("topic", message);
} catch (KafkaProducerException e) {
    log.error("Failed to send", e);
    // Handle error
}

// Async error handling
producer.sendMessageAsync("topic", message)
    .exceptionally(e -> {
        log.error("Async send failed", e);
        return null;
    });
```

## ⚙️ Configuration

### Default (application.yaml)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

kafka:
  producer:
    topic: my-topic
    acks: all
    retries: 3
    batch-size: 16384
    buffer-memory: 33554432
```

### Development (application-dev.yaml)
- Single broker
- Fewer retries
- Debug logging

### Staging (application-stage.yaml)
- 2-broker setup
- Full idempotence
- Info logging

### Production (application-prod.yaml)
- 3-broker cluster
- 5 retries
- Larger buffers
- Warn logging

## 🧪 Testing

### Run All Tests
```bash
mvn test
```

### Run Specific Test Class
```bash
mvn test -Dtest=KafkaProducerImplTest
mvn test -Dtest=KafkaControllerTest
```

### Generate Coverage Report
```bash
mvn clean test jacoco:report
# View: target/site/jacoco/index.html
```

### Test Features
- Embedded Kafka for integration tests
- Mock MVC for REST endpoint testing
- Mockito for unit testing
- Comprehensive test scenarios

## 🐳 Docker

### Start Kafka with Docker Compose
```bash
docker-compose up -d
```

### Services
- **Zookeeper**: Port 2181
- **Kafka**: Ports 9092 (local), 29092 (internal)
- **Kafka UI**: http://localhost:8080

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
docker-compose logs -f kafka
```

## 📦 Deployment

### Build JAR
```bash
mvn clean package
```

### Run JAR
```bash
# Default profile
java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar

# Specific profile
java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar \
    --spring.profiles.active=prod

# Override properties
java -jar target/spring-boot-kafka-producer-0.0.1-SNAPSHOT.jar \
    --spring.kafka.bootstrap-servers=kafka-broker:9092 \
    --kafka.producer.topic=production-topic
```

### Docker Image (Optional)
```dockerfile
FROM openjdk:17-slim
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

## 🏆 Production Features

### 1. **Idempotent Producer**
- Prevents duplicate messages
- Enabled in configuration
- Transparent to application

### 2. **Compression**
- Snappy compression for all messages
- Reduces bandwidth by ~50%
- Automatic and transparent

### 3. **Batching**
- Batch size: 16-65KB (configurable)
- Linger time: 10-20ms (configurable)
- Increases throughput significantly

### 4. **Retries**
- Automatic retries: 1-5 (configurable)
- Exponential backoff
- Configurable per environment

### 5. **Acknowledgments**
- Acks: all (all replicas must acknowledge)
- Ensures no data loss
- Trade-off: latency for durability

### 6. **Error Handling**
- Custom `KafkaProducerException`
- Comprehensive error logging
- Failed messages logged with details

### 7. **Monitoring**
- Structured logging with timestamps
- DEBUG level in dev, INFO in prod
- Easy to integrate with monitoring tools

## 📚 Documentation

- **PRODUCTION_GUIDE.md** - Comprehensive production deployment guide
- **API_REQUESTS.http** - Example API requests for testing
- **pom.xml** - Maven dependencies and build configuration

## 🔧 Troubleshooting

### Connection Issues
```bash
# Check Kafka is running
docker-compose ps

# Check logs
docker-compose logs kafka

# Verify connectivity
telnet localhost 9092
```

### Message Not Appearing
1. Check bootstrap servers configuration
2. Verify topic exists
3. Check logs for exceptions
4. Verify JSON serialization compatibility

### Performance Issues
1. Increase batch size
2. Increase buffer memory
3. Increase linger time
4. Check network latency
5. Check Kafka broker resources

## 📝 Dependencies

- Spring Boot 4.0.5
- Spring Kafka 3.0+
- Jackson Databind (JSON)
- Lombok (POJOs)
- JUnit 5 (Testing)
- Mockito (Mocking)
- Spring Kafka Test (Embedded Kafka)

## 📄 License

This project is provided as-is for production use.

## 🤝 Contributing

For enhancements or issues, please refer to project documentation and Spring Kafka best practices.

## 📞 Support

**Resources:**
- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)

---

**Version**: 1.0.0  
**Last Updated**: 2026-04-12  
**Status**: Production Ready ✅

