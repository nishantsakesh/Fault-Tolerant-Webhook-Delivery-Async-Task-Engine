package com.webhook.engine;

import com.webhook.engine.config.RabbitMqTopologyConfig;
import com.webhook.engine.domain.DeliveryStatus;
import com.webhook.engine.domain.WebhookDelivery;
import com.webhook.engine.dto.IngressWebhookRequest;
import com.webhook.engine.repository.WebhookDeliveryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class WebhookEngineIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("webhook_test_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
            .withExposedPorts(6379);

    @Container
    static RabbitMQContainer rabbitmq = new RabbitMQContainer(
            DockerImageName.parse("heidiks/rabbitmq-delayed-message-exchange:3.13-management")
    );

    @DynamicPropertySource
    static void setProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("spring.rabbitmq.host", rabbitmq::getHost);
        registry.add("spring.rabbitmq.port", rabbitmq::getAmqpPort);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private WebhookDeliveryRepository deliveryRepository;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @BeforeEach
    void resetQueues() {
        RabbitAdmin admin = new RabbitAdmin(rabbitTemplate);
        admin.purgeQueue(RabbitMqTopologyConfig.WORK_QUEUE);
        admin.purgeQueue(RabbitMqTopologyConfig.DLQ_QUEUE);
    }

    @Test
    void shouldQueueAndRetryWhenEndpointIsDown() {
        IngressWebhookRequest request = new IngressWebhookRequest(
                "tenant-alpha",
                "http://127.0.0.1:59999/webhook/receiver",
                "{\"event\":\"user.created\",\"id\":\"usr_123\"}",
                "0123456789abcdef"
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/tasks/dispatch",
                request,
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            WebhookDelivery delivery = deliveryRepository.findAll().stream()
                    .filter(d -> d.getTenantId().equals("tenant-alpha"))
                    .findFirst()
                    .orElse(null);

            assertThat(delivery).isNotNull();
            assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.RETRYING);
            assertThat(delivery.getAttempts()).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void shouldThrottleTenantExceedingRateLimit() {
        IngressWebhookRequest request = new IngressWebhookRequest(
                "tenant-burst",
                "http://localhost:8081/mock",
                "{}",
                "secret-pass"
        );

        int rejectedCount = 0;
        for (int i = 0; i < 60; i++) {
            ResponseEntity<String> res = restTemplate.postForEntity(
                    "/api/v1/tasks/dispatch",
                    request,
                    String.class
            );
            if (res.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                rejectedCount++;
            }
        }

        assertThat(rejectedCount).isGreaterThan(0);
    }
}
