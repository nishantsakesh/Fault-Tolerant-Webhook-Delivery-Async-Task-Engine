package com.webhook.engine.web;

import com.webhook.engine.config.RabbitMqTopologyConfig;
import com.webhook.engine.domain.DeliveryStatus;
import com.webhook.engine.domain.WebhookDelivery;
import com.webhook.engine.dto.IngressWebhookRequest;
import com.webhook.engine.dto.WebhookTaskMessage;
import com.webhook.engine.repository.WebhookDeliveryRepository;
import com.webhook.engine.service.RedisSlidingWindowRateLimiter;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class WebhookTaskController {

    private final RedisSlidingWindowRateLimiter rateLimiter;
    private final WebhookDeliveryRepository deliveryRepository;
    private final RabbitTemplate rabbitTemplate;

    @PostMapping("/dispatch")
    public ResponseEntity<?> queueTask(@Valid @RequestBody IngressWebhookRequest request) {
        
        // Use sliding window rate limiter (defaulting to 50 max RPS per tenant)
        boolean allowed = rateLimiter.tryAcquire(request.tenantId(), 50);
        if (!allowed) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", "Tenant rate limit exceeded. Retry later."));
        }

        UUID deliveryId = UUID.randomUUID();

        // Save initial state to DB
        WebhookDelivery entity = WebhookDelivery.builder()
                .id(deliveryId)
                .tenantId(request.tenantId())
                .targetUrl(request.targetUrl())
                .payload(request.payload())
                .secretKey(request.secretKey())
                .status(DeliveryStatus.PENDING)
                .attempts(0)
                .build();
        deliveryRepository.save(entity);

        // Queue message into RabbitMQ
        WebhookTaskMessage taskMessage = new WebhookTaskMessage(
                deliveryId,
                request.tenantId(),
                request.targetUrl(),
                request.payload(),
                request.secretKey(),
                0
        );

        rabbitTemplate.convertAndSend(
                RabbitMqTopologyConfig.WORK_EXCHANGE,
                RabbitMqTopologyConfig.WORK_ROUTING_KEY,
                taskMessage
        );

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("deliveryId", deliveryId, "status", "QUEUED"));
    }
}
