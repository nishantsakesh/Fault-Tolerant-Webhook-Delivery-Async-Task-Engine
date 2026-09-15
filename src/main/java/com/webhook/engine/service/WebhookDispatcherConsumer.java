package com.webhook.engine.service;

import com.rabbitmq.client.Channel;
import com.webhook.engine.config.RabbitMqTopologyConfig;
import com.webhook.engine.domain.DeliveryStatus;
import com.webhook.engine.dto.WebhookTaskMessage;
import com.webhook.engine.repository.WebhookDeliveryRepository;
import com.webhook.engine.util.HmacSigner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.time.Instant;

@Component
@Slf4j
public class WebhookDispatcherConsumer {

    private final RestClient restClient;
    private final RabbitTemplate rabbitTemplate;
    private final WebhookDeliveryRepository deliveryRepository;

    private final Counter successCounter;
    private final Counter retryCounter;
    private final Counter dlqCounter;
    private final Timer dispatchTimer;

    private static final int MAX_ATTEMPTS = 5;

    public WebhookDispatcherConsumer(
            RestClient restClient,
            RabbitTemplate rabbitTemplate,
            WebhookDeliveryRepository deliveryRepository,
            MeterRegistry meterRegistry
    ) {
        this.restClient = restClient;
        this.rabbitTemplate = rabbitTemplate;
        this.deliveryRepository = deliveryRepository;

        this.successCounter = meterRegistry.counter("webhook.deliveries.success");
        this.retryCounter = meterRegistry.counter("webhook.deliveries.retry");
        this.dlqCounter = meterRegistry.counter("webhook.deliveries.dlq");
        this.dispatchTimer = meterRegistry.timer("webhook.deliveries.latency");
    }

    @RabbitListener(queues = RabbitMqTopologyConfig.WORK_QUEUE, ackMode = "MANUAL")
    public void processTask(
            WebhookTaskMessage message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag
    ) throws IOException {
        
        dispatchTimer.record(() -> {
            try {
                executeHttpDispatch(message);
            } catch (Exception ex) {
                handleDeliveryFailure(message, ex);
            }
        });

        channel.basicAck(deliveryTag, false);
    }

    @Transactional
    protected void executeHttpDispatch(WebhookTaskMessage message) {
        String signature = HmacSigner.calculateSignature(message.payload(), message.secretKey());

        restClient.post()
                .uri(message.targetUrl())
                .contentType(MediaType.APPLICATION_JSON)
                .header("X-Signature-SHA256", signature)
                .header("X-Delivery-Attempt", String.valueOf(message.attempt() + 1))
                .body(message.payload())
                .retrieve()
                .toBodilessEntity();

        successCounter.increment();
        log.info("Delivery success: DeliveryId={}", message.deliveryId());

        deliveryRepository.updateDeliveryState(
                message.deliveryId(),
                DeliveryStatus.DELIVERED,
                message.attempt() + 1,
                200,
                null,
                Instant.now()
        );
    }

    @Transactional
    protected void handleDeliveryFailure(WebhookTaskMessage message, Exception ex) {
        int nextAttempt = message.attempt() + 1;
        String errorMessage = (ex.getMessage() != null) ? ex.getMessage() : "HTTP Socket / Network Timeout";

        log.warn("Delivery failed: DeliveryId={} on attempt {}. Error: {}",
                message.deliveryId(), nextAttempt, errorMessage);

        if (nextAttempt >= MAX_ATTEMPTS) {
            dlqCounter.increment();
            log.error("Exhausted retries for DeliveryId={}. Routing to DLQ.", message.deliveryId());

            deliveryRepository.updateDeliveryState(
                    message.deliveryId(),
                    DeliveryStatus.DEAD_LETTER,
                    nextAttempt,
                    500,
                    errorMessage,
                    Instant.now()
            );

            rabbitTemplate.convertAndSend(
                    RabbitMqTopologyConfig.DLQ_EXCHANGE,
                    RabbitMqTopologyConfig.DLQ_ROUTING_KEY,
                    message
            );
            return;
        }

        retryCounter.increment();
        int delayMillis = (int) (5000 * Math.pow(5, nextAttempt - 1));

        deliveryRepository.updateDeliveryState(
                message.deliveryId(),
                DeliveryStatus.RETRYING,
                nextAttempt,
                500,
                errorMessage,
                Instant.now()
        );

        WebhookTaskMessage nextAttemptMessage = new WebhookTaskMessage(
                message.deliveryId(),
                message.tenantId(),
                message.targetUrl(),
                message.payload(),
                message.secretKey(),
                nextAttempt
        );

        rabbitTemplate.convertAndSend(
                RabbitMqTopologyConfig.DELAY_EXCHANGE,
                RabbitMqTopologyConfig.RETRY_ROUTING_KEY,
                nextAttemptMessage,
                m -> {
                    m.getMessageProperties().setDelay(delayMillis);
                    return m;
                }
        );
    }
}
