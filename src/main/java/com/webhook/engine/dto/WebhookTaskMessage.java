package com.webhook.engine.dto;

import java.util.UUID;

public record WebhookTaskMessage(
    UUID deliveryId,
    String tenantId,
    String targetUrl,
    String payload,
    String secretKey,
    int attempt
) {}
