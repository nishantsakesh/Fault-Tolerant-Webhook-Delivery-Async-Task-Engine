package com.webhook.engine.dto;

import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

public record IngressWebhookRequest(
    @NotBlank String tenantId,
    @NotBlank @URL String targetUrl,
    @NotBlank String payload,
    @NotBlank String secretKey
) {}
