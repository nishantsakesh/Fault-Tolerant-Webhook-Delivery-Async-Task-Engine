package com.webhook.engine.domain;

public enum DeliveryStatus {
    PENDING,
    DELIVERED,
    RETRYING,
    DEAD_LETTER
}
